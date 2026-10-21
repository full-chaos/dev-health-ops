//go:build integration

package teamattribution

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
)

// D5987 option C (CHAOS-10517): LoadRepos answers an instant before the first recorded ownership of a repository with
// the repository's FIRST recorded owners, from the one rule of package ownershipwindow. An instant at or after the first
// record reads as before, an instant in a gap between two records has no owner, and an inactive team's earlier row does
// not set the first record (section 0.4g rule 1).
func TestLoadReposAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)
	const org = "org-load-repos-first-owners"
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	repoIDs := map[string]string{
		"pair": "aaaaaaaa-0000-4000-8000-000000000001", "gap": "aaaaaaaa-0000-4000-8000-000000000002",
		"grant-only": "aaaaaaaa-0000-4000-8000-000000000003", "revoked": "aaaaaaaa-0000-4000-8000-000000000004",
		"tie": "aaaaaaaa-0000-4000-8000-000000000005", "by-name": "aaaaaaaa-0000-4000-8000-000000000006",
	}
	for name, id := range repoIDs {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (toUUID(?), ?, ?, 'github', ?)`,
			id, "acme/"+name, org, synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, name, is_active, updated_at, last_synced, org_id) VALUES ('team-gone', 'Gone', 0, ?, ?, ?)`,
		synced, synced, org); err != nil {
		t.Fatal(err)
	}
	ended, endedEarly := day(time.April, 1), day(time.January, 20)
	type row struct {
		team, repo string
		byName     bool
		primary    uint8
		from       time.Time
		to         *time.Time
	}
	rows := []row{
		{"team-a", "pair", false, 1, day(time.March, 1), nil},
		{"team-b", "pair", false, 2, day(time.March, 1), nil},
		{"team-a", "gap", false, 1, day(time.March, 1), &ended},
		{"team-c", "gap", false, 1, day(time.May, 1), nil},
		{"team-gone", "grant-only", false, 1, day(time.February, 1), nil},
		{"team-a", "grant-only", false, 1, day(time.March, 1), nil},
		// revoked: a row that ended before it began is never valid; the repository is owned from March 1 only.
		{"team-d", "revoked", false, 1, day(time.February, 1), &endedEarly},
		{"team-a", "revoked", false, 1, day(time.March, 1), nil},
		{"team-a", "tie", false, 1, day(time.March, 1), nil},
		{"team-b", "tie", false, 1, day(time.March, 1), nil},
		// by-name: the row holds no repo_id; the repos catalog resolves it.
		{"team-a", "by-name", true, 1, day(time.March, 1), nil},
	}
	for _, r := range rows {
		var repoID any = repoIDs[r.repo]
		insert := `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'inferred', ?, 150, 0, ?, ?, ?)`
		if r.byName {
			insert = strings.Replace(insert, "toUUID(?)", "?", 1)
			repoID = nil
		}
		if err := conn.Exec(ctx, insert, org, r.team, repoID, "acme/"+r.repo, r.primary, r.from, r.to, synced); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	owners := func(query string, at time.Time) map[string][]string {
		t.Helper()
		out, err := conn.Query(ctx, query, clickhouse.Named("org_id", org),
			clickhouse.Named("as_of", at.UTC().Format("2006-01-02 15:04:05.000")), clickhouse.Named("row_limit", uint64(1000)))
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer out.Close()
		got := map[string][]string{}
		for out.Next() {
			var provider, team, teamName, repoID, repoName string
			var primary uint8
			var specificity uint16
			var priority int32
			var updated time.Time
			if err := out.Scan(&provider, &team, &teamName, &repoID, &repoName, &primary, &specificity, &priority, &updated); err != nil {
				t.Fatal(err)
			}
			got[strings.TrimPrefix(repoName, "acme/")] = append(got[strings.TrimPrefix(repoName, "acme/")], team)
		}
		for _, v := range got {
			sort.Strings(v)
		}
		return got
	}
	shared := loadReposQuery(loadReposOwnershipWindow)
	atFirst, beforeAll, inGap, later := day(time.March, 1), day(time.January, 15), day(time.April, 15), day(time.September, 21)
	cases := []struct {
		name string
		at   time.Time
		want map[string][]string
	}{
		{"before every record", beforeAll, map[string][]string{
			"pair": {"team-a", "team-b"}, "gap": {"team-a"}, "grant-only": {"team-a"}, "tie": {"team-a", "team-b"}, "by-name": {"team-a"},
		}},
		{"at the first record", atFirst, map[string][]string{
			"pair": {"team-a", "team-b"}, "gap": {"team-a"}, "grant-only": {"team-a"}, "revoked": {"team-a"},
			"tie": {"team-a", "team-b"}, "by-name": {"team-a"},
		}},
		{"in a gap between records", inGap, map[string][]string{
			"pair": {"team-a", "team-b"}, "grant-only": {"team-a"}, "revoked": {"team-a"}, "tie": {"team-a", "team-b"}, "by-name": {"team-a"},
		}},
		{"after the records", later, map[string][]string{
			"pair": {"team-a", "team-b"}, "gap": {"team-c"}, "grant-only": {"team-a"}, "revoked": {"team-a"},
			"tie": {"team-a", "team-b"}, "by-name": {"team-a"},
		}},
	}
	for _, tc := range cases {
		if got := owners(shared, tc.at); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: LoadRepos owners = %v, want %v", tc.name, got, tc.want)
		}
	}
	// "revoked before first" before every record: the earliest row (team-d, ended before it began) sets the first
	// record of the repository as the shared rule defines it, and is never valid, so nobody owns it.
	if got := owners(shared, beforeAll)["revoked"]; len(got) != 0 {
		t.Errorf("a row revoked before it began owns before the first record: %v", got)
	}
	// Roles are kept: before the first record the main owner is primary and the co-owner a co-owner row (D5988).
	facts, err := ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, org, beforeAll)
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]int{}
	for _, fact := range facts {
		if fact.RepoFullName == "acme/pair" {
			roles[fact.TeamID] = fact.IsPrimary
		}
	}
	if !reflect.DeepEqual(roles, map[string]int{"team-a": 1, "team-b": 2}) {
		t.Errorf("roles of the first owners of pair = %v, want team-a main and team-b co-owner", roles)
	}

	// Plants: each defect in the window must change an answer this test pins.
	exactWindow := ownershipwindow.Window(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"})
	plain := "o.valid_from <= {as_of:DateTime64(3, 'UTC')}\n      AND (o.valid_to IS NULL OR o.valid_to > {as_of:DateTime64(3, 'UTC')})"
	clampAll := strings.ReplaceAll(exactWindow, "greatest({as_of:DateTime64(3, 'UTC')}, ", "least(toDateTime64('2999-01-01 00:00:00', 3, 'UTC'), ")
	inactiveCounted := strings.ReplaceAll(exactWindow, ownershipwindow.ActiveTeam(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"}), "1 = 1")
	for _, plant := range []struct {
		name, window string
		at           time.Time
	}{
		{"clamp removed", plain, beforeAll},
		{"clamp on all rows", clampAll, later},
		{"inactive team counted in the first record", inactiveCounted, beforeAll},
	} {
		if plant.window == exactWindow {
			t.Fatalf("plant %q did not change the window", plant.name)
		}
		if got, want := owners(loadReposQuery(plant.window), plant.at), owners(shared, plant.at); reflect.DeepEqual(got, want) {
			t.Errorf("plant %q gave the same answer as the shared rule: %v", plant.name, got)
		}
	}
}
