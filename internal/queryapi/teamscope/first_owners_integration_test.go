//go:build integration

package teamscope_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// D5987 option C (CHAOS-9404): before the first recorded ownership of a repository the first recorded owners apply, in
// every reader of this package (RepoCondition and OwnerShares render one ownership statement). An instant in a gap
// between two records has no owner. A team that is inactive owns nothing (team-attribution.md section 0.4g rule 1),
// here as in the readers of package teamownership, and does not set the first record.
func TestTeamScopeAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "team-scope-first-owners"
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	repos := map[string]string{
		"pair":       "00000000-0000-4000-8100-000000000001",
		"gap":        "00000000-0000-4000-8100-000000000002",
		"grant-only": "00000000-0000-4000-8100-000000000003",
		"gone-only":  "00000000-0000-4000-8100-000000000004",
	}
	for name, id := range repos {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, "acme/"+name, org, synced, synced)
	}
	crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
        VALUES ('team-gone', generateUUIDv4(), 'Gone', [], ?, ?, ?, 'github', 0)`, synced, synced, org)
	ended := day(time.April, 1)
	for _, r := range []struct {
		team, repo string
		from       time.Time
		to         *time.Time
	}{
		{"team-a", "pair", day(time.March, 1), nil},
		{"team-b", "pair", day(time.March, 1), nil},
		{"team-a", "gap", day(time.March, 1), &ended},
		{"team-c", "gap", day(time.May, 1), nil},
		{"team-gone", "grant-only", day(time.February, 1), nil},
		{"team-a", "grant-only", day(time.March, 1), nil},
		{"team-gone", "gone-only", day(time.February, 1), nil},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, ?, ?, NULL)`,
			org, r.team, repos[r.repo], "acme/"+r.repo, r.from, r.to, synced)
	}

	read := func(teams []string, at time.Time) map[string]bool {
		t.Helper()
		condition, bindings := teamscope.RepoCondition(org, "id", teams, at)
		rows, err := client.Query(ctx, "SELECT toString(id) FROM repos FINAL WHERE org_id = '"+org+"' AND "+condition, bindings)
		if err != nil {
			t.Fatalf("RepoCondition query: %v", err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			got[id] = true
		}
		return got
	}
	shares := func(at time.Time) map[string]bool {
		t.Helper()
		source, bindings := teamscope.OwnerShares(org, nil, at)
		rows, err := client.Query(ctx, "SELECT owner_repo_id, owner_team_id FROM "+source+" AS s", bindings)
		if err != nil {
			t.Fatalf("OwnerShares query: %v", err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var repo, team string
			if err := rows.Scan(&repo, &team); err != nil {
				t.Fatal(err)
			}
			got[repo+"|"+team] = true
		}
		return got
	}

	beforeAll := day(time.February, 15)
	inGap := day(time.April, 15)
	later := day(time.September, 21)
	pair, gap, grantOnly, goneOnly := repos["pair"], repos["gap"], repos["grant-only"], repos["gone-only"]

	for _, tc := range []struct {
		name string
		at   time.Time
		team string
		want map[string]bool
	}{
		{"team-a before every record", beforeAll, "team-a", map[string]bool{pair: true, gap: true, grantOnly: true}},
		{"team-b before every record", beforeAll, "team-b", map[string]bool{pair: true}},
		{"team-c before every record: not a first owner", beforeAll, "team-c", map[string]bool{}},
		{"team-gone owns nothing", beforeAll, "team-gone", map[string]bool{}},
		{"team-a in the gap of gap", inGap, "team-a", map[string]bool{pair: true, grantOnly: true}},
		{"team-c after its start", later, "team-c", map[string]bool{gap: true}},
		{"team-a after the records", later, "team-a", map[string]bool{pair: true, grantOnly: true}},
	} {
		got := read([]string{tc.team}, tc.at)
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("RepoCondition: %s = %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := shares(beforeAll); !got[pair+"|team-a"] || !got[pair+"|team-b"] || !got[gap+"|team-a"] || got[gap+"|team-c"] || !got[grantOnly+"|team-a"] || got[grantOnly+"|team-gone"] || got[goneOnly+"|team-gone"] {
		t.Errorf("OwnerShares before every record = %v", got)
	}
	if got := shares(inGap); got[gap+"|team-a"] || got[gap+"|team-c"] {
		t.Errorf("OwnerShares in the gap of one repository = %v: no owner in a gap between records", got)
	}
}
