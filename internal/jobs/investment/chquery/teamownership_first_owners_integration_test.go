//go:build integration

package chquery

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
)

// D5987 option C (CHAOS-10518): the donor set tests an ownership row of a repository at the later of the unit's instant
// and the first record of the repository (package ownershipwindow). An instant at or after the first record reads as
// before, an instant in a gap between records has no owner, and an inactive team's row does not set the first record.
func TestTheDonorSetAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	repoIDs := map[string]string{
		"pair": "a1000000-0000-4000-8000-000000000001", "gap": "a1000000-0000-4000-8000-000000000002",
		"grant-only": "a1000000-0000-4000-8000-000000000003", "revoked": "a1000000-0000-4000-8000-000000000004",
		"tie": "a1000000-0000-4000-8000-000000000005", "future": "a1000000-0000-4000-8000-000000000006",
	}
	for name, id := range repoIDs {
		mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,?,'github',?)`, id, "acme/"+name, orgAlpha)
	}
	for _, team := range []struct {
		id     string
		active uint8
	}{{"team-a", 1}, {"team-b", 1}, {"team-c", 1}, {"team-d", 1}, {"team-gone", 0}} {
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES (?,generateUUIDv4(),?,'github',?,?,?)`,
			team.id, team.id, team.active, synced, orgAlpha)
	}
	endedApril, endedJanuary, goneEnded := day(time.April, 1), day(time.January, 20), day(time.February, 20)
	type row struct {
		team, repo string
		byID       bool
		primary    uint8
		share      float64
		from       time.Time
		to         *time.Time
	}
	rows := []row{
		{"team-a", "pair", true, 1, 0.6, day(time.March, 1), nil},
		{"team-b", "pair", true, 2, 0.4, day(time.March, 1), nil},
		{"team-a", "gap", false, 1, 1, day(time.March, 1), &endedApril},
		{"team-c", "gap", false, 1, 1, day(time.May, 1), nil},
		// grant-only: an inactive team's earlier closed row does not set the first record.
		{"team-gone", "grant-only", false, 1, 1, day(time.February, 1), &goneEnded},
		{"team-a", "grant-only", false, 1, 1, day(time.March, 1), nil},
		// revoked: a row that ended before it began is never valid.
		{"team-d", "revoked", false, 1, 1, day(time.February, 1), &endedJanuary},
		{"team-a", "revoked", false, 1, 1, day(time.March, 1), nil},
		// future: the only record begins after the run's bound (upTo), so the row that holds at the first record is read.
		{"team-a", "future", false, 1, 1, day(time.October, 30), nil},
		{"team-a", "tie", false, 1, 0.5, day(time.March, 1), nil},
		{"team-b", "tie", false, 1, 0.5, day(time.March, 1), nil},
	}
	for _, r := range rows {
		var repoID any
		if r.byID {
			repoID = uuid.MustParse(repoIDs[r.repo])
		}
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership
			(org_id,provider,team_id,repo_id,repo_full_name,match_type,source,is_primary,share,valid_from,valid_to,updated_at)
			VALUES (?,'github',?,?,?,'exact','provider_access',?,?,?,?,?)`,
			orgAlpha, r.team, repoID, "acme/"+r.repo, r.primary, r.share, r.from, r.to, synced)
	}
	// One work item per donor team: its primary attribution names the team.
	var ids []string
	for _, team := range []string{"team-a", "team-b", "team-c", "team-d"} {
		id := "wi-" + team
		ids = append(ids, id)
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at)
			VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'github',?,'native_team',1,'high','fixture',?)`, orgAlpha, id, team, synced)
	}

	upTo := day(time.September, 21)
	set, err := reader.FetchTeamRepoDonorSet(ctx, ids, orgAlpha, upTo)
	if err != nil {
		t.Fatal(err)
	}
	repoOf := func(name string) uuid.UUID { return uuid.MustParse(repoIDs[name]) }
	for name, want := range map[string]time.Time{
		"pair": day(time.March, 1), "gap": day(time.March, 1), "grant-only": day(time.March, 1),
		"revoked": day(time.February, 1), "tie": day(time.March, 1), "future": day(time.October, 30),
	} {
		if got := set.FirstByRepo[repoOf(name)]; !got.Equal(want) {
			t.Errorf("first record of %s = %v, want %v", name, got, want)
		}
	}

	beforeAll, atFirst, inGap, later := day(time.January, 15), day(time.March, 1), day(time.April, 15), upTo
	// The oracle reads ownership straight from the shared window, renders it in SQL and keeps the rows of the donor teams.
	oracle := func(window string, at time.Time) []string {
		t.Helper()
		query := `SELECT DISTINCT o.team_id, coalesce(toString(o.repo_id), toString(r.id))
			FROM team_repo_ownership AS o FINAL
			LEFT JOIN (SELECT org_id, provider, id, repo, 1 AS matched FROM repos FINAL WHERE org_id = {org_id:String}) AS r
				ON r.org_id = o.org_id AND r.provider = o.provider AND lower(r.repo) = lower(o.repo_full_name)
			WHERE o.org_id = {org_id:String} AND o.team_id != '' AND (o.repo_id IS NOT NULL OR r.matched = 1) AND ` + window
		out, err := conn.Query(ctx, query, clickhouse.Named("org_id", orgAlpha), clickhouse.Named("as_of", at.UTC().Format("2006-01-02 15:04:05.000")))
		if err != nil {
			t.Fatalf("oracle: %v", err)
		}
		defer func() { _ = out.Close() }()
		var keys []string
		for out.Next() {
			var team, repo string
			if err := out.Scan(&team, &repo); err != nil {
				t.Fatal(err)
			}
			if team == "team-gone" {
				continue
			}
			keys = append(keys, "wi-"+team+"/"+team+"/"+repo)
		}
		sort.Strings(keys)
		return keys
	}
	names := ownershipwindow.Names{Org: "org_id", AsOf: "as_of"}
	shared := ownershipwindow.Window(names)
	fromSet := func(s TeamRepoDonorSet, at time.Time) []string {
		var keys []string
		for _, id := range ids {
			keys = append(keys, donorKeys(s.DonorsAt(id, at))...)
		}
		sort.Strings(keys)
		return keys
	}
	key := func(team, repo string) string { return "wi-" + team + "/" + team + "/" + repoIDs[repo] }
	for _, tc := range []struct {
		name string
		at   time.Time
		want []string
	}{
		{"before every record", beforeAll, []string{
			key("team-a", "pair"), key("team-b", "pair"), key("team-a", "gap"), key("team-a", "grant-only"),
			key("team-a", "tie"), key("team-b", "tie"), key("team-a", "future"),
		}},
		{"at the first record", atFirst, []string{
			key("team-a", "pair"), key("team-b", "pair"), key("team-a", "gap"), key("team-a", "grant-only"),
			key("team-a", "revoked"), key("team-a", "tie"), key("team-b", "tie"), key("team-a", "future"),
		}},
		{"in a gap between records", inGap, []string{
			key("team-a", "pair"), key("team-b", "pair"), key("team-a", "grant-only"), key("team-a", "revoked"),
			key("team-a", "tie"), key("team-b", "tie"), key("team-a", "future"),
		}},
		{"after the records", later, []string{
			key("team-a", "pair"), key("team-b", "pair"), key("team-c", "gap"), key("team-a", "grant-only"),
			key("team-a", "revoked"), key("team-a", "tie"), key("team-b", "tie"), key("team-a", "future"),
		}},
	} {
		sort.Strings(tc.want)
		got := fromSet(set, tc.at)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: the donor set gives %v, want %v", tc.name, got, tc.want)
		}
		if want := oracle(shared, tc.at); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: the donor set gives %v, the shared window gives %v", tc.name, got, want)
		}
	}

	// Shares (D5988): before the first record the donor carries the share of the first record's row.
	shares := map[string]float64{}
	for _, donor := range set.DonorsAt("wi-team-a", beforeAll) {
		if donor.Share != nil {
			shares[donor.RepoID.String()] = *donor.Share
		}
	}
	if got := shares[repoIDs["pair"]]; math.Abs(got-0.6) > 1e-9 {
		t.Errorf("share of team-a on pair before the first record = %v, want 0.6", got)
	}
	var tieShare *float64
	for _, donor := range set.DonorsAt("wi-team-b", beforeAll) {
		if donor.RepoID == repoOf("tie") {
			tieShare = donor.Share
		}
	}
	if tieShare == nil || math.Abs(*tieShare-0.5) > 1e-9 {
		t.Errorf("a full tie before the first record keeps both owners at 0.5 (D5990), team-b has %v", tieShare)
	}
	// A row revoked before it began owns nothing: team-d's only row never held.
	if got := fromSet(set, beforeAll); strings.Contains(strings.Join(got, ","), "wi-team-d") {
		t.Errorf("a row revoked before it began owns before the first record: %v", got)
	}

	// Plants: each defect must change an answer this test pins.
	clampRemoved := set
	clampRemoved.FirstByRepo = nil
	if reflect.DeepEqual(fromSet(clampRemoved, beforeAll), fromSet(set, beforeAll)) {
		t.Errorf("plant (clamp removed) gave the answer of the shared rule")
	}
	// Clamp on all rows: every row starts at its repository's first record, so a later record (team-c on gap) holds
	// in the gap before its own start.
	clampAll := set
	clampAll.IntervalsByTeam = map[string][]TeamRepoInterval{}
	for team, intervals := range set.IntervalsByTeam {
		for _, interval := range intervals {
			if first, ok := set.FirstByRepo[interval.RepoID]; ok && first.Before(interval.ValidFrom) {
				interval.ValidFrom = first
			}
			clampAll.IntervalsByTeam[team] = append(clampAll.IntervalsByTeam[team], interval)
		}
	}
	if reflect.DeepEqual(fromSet(clampAll, inGap), fromSet(set, inGap)) {
		t.Errorf("plant (clamp on all rows) gave the answer of the shared rule")
	}
	inactiveCounted := ownershipwindow.FirstRecordsQuery(names)
	activeTeam := ownershipwindow.ActiveTeam(names)
	if !strings.Contains(inactiveCounted, activeTeam) {
		t.Fatalf("the first-record statement has no active-team predicate to plant")
	}
	counted, err := reader.readFirstRecords(ctx, strings.ReplaceAll(inactiveCounted, activeTeam, "1 = 1"), orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	plant := set
	plant.FirstByRepo = counted
	if reflect.DeepEqual(fromSet(plant, beforeAll), fromSet(set, beforeAll)) {
		t.Errorf("plant (inactive team counted in the first record) gave the answer of the shared rule")
	}
	// The same plants on the SQL window.
	plainWindow := "o.valid_from <= {as_of:DateTime64(3, 'UTC')} AND (o.valid_to IS NULL OR o.valid_to > {as_of:DateTime64(3, 'UTC')})"
	clampAllWindow := strings.ReplaceAll(shared, "greatest({as_of:DateTime64(3, 'UTC')}, ", "least({as_of:DateTime64(3, 'UTC')}, ")
	inactiveWindow := strings.ReplaceAll(shared, activeTeam, "1 = 1")
	for name, tc := range map[string]struct {
		window string
		at     time.Time
	}{
		"clamp removed": {plainWindow, beforeAll}, "clamp on all rows": {clampAllWindow, later}, "inactive team counted": {inactiveWindow, beforeAll},
	} {
		if reflect.DeepEqual(oracle(tc.window, tc.at), oracle(shared, tc.at)) {
			t.Errorf("SQL plant %q gave the answer of the shared rule: %s", name, fmt.Sprint(oracle(tc.window, tc.at)))
		}
	}
}
