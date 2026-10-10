//go:build integration

package home

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestThemeAllocationOfANamedRetiredTeamGivesRetractionRowsNoWeight reads the
// allocation by theme for a team id the caller names, where that id was
// retired (package retractionseed).
//
// With the measured day in the window the themes must be those of the control
// organization. With retraction rows only the read must give no theme: before
// the rule it listed the theme of each retracted key with an allocation of 0.
func TestThemeAllocationOfANamedRetiredTeamGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	end := store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	read := func(org, teamID string, from time.Time) []ReworkThemeAllocation {
		t.Helper()
		ids := []string{teamID}
		rows, err := fetchReworkThemeAllocation(ctx, client, from, end,
			scopeClauseMulti(ids, "team_id"), scopeBindingsMulti(ids), "", nil, org)
		if err != nil {
			t.Fatalf("%s %s: %v", org, teamID, err)
		}
		return rows
	}

	if none := read(retractionseed.ControlOrg, "no-such-team", store.Days[0]); len(none) != 0 {
		t.Fatalf("an id with no row = %+v, want no theme", none)
	}
	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: on its one measured day the
		// retired id completed 2+index work items in one theme, merged
		// 1+index pull requests and churned 100*(1+index) lines.
		control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[0])
		if len(control) != 1 || control[0].Theme != "feature_delivery" || control[0].Allocation != float64(2+index) ||
			control[0].PRsMerged != int64(1+index) || control[0].ChurnLOC != int64(100*(1+index)) {
			t.Fatalf("%s control %s = %+v", team.Provider, team.RetiredID, control)
		}
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[0]); !reflect.DeepEqual(retracted, control) {
			t.Errorf("%s: the retraction rows changed the allocation of %s:\n control   %+v\n retracted %+v",
				team.Provider, team.RetiredID, control, retracted)
		}
		// The window of the days computed again: retraction rows only.
		if control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[1]); len(control) != 0 {
			t.Fatalf("%s control %s in the window of the days computed again = %+v, want no theme",
				team.Provider, team.RetiredID, control)
		}
		if recomputed := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[1]); len(recomputed) != 0 {
			t.Errorf("%s: %s holds retraction rows only in the window and reads as %+v, want no theme",
				team.Provider, team.RetiredID, recomputed)
		}
	}
}

// TestATheMeasuredAreaOfAThemeStaysWhenAnotherAreaOfItIsRetracted holds the
// order of the rule and the theme step. Two stored investment areas of one
// repository, team, day and stream map to ONE theme. Both are measured; then
// a newer retraction row is stored over one of them.
//
// The rule is of the key the writer writes (the area), so only the retracted
// area leaves: the theme stays, with the numbers of the measured area. With
// the rule applied after the theme step, the newer retraction row was the row
// of the theme, and the whole theme left the list.
//
// The theme step itself is the reference's and is not this test's subject:
// among the areas of a theme it takes the row with the newest compute time,
// so with both areas measured the allocation is the newer area's 5, not 8.
func TestATheMeasuredAreaOfAThemeStaysWhenAnotherAreaOfItIsRetracted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const (
		control   = "c0c0c0c0-0000-4000-8000-0000000090a1"
		retracted = "c0c0c0c0-0000-4000-8000-0000000090a2"
		recounted = "c0c0c0c0-0000-4000-8000-0000000090a3"
		repo      = "55555555-5555-4555-8555-555555555555"
		team      = "jira:ENG"
	)
	day := store.Days[1]
	first, second, third := store.OldComputedAt, store.OldComputedAt.Add(time.Minute), store.NewComputedAt
	measure := func(org, area string, completed uint32, computedAt time.Time) {
		t.Helper()
		if err := store.Conn.Exec(ctx, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, computed_at)
VALUES (?, ?, ?, ?, ?, 'roadmap', ?, ?, 1, 10, ?)`, org, day, repo, team, area, completed, completed, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	for _, org := range []string{control, retracted} {
		measure(org, "feature_delivery.customer", 3, first)
		measure(org, "feature_delivery.enablement", 5, second)
	}
	// The newest row of the organization: a retraction over ONE area.
	if err := store.Conn.Exec(ctx, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, computed_at)
VALUES (?, ?, ?, ?, 'feature_delivery.customer', 'roadmap', ?)`, retracted, day, repo, team, third); err != nil {
		t.Fatal(err)
	}

	// A third organization: the first area was computed again LAST (7 items).
	// The theme step takes the area whose NEWEST row is the newest.
	measure(recounted, "feature_delivery.customer", 3, first)
	measure(recounted, "feature_delivery.enablement", 5, second)
	measure(recounted, "feature_delivery.customer", 7, third)

	read := func(org string) []ReworkThemeAllocation {
		t.Helper()
		rows, err := fetchReworkThemeAllocation(ctx, client, day, day.AddDate(0, 0, 1), "", nil, "", nil, org)
		if err != nil {
			t.Fatalf("%s: %v", org, err)
		}
		return rows
	}
	want := []ReworkThemeAllocation{{Theme: "feature_delivery", Label: "Feature Delivery", Allocation: 5, AllocationPct: 100, PRsMerged: 1, ChurnLOC: 10}}
	if got := read(control); !reflect.DeepEqual(got, want) {
		t.Fatalf("control (both areas measured) = %+v, want %+v", got, want)
	}
	if got := read(retracted); !reflect.DeepEqual(got, want) {
		t.Errorf("with a retraction over one area = %+v, want the measured area of the theme %+v", got, want)
	}
	want[0].Allocation = 7
	if got := read(recounted); !reflect.DeepEqual(got, want) {
		t.Errorf("with one area computed again last = %+v, want that area's newest row %+v", got, want)
	}
}

// TestARetractionOverOneStoredKeyLeavesTheMeasuredKeyBesideIt holds each
// column of the key the rule is applied to: the key the writer writes (day,
// repository, team, investment area, project stream). One key is measured
// (5 items). Then a NEWER retraction row is stored over a sibling key that
// differs from it in ONE column. The measured key stays, whichever column
// differs: a rule judged on a key that lacks that column would take the
// newer retraction row as the row of both keys and drop the measured one.
//
// The last case is the rule itself: a newer retraction row over the SAME key
// leaves no theme.
func TestARetractionOverOneStoredKeyLeavesTheMeasuredKeyBesideIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	type storedKey struct {
		day                      time.Time
		repo, team, area, stream string
	}
	measured := storedKey{
		day: store.Days[1], repo: "55555555-5555-4555-8555-555555555555",
		team: "jira:ENG", area: "feature_delivery.customer", stream: "roadmap",
	}
	sibling := func(change func(*storedKey)) storedKey {
		key := measured
		change(&key)
		return key
	}
	theme := []ReworkThemeAllocation{{Theme: "feature_delivery", Label: "Feature Delivery", Allocation: 5, AllocationPct: 100, PRsMerged: 1, ChurnLOC: 10}}
	cases := []struct {
		column    string
		org       string
		retracted storedKey
		want      []ReworkThemeAllocation
	}{
		{"repository", "c0c0c0c0-0000-4000-8000-0000000090b1",
			sibling(func(key *storedKey) { key.repo = "66666666-6666-4666-8666-666666666666" }), theme},
		{"team", "c0c0c0c0-0000-4000-8000-0000000090b2",
			sibling(func(key *storedKey) { key.team = "jira:OPS" }), theme},
		{"project stream", "c0c0c0c0-0000-4000-8000-0000000090b3",
			sibling(func(key *storedKey) { key.stream = "operations" }), theme},
		{"day", "c0c0c0c0-0000-4000-8000-0000000090b4",
			sibling(func(key *storedKey) { key.day = store.Days[2] }), theme},
		{"investment area", "c0c0c0c0-0000-4000-8000-0000000090b5",
			sibling(func(key *storedKey) { key.area = "feature_delivery.enablement" }), theme},
		{"no column: the same key", "c0c0c0c0-0000-4000-8000-0000000090b6", measured, nil},
	}
	for _, testCase := range cases {
		if err := store.Conn.Exec(ctx, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, computed_at)
VALUES (?, ?, ?, ?, ?, ?, 5, 5, 1, 10, ?)`,
			testCase.org, measured.day, measured.repo, measured.team, measured.area, measured.stream, store.OldComputedAt); err != nil {
			t.Fatal(err)
		}
		key := testCase.retracted
		if err := store.Conn.Exec(ctx, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			testCase.org, key.day, key.repo, key.team, key.area, key.stream, store.NewComputedAt); err != nil {
			t.Fatal(err)
		}
	}
	// The window holds both days, so the sibling of the day case is read too.
	from, to := store.Days[1], store.Days[2].AddDate(0, 0, 1)
	for _, testCase := range cases {
		got, err := fetchReworkThemeAllocation(ctx, client, from, to, "", nil, "", nil, testCase.org)
		if err != nil {
			t.Fatalf("%s: %v", testCase.column, err)
		}
		if len(got) == 0 && len(testCase.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("a retraction over a key that differs in the %s: got %+v, want %+v", testCase.column, got, testCase.want)
		}
	}
}
