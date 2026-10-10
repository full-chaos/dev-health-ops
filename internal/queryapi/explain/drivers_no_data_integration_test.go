//go:build integration

package explain

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// CHAOS-9101: a driver or contributor with no stored value on either side is
// not a driver. One with a value in only one window stays (the flags say which).
func TestExplainOmitsGroupsWithNoDataOnBothSides(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "drivers-no-data"
	cur := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prev := cur.AddDate(0, 0, -1)
	ins := func(team string, day time.Time, v any) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily (day, work_scope_id, team_id, team_name, cycle_time_p50_hours, items_completed, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			day, "scope-"+team, team, team, v, uint32(1), cur.AddDate(0, 0, 3), org); err != nil {
			t.Fatal(err)
		}
	}
	ins("tm-both", cur, 48.0)
	ins("tm-both", prev, 24.0)
	ins("tm-nodata-now", cur, nil) // no value in either window
	ins("tm-nodata-both", cur, nil)
	ins("tm-nodata-both", prev, nil)
	ins("tm-prior-only", cur, nil) // value only in the comparison window
	ins("tm-prior-only", prev, 12.0)
	ins("tm-current-only", cur, 30.0) // value now, no comparison row
	got, err := BuildExplainResponse(ctx, reader, org, Params{Metric: "cycle_time", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur})
	if err != nil {
		t.Fatal(err)
	}
	ids := func(items []Contributor) string {
		var out []string
		for _, c := range items {
			out = append(out, c.ID)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	if g := ids(got.Drivers); g != "tm-both,tm-current-only,tm-prior-only" {
		t.Errorf("drivers = %s, want tm-both,tm-current-only,tm-prior-only (a group with no value in either window is not a driver; a value in one window is enough)", g)
	}
	if g := ids(got.Contributors); g != "tm-both,tm-current-only" {
		t.Errorf("contributors = %s, want tm-both,tm-current-only (a contributor with no current value is not one)", g)
	}
	for _, d := range got.Drivers {
		if !d.HasData && !d.HasPriorData {
			t.Errorf("driver %s has no data on both sides", d.ID)
		}
	}
}

// CHAOS-9121 (D5856): a group with a stored value in the comparison window and
// no row at all in the current window is the same state as one whose current
// value is NULL: listed, value 0 as the placeholder, has_data false,
// has_prior_data true, delta null.
func TestExplainListsAGroupWithAPriorRowAndNoCurrentRow(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "drivers-prior-no-current-row"
	cur := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prev := cur.AddDate(0, 0, -1)
	ins := func(repo string, day time.Time, churn uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			repo, day, churn, cur.AddDate(0, 0, 3), org); err != nil {
			t.Fatal(err)
		}
	}
	ins("11111111-1111-1111-1111-111111111111", cur, 5) // both windows
	ins("11111111-1111-1111-1111-111111111111", prev, 10)
	ins("22222222-2222-2222-2222-222222222222", prev, 7) // prior row only
	got, err := BuildExplainResponse(ctx, reader, org, Params{Metric: "churn", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Drivers) != 2 {
		t.Fatalf("drivers = %+v, want the two groups", got.Drivers)
	}
	byID := map[string]Contributor{}
	for _, d := range got.Drivers {
		byID[d.ID] = d
	}
	gone, ok := byID["22222222-2222-2222-2222-222222222222"]
	if !ok {
		t.Fatalf("the group with a prior row and no current row is absent: %+v", got.Drivers)
	}
	if gone.HasData || !gone.HasPriorData || gone.DeltaPct != nil || gone.Value != 0 {
		t.Errorf("prior-only driver = %+v (delta %v), want value 0, has_data false, has_prior_data true, delta null", gone, gone.DeltaPct)
	}
	if both := byID["11111111-1111-1111-1111-111111111111"]; both.DeltaPct == nil || !near(*both.DeltaPct, -50) || !both.HasData || !both.HasPriorData {
		t.Errorf("measured driver = %+v (delta %v), want -50 with both flags", both, both.DeltaPct)
	}
	if got.Drivers[0].ID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("the driver without a delta ranks first: %+v", got.Drivers)
	}
	for _, c := range got.Contributors {
		if c.ID == "22222222-2222-2222-2222-222222222222" {
			t.Errorf("a contributor has a current side only: %+v", c)
		}
	}
}

// The driver LIMIT is 3 and the union adds candidates: groups that hold a delta
// take the places first, a prior-only group (no delta) takes the one that is
// left, and ranks after them.
func TestExplainPriorOnlyGroupsTakeTheLimitSetAfterGroupsWithADelta(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "drivers-prior-only-limit-set"
	cur := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prev := cur.AddDate(0, 0, -1)
	ins := func(repo string, day time.Time, churn uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			repo, day, churn, cur.AddDate(0, 0, 3), org); err != nil {
			t.Fatal(err)
		}
	}
	for i, repo := range []string{"aaaaaaaa-0000-0000-0000-000000000001", "aaaaaaaa-0000-0000-0000-000000000002"} { // two groups with a delta
		ins(repo, cur, 10)
		ins(repo, prev, uint32(10+10*(i+1)))
	}
	ins("bbbbbbbb-0000-0000-0000-000000000001", prev, 3) // two prior-only groups
	ins("bbbbbbbb-0000-0000-0000-000000000002", prev, 4)
	got, err := BuildExplainResponse(ctx, reader, org, Params{Metric: "churn", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Drivers) != 3 {
		t.Fatalf("drivers = %+v, want the 3 places", got.Drivers)
	}
	for _, d := range got.Drivers[:2] {
		if d.DeltaPct == nil {
			t.Errorf("a driver without a delta ranks before a driver with one: %+v", got.Drivers)
		}
	}
	if last := got.Drivers[2]; last.ID != "bbbbbbbb-0000-0000-0000-000000000001" && last.ID != "bbbbbbbb-0000-0000-0000-000000000002" || last.HasData || !last.HasPriorData || last.DeltaPct != nil {
		t.Errorf("third driver = %+v, want a prior-only group (has_data false, has_prior_data true, delta null)", last)
	}
}

// The same state for a metric stored per team with a Nullable column and an avg
// aggregator (cycle_time): a team with a prior value and no current row is a
// driver with has_data false, and a team with no value on either side is not.
func TestExplainListsATeamWithAPriorRowAndNoCurrentRow(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "drivers-team-prior-no-current-row"
	cur := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prev := cur.AddDate(0, 0, -1)
	ins := func(team string, day time.Time, v any) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily (day, work_scope_id, team_id, team_name, cycle_time_p50_hours, items_completed, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			day, "scope-"+team, team, team, v, uint32(1), cur.AddDate(0, 0, 3), org); err != nil {
			t.Fatal(err)
		}
	}
	ins("tm-gone", prev, 48.0)
	ins("tm-gone-undefined", prev, nil) // a prior row with no value: not a driver
	got, err := BuildExplainResponse(ctx, reader, org, Params{Metric: "cycle_time", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Drivers) != 1 {
		t.Fatalf("drivers = %+v, want only tm-gone", got.Drivers)
	}
	if d := got.Drivers[0]; d.ID != "tm-gone" || d.HasData || !d.HasPriorData || d.DeltaPct != nil || d.Value != 0 {
		t.Errorf("driver = %+v (delta %v), want tm-gone with value 0, has_data false, has_prior_data true, delta null", d, d.DeltaPct)
	}
}
