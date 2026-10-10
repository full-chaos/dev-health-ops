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
	if g := ids(got.Drivers); g != "tm-both,tm-prior-only" {
		t.Errorf("drivers = %s, want tm-both,tm-prior-only (a group with no value in either window is not a driver)", g)
	}
	if g := ids(got.Contributors); g != "tm-both" {
		t.Errorf("contributors = %s, want tm-both (a contributor with no current value is not one)", g)
	}
	for _, d := range got.Drivers {
		if !d.HasData && !d.HasPriorData {
			t.Errorf("driver %s has no data on both sides", d.ID)
		}
	}
}
