//go:build integration

package explain

import (
	"context"
	"testing"
	"time"
)

// CHAOS-9111: the metric's own delta_pct is null when a window holds no stored
// value (the flags say which); two measured values keep their percent.
func TestExplainMetricDeltaIsNullWithoutDataInAWindow(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "metric-delta-no-data"
	cur := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prev := cur.AddDate(0, 0, -1)
	ins := func(team string, day time.Time, v float64) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily (day, work_scope_id, team_id, team_name, cycle_time_p50_hours, items_completed, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			day, "scope-"+team, team, team, v, uint32(1), cur.AddDate(0, 0, 3), org); err != nil {
			t.Fatal(err)
		}
	}
	ins("tm", cur, 48)
	for _, tc := range []struct {
		name               string
		prior              bool
		hasPrior, wantNull bool
		wantPct            float64
	}{
		{"no prior window", false, false, true, 0},
		{"two measured windows", true, true, false, 100},
	} {
		if tc.prior {
			ins("tm", prev, 24)
		}
		got, err := BuildExplainResponse(ctx, reader, org, Params{Metric: "cycle_time", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur})
		if err != nil {
			t.Fatal(err)
		}
		if got.HasData != true || got.HasPriorData != tc.hasPrior {
			t.Fatalf("%s: has_data %v has_prior_data %v, want true %v", tc.name, got.HasData, got.HasPriorData, tc.hasPrior)
		}
		if tc.wantNull && got.DeltaPct != nil {
			t.Errorf("%s: delta_pct = %v, want null", tc.name, *got.DeltaPct)
		}
		if !tc.wantNull && (got.DeltaPct == nil || *got.DeltaPct != tc.wantPct) {
			t.Errorf("%s: delta_pct = %v, want %v", tc.name, got.DeltaPct, tc.wantPct)
		}
	}
}
