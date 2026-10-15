//go:build integration

package home

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// CHAOS-9624, D6027: the tile in ISSUE_SIZE says what its share is a share of. A unit weighs the sum of its
// positive story points, an unsized unit (no size row, or a size of 0) counts 1 work unit, the newest size row of a
// unit wins, and the sizing fields count the distinct units of the same window the themes are read over. A unit that
// ends outside the window is in neither the shares nor the sizing.
func TestInvestmentByThemeIssueSizeCarriesDenominatorAndSizing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-9624-issue-size"
	const repo = "96240011-9624-4624-8624-962496249624"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/a', 'github', ?, ?, ?)`, repo, org, synced, synced); err != nil {
		t.Fatal(err)
	}
	end := day.AddDate(0, 0, 3).Add(12 * time.Hour)
	seedSize := func(unit string, size float64, computed time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`, org, unit, size, computed); err != nil {
			t.Fatalf("seed size %s: %v", unit, err)
		}
	}
	at := day.Add(10 * time.Hour)
	seedThemeUnit(t, ctx, admin, org, "wu-a", repo, at, 10, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	seedSize("wu-a", 13, at.Add(-48*time.Hour)) // older row: superseded
	seedSize("wu-a", 5, at.Add(time.Hour))
	seedThemeUnit(t, ctx, admin, org, "wu-b", repo, at, 10, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	seedSize("wu-b", 0, at.Add(time.Hour))                                                                      // a size of 0: unsized, counts 1
	seedThemeUnit(t, ctx, admin, org, "wu-c", repo, at, 10, map[string]float64{"maintenance.debt": 1}, "ok", 0) // no size row: unsized
	seedThemeUnit(t, ctx, admin, org, "wu-d", repo, at, 10, map[string]float64{"feature_delivery.roadmap": 1}, "ok", 0)
	seedSize("wu-d", 3, at.Add(time.Hour))
	seedThemeUnit(t, ctx, admin, org, "wu-out", repo, day.AddDate(0, 0, 20), 10, map[string]float64{"risk.security": 1}, "ok", 0)
	seedSize("wu-out", 9, at.Add(time.Hour))

	f := Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))}}
	startDay, endDay := day.AddDate(0, 0, -4), day.AddDate(0, 0, 4)
	rows, err := fetchThemeAllocationIn(ctx, client, f, org, startDay, endDay, end, investmentexplain.MeasureIssueSize)
	if err != nil {
		t.Fatalf("fetchThemeAllocationIn: %v", err)
	}
	want := map[string]float64{"quality": 6, "feature_delivery": 3, "maintenance": 1}
	if len(rows) != len(want) {
		t.Fatalf("themes = %+v, want %v", rows, want)
	}
	for _, row := range rows {
		if w, ok := want[row.Theme]; !ok || math.Abs(row.Allocation-w) > 1e-3 {
			t.Errorf("%s allocation = %v, want %v", row.Theme, row.Allocation, want[row.Theme])
		}
		if row.Measure != "ISSUE_SIZE" || math.Abs(row.Denominator-10) > 1e-3 {
			t.Errorf("%s measure/denominator = %s/%v, want ISSUE_SIZE/10", row.Theme, row.Measure, row.Denominator)
		}
		if row.SizingCoverage == nil || *row.SizingCoverage != 0.5 || row.UnsizedUnitCount == nil || *row.UnsizedUnitCount != 2 || row.WorkUnitCount == nil || *row.WorkUnitCount != 4 {
			t.Errorf("%s sizing = %v/%v/%v, want 0.5 coverage, 2 unsized of 4 units (wu-out is outside the window)", row.Theme, row.SizingCoverage, row.UnsizedUnitCount, row.WorkUnitCount)
		}
	}

	// Through the production entry point the tile is in ISSUE_SIZE: the same numbers and sizing as the direct read.
	resp, err := BuildResponse(ctx, client, nil, org, f, end)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if len(resp.ReworkThemeAllocation) != 3 {
		t.Fatalf("tile = %+v, want 3 themes", resp.ReworkThemeAllocation)
	}
	for _, row := range resp.ReworkThemeAllocation {
		if row.Measure != "ISSUE_SIZE" || math.Abs(row.Denominator-10) > 1e-3 || row.SizingCoverage == nil || *row.SizingCoverage != 0.5 || row.WorkUnitCount == nil || *row.WorkUnitCount != 4 {
			t.Errorf("row %s = %+v, want ISSUE_SIZE over 10 with 2 of 4 units sized", row.Theme, row)
		}
	}

	// The COUNT read of the same data: WORK_UNITS over 4 units, no sizing whatever sizes are stored.
	counted, err := fetchThemeAllocationIn(ctx, client, f, org, startDay, endDay, end, investmentexplain.MeasureCount)
	if err != nil {
		t.Fatalf("COUNT read: %v", err)
	}
	if len(counted) != 3 {
		t.Fatalf("COUNT tile = %+v, want 3 themes", counted)
	}
	for _, row := range counted {
		if row.Measure != "WORK_UNITS" || math.Abs(row.Denominator-4) > 1e-3 || row.SizingCoverage != nil || row.UnsizedUnitCount != nil || row.WorkUnitCount != nil {
			t.Errorf("COUNT row %s = %+v, want WORK_UNITS over 4 and no sizing", row.Theme, row)
		}
	}

	// A window with no unit lists no theme, so no sizing can be read as a measured zero.
	empty, err := fetchThemeAllocationIn(ctx, client, f, org, startDay.AddDate(0, 0, -400), startDay.AddDate(0, 0, -390), end, investmentexplain.MeasureIssueSize)
	if err != nil {
		t.Fatalf("empty window: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty window = %+v, want no row", empty)
	}
}
