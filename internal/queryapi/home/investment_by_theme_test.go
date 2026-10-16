package home

import (
	"github.com/full-chaos/dev-health-go/readers"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// Round 126 r1 P1 #2: a dense canonical distribution carries zero-weight themes; the tile lists
// only the themes with positive effort, as the Investment page does.
func TestThemeAllocationDropsZeroWeightThemes(t *testing.T) {
	rows := []investmentexplain.ThemeEffortRow{
		{Theme: "feature_delivery", Value: 100},
		{Theme: "maintenance", Value: 0},
		{Theme: "operational", Value: 0},
		{Theme: "quality", Value: 0},
		{Theme: "risk", Value: 0},
	}
	got := themeAllocationFromEffort(rows, investmentexplain.MeasureCount, nil)
	if len(got) != 1 || got[0].Theme != "feature_delivery" || got[0].AllocationPct != 100 {
		t.Fatalf("tile = %+v, want only feature_delivery at 100%%", got)
	}
}

func themeRows() []investmentexplain.ThemeEffortRow {
	return []investmentexplain.ThemeEffortRow{
		{Theme: "feature_delivery", Value: 6},
		{Theme: "quality", Value: 2},
		{Theme: "maintenance", Value: 0},
	}
}

// The tile says what its share is of: the measure, the total allocation_pct divides by (the same on every row)
// and, in ISSUE_SIZE, the sizing of the units (CHAOS-9624, D6027). Unsized units are named, not hidden.
func TestThemeAllocationCarriesMeasureDenominatorAndSizing(t *testing.T) {
	sizing := &readers.SizingCoverage{SizedUnits: 2, Units: 8}
	got := themeAllocationFromEffort(themeRows(), investmentexplain.MeasureIssueSize, sizing)
	if len(got) != 2 {
		t.Fatalf("rows = %+v, want 2 themes", got)
	}
	for _, row := range got {
		if row.Measure != "ISSUE_SIZE" || row.Denominator != 8 {
			t.Errorf("row %s measure/denominator = %s/%v, want ISSUE_SIZE/8 (zero-weight theme excluded)", row.Theme, row.Measure, row.Denominator)
		}
		if row.SizingCoverage == nil || *row.SizingCoverage != 0.25 || row.UnsizedUnitCount == nil || *row.UnsizedUnitCount != 6 || row.WorkUnitCount == nil || *row.WorkUnitCount != 8 {
			t.Errorf("row %s sizing = %v/%v/%v, want 0.25 / 6 unsized / 8 units", row.Theme, row.SizingCoverage, row.UnsizedUnitCount, row.WorkUnitCount)
		}
	}
	if got[0].Allocation/got[0].Denominator*100 != got[0].AllocationPct {
		t.Errorf("allocation_pct %v is not allocation / denominator", got[0].AllocationPct)
	}
}

// COUNT serves WORK_UNITS and no sizing, even when a sizing value is passed in: null is "not measured".
func TestThemeAllocationCountCarriesNoSizing(t *testing.T) {
	got := themeAllocationFromEffort(themeRows(), investmentexplain.MeasureCount, &readers.SizingCoverage{SizedUnits: 1, Units: 2})
	for _, row := range got {
		if row.Measure != "WORK_UNITS" || row.SizingCoverage != nil || row.UnsizedUnitCount != nil || row.WorkUnitCount != nil {
			t.Errorf("COUNT row = %+v, want WORK_UNITS and no sizing", row)
		}
	}
}

// A window with no unit in ISSUE_SIZE is not measured: nil, never a 0 that reads as "nothing sized"; and
// all-unsized units are a measured 0 coverage with every unit named.
func TestThemeAllocationSizingNilWithoutUnitsAndZeroWhenNoneSized(t *testing.T) {
	none := themeAllocationFromEffort(themeRows(), investmentexplain.MeasureIssueSize, &readers.SizingCoverage{})
	for _, row := range none {
		if row.SizingCoverage != nil || row.UnsizedUnitCount != nil || row.WorkUnitCount != nil {
			t.Errorf("no-unit row = %+v, want nil sizing", row)
		}
	}
	unsized := themeAllocationFromEffort(themeRows(), investmentexplain.MeasureIssueSize, &readers.SizingCoverage{SizedUnits: 0, Units: 4})
	r := unsized[0]
	if r.SizingCoverage == nil || *r.SizingCoverage != 0 || r.UnsizedUnitCount == nil || *r.UnsizedUnitCount != 4 || r.WorkUnitCount == nil || *r.WorkUnitCount != 4 {
		t.Errorf("all-unsized row = %+v, want measured coverage 0, 4 unsized of 4", r)
	}
}

// A zero denominator lists no row (nothing to label), so no 0/0 share reaches the wire.
func TestThemeAllocationZeroDenominatorListsNothing(t *testing.T) {
	got := themeAllocationFromEffort([]investmentexplain.ThemeEffortRow{{Theme: "quality", Value: 0}}, investmentexplain.MeasureIssueSize, &readers.SizingCoverage{SizedUnits: 0, Units: 1})
	if len(got) != 0 {
		t.Fatalf("rows = %+v, want none", got)
	}
}
