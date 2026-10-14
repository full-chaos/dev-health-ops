package home

import (
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
	got := themeAllocationFromEffort(rows)
	if len(got) != 1 || got[0].Theme != "feature_delivery" || got[0].AllocationPct != 100 {
		t.Fatalf("tile = %+v, want only feature_delivery at 100%%", got)
	}
}
