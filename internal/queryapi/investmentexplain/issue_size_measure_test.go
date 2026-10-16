package investmentexplain

import (
	"github.com/full-chaos/dev-health-go/readers"
	"strings"
	"testing"
)

// TestIssueSizeIsAnInvestmentMeasureAndLinesOfCodeAreNot pins the names a request may give the mix read
// (CHAOS-9619, D5999, D6014): ISSUE_SIZE is accepted in any case, lines of code are refused, and the ISSUE_SIZE value
// weighs each row by the unit's size weight while COUNT does not.
func TestIssueSizeIsAnInvestmentMeasureAndLinesOfCodeAreNot(t *testing.T) {
	for _, name := range []string{"ISSUE_SIZE", "issue_size", " Issue_Size "} {
		if measure, ok := NormalizeMeasure(name); !ok || measure != MeasureIssueSize {
			t.Errorf("NormalizeMeasure(%q) = %q, %v; want ISSUE_SIZE", name, measure, ok)
		}
	}
	for _, name := range []string{"CHURN_LOC", "LOC", "effort_value"} {
		if _, ok := NormalizeMeasure(name); ok {
			t.Errorf("NormalizeMeasure(%q) is accepted; lines of code are not an Investment measure", name)
		}
	}
	sized, err := MeasureValueSQL(MeasureIssueSize, "1.0", "1.0")
	if err != nil || !strings.Contains(sized, "unit_size_weight") {
		t.Fatalf("ISSUE_SIZE value = %q (%v), want it weighed by unit_size_weight", sized, err)
	}
	count, err := MeasureValueSQL(MeasureCount, "1.0", "1.0")
	if err != nil || strings.Contains(count, "unit_size_weight") {
		t.Fatalf("COUNT value = %q (%v), want no size weight", count, err)
	}
	if err := (BreakdownFilters{Measure: MeasureIssueSize}).requireMeasure(); err != nil {
		t.Fatalf("a read in ISSUE_SIZE is refused: %v", err)
	}
}

// TestSizingCoverageNoteIsOnlyBelowHalf pins the "mostly unsized" sentence (D6031): it is written when fewer than
// half of the units carry a size, never at exactly half, never for a read with no unit.
func TestSizingCoverageNoteIsOnlyBelowHalf(t *testing.T) {
	for _, tc := range []struct {
		sized, units int
		want         string
	}{
		{1, 4, "Mostly unsized: 1 of 4 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{0, 3, "Mostly unsized: 0 of 3 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{2, 5, "Mostly unsized: 2 of 5 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{2, 4, ""},
		{3, 5, ""},
		{0, 0, ""},
	} {
		if got := (readers.SizingCoverage{SizedUnits: tc.sized, Units: tc.units}).Note(); got != tc.want {
			t.Errorf("Note(%d of %d) = %q, want %q", tc.sized, tc.units, got, tc.want)
		}
	}
}
