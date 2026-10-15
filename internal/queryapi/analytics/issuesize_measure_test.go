package analytics

import (
	"errors"
	"strings"
	"testing"
)

// TestIssueSizeMeasureOnlyMovesTheSQLOfItsOwnReads pins the boundary of CHAOS-9619: a COUNT read runs on its
// context unchanged (its SQL does not move), an ISSUE_SIZE read runs on the sized source, and ISSUE_SIZE is refused
// outside the investment path, where no unit has a size.
func TestIssueSizeMeasureOnlyMovesTheSQLOfItsOwnReads(t *testing.T) {
	for _, repoScoped := range []bool{false, true} {
		ictx := investmentContextFor("org-1", []Dimension{DimensionTheme}, false, repoScoped, false, nil)
		for _, measure := range []Measure{MeasureCount, MeasureThroughput, MeasureCycleTimeHours} {
			got, err := ictx.sizedFor(measure)
			if err != nil || got.Source != ictx.Source {
				t.Fatalf("%s (repo scoped %v): the source moved or failed (%v)", measure, repoScoped, err)
			}
		}
		sized, err := ictx.sizedFor(MeasureIssueSize)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(sized.Source, "FROM work_unit_issue_size") || !strings.HasSuffix(sized.Source, " AS work_unit_investments") {
			t.Fatalf("the ISSUE_SIZE source does not read the unit sizes under the read's alias:\n%s", sized.Source)
		}
		expr, err := sized.measure(MeasureIssueSize)
		if err != nil || !strings.Contains(expr, UnitSizeWeightColumn) {
			t.Fatalf("the ISSUE_SIZE expression %q (%v) does not weigh by %s", expr, err, UnitSizeWeightColumn)
		}
	}
	if _, err := IssueSizedSource("(SELECT 1) AS something_else", "work_unit_investments"); err == nil {
		t.Fatal("a source without the read's alias was wrapped")
	}
	_, err := dbExpression(MeasureIssueSize, false, false)
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("ISSUE_SIZE on the non-investment path = %v, want a validation error", err)
	}
}
