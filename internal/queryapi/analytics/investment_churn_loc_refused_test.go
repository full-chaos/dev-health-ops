package analytics

import (
	"errors"
	"strings"
	"testing"
)

// CHAOS-9695 (D5999, D6014): lines of code are a health measure of delivery, never an Investment measure. The
// measure stays for the non-investment path (team and repository dimensions over work_item_completion_daily) and is
// refused, as a validation error naming the measure, on every investment path.
func TestDbExpression_ChurnLOCIsRefusedOnTheInvestmentPathOnly(t *testing.T) {
	for _, useRepoAllocation := range []bool{false, true} {
		_, err := dbExpression(MeasureChurnLOC, true, useRepoAllocation)
		var validation *ValidationError
		if !errors.As(err, &validation) || validation.Field != "measure" {
			t.Fatalf("dbExpression(churn_loc, investment, repoAllocation=%v) error = %v, want a *ValidationError on measure", useRepoAllocation, err)
		}
		if !strings.Contains(validation.Message, "not an Investment measure") {
			t.Errorf("error %q does not say churn_loc is not an Investment measure", validation.Message)
		}
	}
	if _, err := ownershipDbExpression(MeasureChurnLOC, "share"); err == nil {
		t.Error("ownershipDbExpression(churn_loc) built an expression: the team ownership read must refuse it")
	}
	got, err := dbExpression(MeasureChurnLOC, false, false)
	if err != nil || got != "SUM(churn_loc)" {
		t.Fatalf("dbExpression(churn_loc, non-investment) = %q, %v, want SUM(churn_loc)", got, err)
	}
	for _, measure := range []Measure{MeasureCount, MeasureThroughput} {
		if _, err := dbExpression(measure, true, false); err != nil {
			t.Errorf("dbExpression(%s, investment): %v", measure, err)
		}
	}
}

func TestCompileSankey_ChurnLOCIsRefusedOnTheInvestmentPath(t *testing.T) {
	req := SankeyRequest{
		Path:      []Dimension{DimensionTeam, DimensionTheme},
		Measure:   MeasureChurnLOC,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  100,
		MaxEdges:  500,
	}
	_, _, err := CompileSankey(req, "org-1", 30, true, nil)
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("CompileSankey(theme path, churn_loc) error = %v, want a *ValidationError", err)
	}
	req.Path = []Dimension{DimensionTeam, DimensionRepo}
	if _, _, err := CompileSankey(req, "org-1", 30, false, nil); err != nil {
		t.Fatalf("CompileSankey(team and repository path, churn_loc, non-investment) = %v, want it compiled: the health measure stays", err)
	}
}

// CHAOS-9695, round 1: WORK_TYPE (and the fixed TEAM and REPO templates) never read the measure, so the investment
// refusal sits at the top of CompileFlowMatrix and covers every dimension.
func TestCompileFlowMatrix_ChurnLOCIsRefusedOnTheInvestmentPathForEveryDimension(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name          string
		dimension     Dimension
		useInvestment *bool
		refused       bool
	}{
		{"work type, unset (investment by default)", DimensionWorkType, nil, true},
		{"work type, forced", DimensionWorkType, &yes, true},
		{"theme", DimensionTheme, nil, true},
		{"theme, explicit false (canonical only)", DimensionTheme, &no, true},
		{"subcategory", DimensionSubcategory, nil, true},
		{"team, forced investment", DimensionTeam, &yes, true},
		{"repo, forced investment", DimensionRepo, &yes, true},
		{"team, health", DimensionTeam, nil, false},
		{"repo, health", DimensionRepo, &no, false},
	} {
		req := FlowMatrixRequest{
			Dimension: tc.dimension, Measure: MeasureChurnLOC, UseInvestment: tc.useInvestment,
			StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-31"), MaxNodes: 100, MaxEdges: 500,
		}
		_, _, err := CompileFlowMatrix(req, "org-1", 30, nil)
		var validation *ValidationError
		switch {
		case tc.refused && !errors.As(err, &validation):
			t.Errorf("%s: CompileFlowMatrix(churn_loc) error = %v, want a *ValidationError", tc.name, err)
		case !tc.refused && err != nil:
			t.Errorf("%s: CompileFlowMatrix(churn_loc) = %v, want the health read compiled", tc.name, err)
		}
	}
}
