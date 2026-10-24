package analytics

import (
	"strings"
	"testing"
)

const categorisedOnlyPredicate = CategorizedOnlySQL

func TestInvestmentContext_MixDimensionsExcludeGatedUnits(t *testing.T) {
	for _, dim := range []Dimension{DimensionTheme, DimensionSubcategory} {
		for _, repoScoped := range []bool{false, true} {
			ictx := investmentContextFor("org-1", []Dimension{dim}, false, repoScoped, false, nil)
			if !strings.Contains(ictx.DateFilter, categorisedOnlyPredicate) {
				t.Errorf("dimension %s repoScoped=%v: the mix must exclude gated units, DateFilter = %s", dim, repoScoped, ictx.DateFilter)
			}
		}
	}
}

func TestInvestmentContext_NonMixDimensionsKeepGatedUnits(t *testing.T) {
	for _, dim := range []Dimension{DimensionRepo, DimensionTeam, DimensionWorkType} {
		ictx := investmentContextFor("org-1", []Dimension{dim}, false, false, false, nil)
		if strings.Contains(ictx.DateFilter, "categorization_status") {
			t.Errorf("dimension %s counts work units and must keep gated units, DateFilter = %s", dim, ictx.DateFilter)
		}
	}
}

func TestCompileBreakdown_ThemeSQLCarriesCategorisedOnlyPredicate(t *testing.T) {
	req := BreakdownRequest{Dimension: DimensionTheme, Measure: MeasureCount, StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-31"), TopN: 10}
	q, err := CompileBreakdown(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q.sql, categorisedOnlyPredicate) {
		t.Errorf("theme breakdown SQL lacks the categorised-only predicate: %s", q.sql)
	}
}
