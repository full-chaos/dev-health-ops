package analytics

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9264: a work-category filter named the retired rule classifier's investment_area on the daily path. The
// categories are the canonical themes now, which only the investment path reads, so the daily path refuses the
// filter (it never matches the wrong key silently), and the catalog keeps refusing a bad dimension first.
func TestWorkCategoryFilterIsRefusedOnTheDailyPath(t *testing.T) {
	filters := &model.FilterInput{Why: &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}}}
	if _, err := translateFilters("org-1", filters, true, defaultFilterColumns()); err != nil {
		t.Fatalf("investment path: %v", err)
	}
	_, err := translateFilters("org-1", filters, false, defaultFilterColumns())
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Field != "why" {
		t.Fatalf("daily path: err = %v, want a validation error on why", err)
	}
	if _, err := CompileCatalogValues(DimensionAuthor, 100, "org-1", queryTimeoutSecs, filters); !errors.As(err, &validation) || validation.Field != "dimension" {
		t.Fatalf("catalog, author dimension with a work-category filter: err = %v, want the dimension error first", err)
	}
}
