package analytics

// TestCompileSankeyCoverage_UnfilteredSQLUnchangedByUnitSelection is
// CHAOS-5498's byte-identity guard: the fix may only change what a
// WORK-CATEGORY-FILTERED coverage query compiles to. Every unfiltered
// request -- which is every request the reference org's own A/B baselines
// were measured with -- must compile to exactly the bytes it did before.
//
// This is a plain unit test on purpose: it needs no engine, so it runs in the
// default suite on every push rather than only in the integration tier, where
// a regression could sit unnoticed until someone ran containers.
//
// It is deliberately NOT a golden-file test. A golden that is regenerated
// whenever it fails asserts only that someone pressed the button; this asserts
// the one PROPERTY that matters -- the compiled SQL contains no ARRAY JOIN and
// no subcategory_kv reference when no work-category filter is present -- plus
// equality between the two filter shapes that must not differ.

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// subcategoryArrayJoin is the exact join CHAOS-5498 removed. Naming it in full
// keeps this test blind to the unrelated evidence_ref ARRAY JOIN that the team
// subquery legitimately carries.
const subcategoryArrayJoin = "ARRAY JOIN CAST(subcategory_distribution_json"

func TestCompileSankeyCoverage_UnfilteredSQLUnchangedByUnitSelection(t *testing.T) {
	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	// An issue-type-only filter is the case most likely to regress by
	// accident: it is a `why` filter, so a future edit that keys the ARRAY
	// JOIN on "any why filter" rather than on work_category specifically
	// would reintroduce the multiplication here without touching any
	// work-category test.
	issueTypeOnly := &model.FilterInput{
		Why: &model.WhyFilterInput{IssueType: []string{"bug"}},
	}

	for _, c := range []struct {
		name    string
		filters *model.FilterInput
	}{
		{"no filters at all", nil},
		{"issue-type filter only", issueTypeOnly},
	} {
		t.Run(c.name, func(t *testing.T) {
			compiled, err := compileSankeyCoverage(req, "org-under-test", 60, true, c.filters)
			if err != nil {
				t.Fatalf("compileSankeyCoverage: %v", err)
			}
			if strings.Contains(compiled.sql, subcategoryArrayJoin) {
				t.Errorf("unfiltered coverage SQL contains the subcategory ARRAY JOIN -- CHAOS-5498 removed it, and reintroducing it re-weights every effort-weighted column by each unit's matching-subcategory count:\n%s", compiled.sql)
			}
			if strings.Contains(compiled.sql, "subcategory_kv") {
				t.Errorf("unfiltered coverage SQL references subcategory_kv, which only resolves under an ARRAY JOIN:\n%s", compiled.sql)
			}
		})
	}

	// The two shapes above must compile IDENTICALLY. An issue-type filter is
	// never translated into a predicate by this query (translateFilters has no
	// issue-type branch), so any difference means something started keying off
	// the presence of a `why` filter rather than off work_category.
	noFilter, err := compileSankeyCoverage(req, "org-under-test", 60, true, nil)
	if err != nil {
		t.Fatalf("compileSankeyCoverage(nil): %v", err)
	}
	issueType, err := compileSankeyCoverage(req, "org-under-test", 60, true, issueTypeOnly)
	if err != nil {
		t.Fatalf("compileSankeyCoverage(issueType): %v", err)
	}
	if noFilter.sql != issueType.sql {
		t.Errorf("an issue-type-only filter changed the compiled coverage SQL; it must not.\nno filter:\n%s\n\nissue-type only:\n%s", noFilter.sql, issueType.sql)
	}

	// And the work-category form must differ ONLY by the added predicate --
	// never by a join. This is the positive control for the two negatives
	// above: if the compiler stopped emitting any work-category predicate at
	// all, the ARRAY JOIN assertions would still pass vacuously.
	withCategory, err := compileSankeyCoverage(req, "org-under-test", 60, true, &model.FilterInput{
		Why: &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}},
	})
	if err != nil {
		t.Fatalf("compileSankeyCoverage(workCategory): %v", err)
	}
	if !strings.Contains(withCategory.sql, "arrayExists(k -> splitByChar('.', k)[1] IN {work_categories:Array(String)}, mapKeys(subcategory_distribution_json))") {
		t.Errorf("work-category filtered SQL is missing the unit-selecting predicate; without it the two negative assertions above pass vacuously:\n%s", withCategory.sql)
	}
	if strings.Contains(withCategory.sql, subcategoryArrayJoin) {
		t.Errorf("work-category filtered SQL still contains the subcategory ARRAY JOIN:\n%s", withCategory.sql)
	}
}
