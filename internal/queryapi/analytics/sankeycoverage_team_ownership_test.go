package analytics

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-9264: teamCoverage is the share of units WITH a team, and a unit has a team when a team OWNS one of its
// repository rows (the shared ownership predicate of the team scope), not when the work-item attribution vote found
// one. This is a plain unit test of the compiled statement: the vote subquery is gone from coverage, the ownership
// predicate is in, and the denominator and the repository columns are the ones they were.
func TestCompileSankeyCoverage_TeamCoverageReadsOwnershipNotTheVote(t *testing.T) {
	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure:   model.MeasureInputCount,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")},
		MaxNodes:  16,
		MaxEdges:  100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}
	teamScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}}
	for _, c := range []struct {
		name    string
		filters *model.FilterInput
	}{{"no filters", nil}, {"team scope", teamScope}} {
		t.Run(c.name, func(t *testing.T) {
			compiled, err := compileSankeyCoverage(req, "org-under-test", 60, true, c.filters)
			if err != nil {
				t.Fatalf("compileSankeyCoverage: %v", err)
			}
			for _, vote := range []string{"work_item_team_attributions", "ut.team_label", "AS ut ON"} {
				if strings.Contains(compiled.sql, vote) {
					t.Errorf("coverage statement still reads the attribution vote (%q)", vote)
				}
			}
			if !strings.Contains(compiled.sql, teamscope.Marker) {
				t.Errorf("coverage statement does not carry the shared ownership predicate (%q)", teamscope.Marker)
			}
			// The numerator is the effort-weighted share, over the unit's repository rows, and the any-team
			// predicate carries no team filter of its own.
			if !strings.Contains(compiled.sql, "latest_repo_rows") {
				t.Error("the ownership test is not applied to the unit's repository rows")
			}
			seen := map[string]int{}
			for _, binding := range compiled.bindings {
				seen[binding.Name]++
				if seen[binding.Name] > 1 {
					t.Errorf("binding %q is bound twice", binding.Name)
				}
			}
			if c.filters == nil {
				if _, ok := seen[teamscope.BindingTeamIDs]; ok {
					t.Error("an org-scope coverage binds team ids: the any-team predicate must not filter by team")
				}
			}
		})
	}
}
