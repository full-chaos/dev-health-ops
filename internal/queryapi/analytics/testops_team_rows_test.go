package analytics

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A tied repository (D5990) has one TestOps row per main owner. A request that
// reads a team keeps every owner of the newest run; any other request reads one
// row per repository and day, so a tied repository is counted once.
func TestTestopsMeasureReadsOneRowPerOwnerOnlyForATeamRead(t *testing.T) {
	t.Parallel()
	flat, _, _ := nonInvestmentSourceAndDateFilter(MeasurePipelineSuccessRate, false)
	if !strings.Contains(flat, "ORDER BY computed_at DESC, team_id ASC") || !strings.Contains(flat, "LIMIT 1 BY org_id, repo_id, day\n") {
		t.Errorf("non-team read = %q, want one row per (org, repo, day), smallest team_id first", flat)
	}
	teamed, _, _ := nonInvestmentSourceAndDateFilter(MeasurePipelineSuccessRate, true)
	for _, want := range []string{
		"max(computed_at) OVER (PARTITION BY org_id, repo_id, day)",
		"WHERE computed_at = latest_computed_at",
		"LIMIT 1 BY org_id, repo_id, day, team_id",
	} {
		if !strings.Contains(teamed, want) {
			t.Errorf("team read = %q, want it to contain %q", teamed, want)
		}
	}
	other, _, _ := nonInvestmentSourceAndDateFilter(MeasurePRReworkRatio, true)
	if strings.Contains(other, "team_id") {
		t.Errorf("a table without a team key must read as before: %q", other)
	}
}

func TestNeedsTeamRowsForATeamDimensionOrATeamScope(t *testing.T) {
	t.Parallel()
	teamScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}}
	repoScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"repo-a"}}}
	cases := []struct {
		name       string
		dimensions []Dimension
		filters    *model.FilterInput
		want       bool
	}{
		{"team dimension", []Dimension{DimensionTeam}, nil, true},
		{"team scope", []Dimension{DimensionRepo}, teamScope, true},
		{"repo only", []Dimension{DimensionRepo}, repoScope, false},
		{"nothing", nil, nil, false},
	}
	for _, tc := range cases {
		if got := needsTeamRows(tc.dimensions, tc.filters); got != tc.want {
			t.Errorf("%s: needsTeamRows = %v, want %v", tc.name, got, tc.want)
		}
	}
}
