//go:build integration

package analytics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A TestOps measure reads its team from the ownership join, never from the stored team_id: a team scope on the TestOps
// path is the repositories the team owns. "unassigned" is a key of the completion table's stored team only, so on this
// path it names no team, and the sankey coverage of a TestOps measure answers as it did before "unassigned" was a key.
func TestTestopsSankeyCoverageIgnoresTheUnassignedScopeKey(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-14281-testops-scope"
	const owned, unowned = "14281000-0000-4000-8000-0000000000a1", "14281000-0000-4000-8000-0000000000b1"
	e.repo(org, owned, "acme/owned")
	e.repo(org, unowned, "acme/unowned")
	e.owner(org, "gh:payments", owned, "acme/owned", 1, openFrom(), nil)
	e.daily(org, repoDay{repo: owned, day: day1, runs: 10, ok: 8})
	e.daily(org, repoDay{repo: unowned, day: day1, runs: 10, ok: 5})
	// The coverage statement reads the completion table for every measure: one row of the real team, one of the empty team.
	seedWorkItemCompletionGeneration(t, e.ctx, e.admin, org, "2026-01-02", owned, "gh:payments", "2026-01-06 00:00:00", 4)
	seedWorkItemCompletionGeneration(t, e.ctx, e.admin, org, "2026-01-02", unowned, "", "2026-01-06 00:00:00", 4)

	coverage := func(ids ...string) string {
		health := false
		result, err := Resolve(e.ctx, e.client, org, model.AnalyticsRequestInput{
			UseInvestment: &health,
			Filters:       teamScope(ids...),
			Sankey: &model.SankeyRequestInput{
				Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
				Measure: model.MeasureInputPipelineSuccessRate,
				DateRange: &model.DateRangeInput{
					StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07"),
				},
				MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
			},
		})
		if err != nil {
			t.Fatalf("Resolve %v: %v", ids, err)
		}
		if result.Sankey == nil || result.Sankey.Coverage == nil {
			return "null"
		}
		return fmt.Sprintf("%.4f", result.Sankey.Coverage.TeamCoverage)
	}

	base := coverage("gh:payments")
	if base != "1.0000" {
		t.Fatalf("real-team coverage = %s, want 1.0000 (the fixture's one real-team row)", base)
	}
	for _, tc := range []struct {
		name string
		ids  []string
		want string
	}{
		{"real team only", []string{"gh:payments"}, base},
		{"real team and unassigned", []string{"gh:payments", "unassigned"}, base},
	} {
		if got := coverage(tc.ids...); got != tc.want {
			t.Errorf("%s: team coverage = %s, want %s (the real-team scope)", tc.name, got, tc.want)
		}
	}
	t.Logf("real-team coverage %s; scope [unassigned] coverage %s", base, coverage("unassigned"))
	if got := coverage("unassigned"); got != "null" {
		t.Errorf("scope [unassigned]: team coverage = %s, want null (as on main: no completion row has the team \"unassigned\")", got)
	}
}

// The chart of a TestOps measure with the scope ["unassigned"] selects what main selects: the ownership join knows no
// team of that name, so the chart is empty, and ["gh:payments","unassigned"] is the chart of ["gh:payments"].
func TestTestopsChartIgnoresTheUnassignedScopeKey(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-14281-testops-chart"
	const owned, unowned = "14281000-0000-4000-8000-0000000000a1", "14281000-0000-4000-8000-0000000000b1"
	e.repo(org, owned, "acme/owned")
	e.repo(org, unowned, "acme/unowned")
	e.owner(org, "gh:payments", owned, "acme/owned", 1, openFrom(), nil)
	e.daily(org, repoDay{repo: owned, day: day1, runs: 10, ok: 8})
	e.daily(org, repoDay{repo: unowned, day: day1, runs: 10, ok: 5})

	chart := func(ids ...string) string {
		health := false
		result, err := Resolve(e.ctx, e.client, org, model.AnalyticsRequestInput{
			UseInvestment: &health,
			Filters:       teamScope(ids...),
			Timeseries: []model.TimeseriesRequestInput{{
				Dimension: model.DimensionInputTeam, Measure: model.MeasureInputPipelineSuccessRate,
				Interval:  model.BucketIntervalInputDay,
				DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			}},
			Sankey: &model.SankeyRequestInput{
				Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
				Measure: model.MeasureInputPipelineSuccessRate,
				DateRange: &model.DateRangeInput{
					StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07"),
				},
				MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
			},
		})
		if err != nil {
			t.Fatalf("Resolve %v: %v", ids, err)
		}
		out := ""
		for _, s := range result.Timeseries {
			out += fmt.Sprintf("ts %s/%s:%d;", s.Scope, s.DimensionValue, len(s.Buckets))
		}
		if result.Sankey != nil {
			for _, n := range result.Sankey.Nodes {
				out += "node " + n.ID + ";"
			}
			for _, ed := range result.Sankey.Edges {
				out += "edge " + ed.Source + ">" + ed.Target + ";"
			}
		}
		return out
	}
	real := chart("gh:payments")
	if real == "" {
		t.Fatal("the real-team chart is empty: the fixture serves nothing")
	}
	if got := chart("gh:payments", "unassigned"); got != real {
		t.Errorf("chart [gh:payments unassigned] = %q, want the chart of [gh:payments] = %q", got, real)
	}
	t.Logf("chart [unassigned] = %q", chart("unassigned"))
	if got, main := chart("unassigned"), chart("no-such-team"); got != main {
		t.Errorf("chart [unassigned] = %q, want the chart of a team that owns nothing = %q", got, main)
	}
}

// Every measure of the registry: the coverage of a scope with "unassigned" is the coverage of the stored form for a
// measure whose chart groups by the stored team (the empty team is selected), and the coverage of the real team alone
// for a TestOps measure (the scope key names no owner).
func TestSankeyCoverageUnassignedScopeFollowsTheChartsKeyForEveryMeasure(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-14281-all-measures"
	const owned, unowned = "14281000-0000-4000-8000-0000000000a1", "14281000-0000-4000-8000-0000000000b1"
	e.repo(org, owned, "acme/owned")
	e.repo(org, unowned, "acme/unowned")
	e.owner(org, "gh:payments", owned, "acme/owned", 1, openFrom(), nil)
	e.daily(org, repoDay{repo: owned, day: day1, runs: 10, ok: 8})
	e.daily(org, repoDay{repo: unowned, day: day1, runs: 10, ok: 5})
	seedWorkItemCompletionGeneration(t, e.ctx, e.admin, org, "2026-01-02", owned, "gh:payments", "2026-01-06 00:00:00", 4)
	seedWorkItemCompletionGeneration(t, e.ctx, e.admin, org, "2026-01-02", unowned, "", "2026-01-06 00:00:00", 4)

	exercised := 0
	for _, entry := range catalogMeasures {
		measure := entry.measure
		coverage := func(ids ...string) (string, bool) {
			health := false
			result, err := Resolve(e.ctx, e.client, org, model.AnalyticsRequestInput{
				UseInvestment: &health,
				Filters:       teamScope(ids...),
				Sankey: &model.SankeyRequestInput{
					Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
					Measure: model.MeasureInput(strings.ToUpper(string(measure))),
					DateRange: &model.DateRangeInput{
						StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07"),
					},
					MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
				},
			})
			if err != nil || result.Sankey == nil {
				return "", false
			}
			if result.Sankey.Coverage == nil {
				return "null", true
			}
			return fmt.Sprintf("%.4f", result.Sankey.Coverage.TeamCoverage), true
		}
		realOnly, ok := coverage("gh:payments")
		if !ok {
			t.Logf("measure %s: no sankey, skipped", measure)
			continue
		}
		exercised++
		want := realOnly
		if !isTestopsMeasure(measure) {
			want, _ = coverage("gh:payments", "")
		}
		if got, _ := coverage("gh:payments", "unassigned"); got != want {
			t.Errorf("measure %s: coverage [gh:payments unassigned] = %s, want %s", measure, got, want)
		}
	}
	if exercised < 12 {
		t.Errorf("only %d measures were exercised", exercised)
	}
	t.Logf("%d measures exercised", exercised)
}
