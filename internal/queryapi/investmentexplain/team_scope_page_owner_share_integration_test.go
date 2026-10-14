//go:build integration

package investmentexplain

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// TestInvestmentPageWeighsATeamScopeByTheOwnersShareLikeTheExplanation pins CHAOS-9629 (D5988, D5990) against a real
// ClickHouse: on scope TEAM with ids, EVERY team-scoped read of the Investment page (breakdown by THEME and
// SUBCATEGORY, timeseries, sankey, flow matrix) weighs a repository by the share of it the teams in view hold, as
// the explanation (and the REST readers) already did. Before, only the TEAM dimension applied the share, so a
// co-owner holding 0.4 of a repository read the whole of it on every other dimension.
//
//   - repo-lead: team-main 0.6, team-co 0.4. One unit: maintenance.debt 0.5, quality.bugfix 0.5.
//   - repo-tie: team-main 0.5, team-tie 0.5. One unit: risk.security 1.
//   - repo-solo: team-main 1.0. One unit: maintenance.debt 1.
//
// The units carry 100, 50 and 10 lines: the measure is COUNT (one unit counts 1), so the page and the explanation
// read the owners' shares of the units and nothing else.
func TestInvestmentPageWeighsATeamScopeByTheOwnersShareLikeTheExplanation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-page-owner-share-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)

	type owner struct {
		team    string
		primary uint8
		share   float64
	}
	for i, repo := range []struct {
		name   string
		lines  float64
		subs   map[string]float64
		themes map[string]float64
		owners []owner
	}{
		{"acme/repo-lead", 100, map[string]float64{"maintenance.debt": 0.5, "quality.bugfix": 0.5}, map[string]float64{"maintenance": 0.5, "quality": 0.5}, []owner{{"team-main", 1, 0.6}, {"team-co", 2, 0.4}}},
		{"acme/repo-tie", 50, map[string]float64{"risk.security": 1}, map[string]float64{"risk": 1}, []owner{{"team-main", 1, 0.5}, {"team-tie", 1, 0.5}}},
		{"acme/repo-solo", 10, map[string]float64{"maintenance.debt": 1}, map[string]float64{"maintenance": 1}, []owner{{"team-main", 1, 1.0}}},
	} {
		id := uuid.New()
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", repo.name, err)
		}
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', ?, ?, ?, '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			"wu-"+string(rune('a'+i)), from, to, id, repo.lines, repo.themes, repo.subs, computed, org); err != nil {
			t.Fatalf("seed work unit %s: %v", repo.name, err)
		}
		for _, o := range repo.owners {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, o.primary, ownedFrom, ownedFrom, o.share); err != nil {
				t.Fatalf("seed ownership %s/%s: %v", repo.name, o.team, err)
			}
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	window := &model.DateRangeInput{StartDate: graphqldate.New(from.AddDate(0, 0, -1)), EndDate: graphqldate.New(to.AddDate(0, 0, 1))}
	useInvestment := true

	for _, tc := range []struct {
		name         string
		teams        []string
		subcategory  map[string]float64 // expected COUNT per subcategory
		wantTotalSum float64
	}{
		{"co-owner at 0.4 of a repository", []string{"team-co"}, map[string]float64{"maintenance.debt": 0.2, "quality.bugfix": 0.2}, 0.4},
		{"main owner: its 0.6, half of the tie, all of the solo repository", []string{"team-main"}, map[string]float64{"maintenance.debt": 0.3 + 1, "quality.bugfix": 0.3, "risk.security": 0.5}, 2.1},
		{"a tied owner reads its equal half", []string{"team-tie"}, map[string]float64{"risk.security": 0.5}, 0.5},
		{"the owners of one repository read it once", []string{"team-main", "team-co"}, map[string]float64{"maintenance.debt": 0.5 + 1, "quality.bugfix": 0.5, "risk.security": 0.5}, 2.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filters := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: tc.teams}}

			// The explanation's read: BreakdownFilters at the same team scope, measure COUNT.
			condition, bindings := teamscope.RepoCondition(org, "repo_id", tc.teams, asOf)
			explainRows, err := reader.FetchInvestmentBreakdown(ctx, BreakdownFilters{
				OrgID: org, StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1),
				TeamScopeCondition: condition, TeamScopeBindings: bindings, TeamIDs: tc.teams, TeamAsOf: asOf,
				Measure: MeasureCount,
			})
			if err != nil {
				t.Fatalf("explain breakdown: %v", err)
			}
			explainSub, explainTheme := map[string]float64{}, map[string]float64{}
			for _, row := range explainRows {
				explainSub[row.Subcategory] += row.Value
				explainTheme[row.Theme] += row.Value
			}

			// The page: breakdowns, timeseries, sankey and flow matrix, all at the same scope.
			result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
				Filters:       filters,
				UseInvestment: &useInvestment,
				Breakdowns: []model.BreakdownRequestInput{
					{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: window, TopN: 20},
					{Dimension: model.DimensionInputSubcategory, Measure: model.MeasureInputCount, DateRange: window, TopN: 20},
				},
				Timeseries: []model.TimeseriesRequestInput{
					{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, Interval: model.BucketIntervalInputDay, DateRange: window},
				},
				Sankey:     &model.SankeyRequestInput{Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory}, Measure: model.MeasureInputCount, DateRange: window, MaxNodes: 30, MaxEdges: 60},
				FlowMatrix: &model.FlowMatrixRequestInput{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: window, MaxNodes: 30, MaxEdges: 60},
			})
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			near := func(what string, got, want float64) {
				t.Helper()
				// The shares are Float32 in the shared read (the page's own narrowing).
				if math.Abs(got-want) > 1e-3 {
					t.Errorf("%s = %v, want %v", what, got, want)
				}
			}
			pageTheme, pageSub := map[string]float64{}, map[string]float64{}
			if len(result.Breakdowns) != 2 {
				t.Fatalf("page breakdowns = %d, want THEME then SUBCATEGORY", len(result.Breakdowns))
			}
			for i, target := range []map[string]float64{pageTheme, pageSub} {
				for _, item := range result.Breakdowns[i].Items {
					if item.Value != nil {
						target[item.Key] = *item.Value
					}
				}
			}
			for key, want := range tc.subcategory {
				near("explain "+key, explainSub[key], want)
				near("page breakdown "+key, pageSub[key], want)
			}
			for key, want := range explainTheme {
				near("page theme "+key, pageTheme[key], want)
			}
			if len(pageSub) != len(tc.subcategory) || len(pageTheme) != len(explainTheme) {
				t.Errorf("page keys: subcategories %v themes %v; want subcategories %v themes %v", pageSub, pageTheme, tc.subcategory, explainTheme)
			}

			sum := func(values map[string]float64) float64 {
				total := 0.0
				for _, v := range values {
					total += v
				}
				return total
			}
			near("page theme total", sum(pageTheme), tc.wantTotalSum)
			near("page subcategory total", sum(pageSub), tc.wantTotalSum)

			series := 0.0
			for _, ts := range result.Timeseries {
				for _, bucket := range ts.Buckets {
					if bucket.Value != nil {
						series += *bucket.Value
					}
				}
			}
			near("page timeseries total", series, tc.wantTotalSum)

			if result.Sankey == nil {
				t.Fatal("page sankey is nil")
			}
			// A two-stage path counts the unit across its subcategories once: the theme->subcategory edges sum to the total.
			edges := 0.0
			for _, edge := range result.Sankey.Edges {
				if edge.Value != nil {
					edges += *edge.Value
				}
			}
			near("page sankey edges total", edges, tc.wantTotalSum)

			if result.FlowMatrix == nil {
				t.Fatal("page flow matrix is nil")
			}
			nodes := 0.0
			for _, node := range result.FlowMatrix.Nodes {
				if node.Value != nil {
					nodes += *node.Value
				}
			}
			near("page flow matrix nodes total", nodes, tc.wantTotalSum)
		})
	}
}
