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
)

// TestInvestmentPageTeamScopeWeighsAMeanByShareAndAdditiveMeasuresBySumCHAOS9629 pins D5998 on the Investment page's
// team scope against a real ClickHouse:
//
//   - an INTENSIVE measure (cycle time) is the share-weighted MEAN, sum(value x share) / sum(share), never value x
//     share and never a sum: a repository held at 0.4 with a 10 h unit and one held at 1.0 with a 20 h unit read
//     (0.4 x 10 + 1.0 x 20) / 1.4 = 17.142857 h, not 24 h and not 30 h. The TEAM dimension (the ut ownership join)
//     already served this mean; the scope weight now serves it on THEME and every other dimension.
//
//   - coverage: numerator and denominator carry the same weight; with every share 1.0 a team scope reads the same
//     coverage as the repository scope of the same repository, byte for byte.
//
//   - repo-x: team-main 0.6, team-co 0.4; one unit of 10 h, maintenance.
//
//   - repo-y: team-co 1.0; one unit of 20 h, maintenance.
//
//   - repo-solo: team-solo 1.0; one unit of 5 h, risk.
func TestInvestmentPageTeamScopeWeighsAMeanByShareAndAdditiveMeasuresBySum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-page-cycle-time-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	type owner struct {
		team  string
		share float64
	}
	repoIDs := map[string]uuid.UUID{}
	for i, repo := range []struct {
		name   string
		hours  int
		theme  string
		sub    string
		owners []owner
	}{
		{"acme/repo-x", 10, "maintenance", "maintenance.debt", []owner{{"team-main", 0.6}, {"team-co", 0.4}}},
		{"acme/repo-y", 20, "maintenance", "maintenance.debt", []owner{{"team-co", 1.0}}},
		{"acme/repo-solo", 5, "risk", "risk.security", []owner{{"team-solo", 1.0}}},
	} {
		id := uuid.New()
		repoIDs[repo.name] = id
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, repo.name, org, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", repo.name, err)
		}
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', 100, map(?, 1.0), map(?, 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			"wu-"+string(rune('a'+i)), day, day.Add(time.Duration(repo.hours)*time.Hour), id, repo.theme, repo.sub, computed, org); err != nil {
			t.Fatalf("seed work unit %s: %v", repo.name, err)
		}
		for _, o := range repo.owners {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, id, repo.name, ownedFrom, ownedFrom, o.share); err != nil {
				t.Fatalf("seed ownership %s/%s: %v", repo.name, o.team, err)
			}
		}
	}

	window := &model.DateRangeInput{StartDate: graphqldate.New(day.AddDate(0, 0, -1)), EndDate: graphqldate.New(day.AddDate(0, 0, 3))}
	useInvestment := true
	cycle := func(dimension model.DimensionInput, teams ...string) map[string]float64 {
		t.Helper()
		req := model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Breakdowns:    []model.BreakdownRequestInput{{Dimension: dimension, Measure: model.MeasureInputCycleTimeHours, DateRange: window, TopN: 20}},
		}
		if len(teams) > 0 {
			req.Filters = &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: teams}}
		}
		out, err := analytics.Resolve(ctx, client, org, req)
		if err != nil {
			t.Fatalf("page %s %v: %v", dimension, teams, err)
		}
		values := map[string]float64{}
		for _, item := range out.Breakdowns[0].Items {
			if item.Value != nil {
				values[item.Key] = *item.Value
			}
		}
		return values
	}
	near := func(what string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-3 {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}

	// A share-weighted mean on THEME under a team scope (was: the unweighted mean of the whole repositories, (10+20)/2 = 15).
	near("team-co scope, THEME maintenance cycle time", cycle(model.DimensionInputTheme, "team-co")["maintenance"], (0.4*10+1.0*20)/1.4)
	near("team-main scope, THEME maintenance cycle time", cycle(model.DimensionInputTheme, "team-main")["maintenance"], 10)
	near("team-co and team-main together own repo-x once", cycle(model.DimensionInputTheme, "team-co", "team-main")["maintenance"], (1.0*10+1.0*20)/2.0)
	// The TEAM dimension (the ut join) is the same mean per team, with or without the scope.
	byTeam := cycle(model.DimensionInputTeam)
	near("TEAM dimension, team-co", byTeam["team-co"], (0.4*10+1.0*20)/1.4)
	near("TEAM dimension, team-main", byTeam["team-main"], 10)
	near("TEAM dimension under scope team-co", cycle(model.DimensionInputTeam, "team-co")["team-co"], (0.4*10+1.0*20)/1.4)

	// Coverage: with every share 1.0 the team scope reads the repository scope's coverage exactly.
	sankey := func(filters *model.FilterInput) *model.SankeyCoverage {
		t.Helper()
		out, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Filters:       filters,
			Sankey:        &model.SankeyRequestInput{Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputRepo}, Measure: model.MeasureInputCount, DateRange: window, MaxNodes: 30, MaxEdges: 60},
		})
		if err != nil || out.Sankey == nil || out.Sankey.Coverage == nil {
			t.Fatalf("sankey coverage: %v %+v", err, out)
		}
		return out.Sankey.Coverage
	}
	teamScope := sankey(&model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-solo"}}})
	repoScope := sankey(&model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{repoIDs["acme/repo-solo"].String()}}})
	if teamScope.TeamCoverage != repoScope.TeamCoverage || teamScope.RepoCoverage != repoScope.RepoCoverage {
		t.Errorf("share 1.0: team scope coverage %+v, repository scope coverage %+v, want equal", teamScope, repoScope)
	}
	// A co-owned scope still reads a coverage (numerator and denominator weighed alike): every unit in scope is owned.
	co := sankey(&model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-co"}}})
	near("team-co coverage of teams", co.TeamCoverage, 1)
	near("team-co coverage of repositories", co.RepoCoverage, 1)
}
