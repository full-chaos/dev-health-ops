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

// TestIssueSizeMeasureTakesEachOwnerAtItsShare pins CHAOS-9619 with D5988 against a real ClickHouse: in ISSUE_SIZE a
// co-owner reads its linked share of a unit's size, the TEAM grouping splits each unit's size among its owners and
// sums back to the sizes of the owned units (unassigned holds the rest), the explanation reads what the page reads,
// and the coverage cards beside the chart are weighed by the size too.
//
//   - repo-lead: team-main 0.6, team-co 0.4; unit wu-a, size 5.
//   - repo-tie: team-main 0.5, team-tie 0.5; unit wu-b, no size (weighs 1).
//   - repo-solo: team-main 1.0; unit wu-c, size 2.
//   - no repository, no owner: unit wu-d, size 3.
func TestIssueSizeMeasureTakesEachOwnerAtItsShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const org = "investmentexplain-issue-size-owner-share-org"
	synced := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	type owner struct {
		team    string
		primary uint8
		share   float64
	}
	for _, unit := range []struct {
		id, repo string
		size     float64
		owners   []owner
	}{
		{"wu-a", "acme/repo-lead", 5, []owner{{"team-main", 1, 0.6}, {"team-co", 2, 0.4}}},
		{"wu-b", "acme/repo-tie", 0, []owner{{"team-main", 1, 0.5}, {"team-tie", 1, 0.5}}},
		{"wu-c", "acme/repo-solo", 2, []owner{{"team-main", 1, 1.0}}},
		{"wu-d", "", 3, nil},
	} {
		repoID := uuid.New()
		repoValue := "NULL"
		if unit.repo != "" {
			repoValue = "toUUID('" + repoID.String() + "')"
			exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repoID, unit.repo, org, synced, synced)
		}
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, `+repoValue+`, 'jira', 'churn_loc', 900, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			unit.id, from, to, computed, org)
		if unit.size > 0 {
			exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`, org, unit.id, unit.size, computed)
		}
		for _, o := range unit.owners {
			exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
				org, o.team, repoID, unit.repo, o.primary, ownedFrom, ownedFrom, o.share)
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	start, end := from.AddDate(0, 0, -1), to.AddDate(0, 0, 1)
	window := &model.DateRangeInput{StartDate: graphqldate.New(start), EndDate: graphqldate.New(end)}
	useInvestment := true
	near := func(what string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-6 {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}

	for _, tc := range []struct {
		teams []string
		want  float64
	}{
		{[]string{"team-co"}, 0.4 * 5},
		{[]string{"team-main"}, 0.6*5 + 0.5*1 + 2},
		{[]string{"team-tie"}, 0.5 * 1},
		{[]string{"team-main", "team-co"}, 5 + 0.5 + 2},
	} {
		condition, bindings := teamscope.RepoCondition(org, "repo_id", tc.teams, asOf)
		rows, err := reader.FetchInvestmentBreakdown(ctx, BreakdownFilters{
			OrgID: org, StartTS: start, EndTS: end, TeamScopeCondition: condition, TeamScopeBindings: bindings,
			TeamIDs: tc.teams, TeamAsOf: asOf, Measure: MeasureIssueSize,
		})
		if err != nil {
			t.Fatalf("explain %v: %v", tc.teams, err)
		}
		explain := 0.0
		for _, row := range rows {
			explain += row.Value
		}
		near("explain ISSUE_SIZE at "+tc.teams[0], explain, tc.want)

		result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Filters:       &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: tc.teams}},
			Breakdowns:    []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputIssueSize, DateRange: window, TopN: 20}},
		})
		if err != nil {
			t.Fatalf("page %v: %v", tc.teams, err)
		}
		page := 0.0
		for _, item := range result.Breakdowns[0].Items {
			if item.Value != nil {
				page += *item.Value
			}
		}
		near("page ISSUE_SIZE at "+tc.teams[0], page, tc.want)
	}

	// The TEAM grouping of the whole organization: each owner its share of each unit's size; the owners and
	// unassigned add back to the sum of the unit weights (5 + 1 + 2 + 3).
	result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &useInvestment,
		Breakdowns:    []model.BreakdownRequestInput{{Dimension: model.DimensionInputTeam, Measure: model.MeasureInputIssueSize, DateRange: window, TopN: 20}},
		Sankey: &model.SankeyRequestInput{
			Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory}, Measure: model.MeasureInputIssueSize,
			DateRange: window, MaxNodes: 30, MaxEdges: 60,
		},
	})
	if err != nil {
		t.Fatalf("page by team: %v", err)
	}
	byTeam := map[string]float64{}
	total := 0.0
	for _, item := range result.Breakdowns[0].Items {
		if item.Value != nil {
			byTeam[item.Key] += *item.Value
			total += *item.Value
		}
	}
	near("team-main", byTeam["team-main"], 0.6*5+0.5*1+2)
	near("team-co", byTeam["team-co"], 0.4*5)
	near("team-tie", byTeam["team-tie"], 0.5*1)
	near("TEAM grouping total", total, 5+1+2+3)

	// Coverage weighed like the chart: wu-d (size 3) has no repository and no owner.
	coverage := result.Sankey.Coverage
	if coverage == nil {
		t.Fatal("the sankey carries no coverage")
	}
	near("teamCoverage", coverage.TeamCoverage, 8.0/11.0)
	near("repoCoverage", coverage.RepoCoverage, 8.0/11.0)
	if coverage.SizingCoverage == nil || coverage.UnsizedUnitCount == nil {
		t.Fatalf("coverage = %+v, want the sizing fields", coverage)
	}
	near("sizingCoverage", *coverage.SizingCoverage, 3.0/4.0)
	if *coverage.UnsizedUnitCount != 1 {
		t.Errorf("unsizedUnitCount = %d, want 1", *coverage.UnsizedUnitCount)
	}
}
