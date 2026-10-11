//go:build integration

package analytics

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9221 round 135: team-scoped sankey coverage keeps a unit that has no allocation row (its own repo_id is the fallback).
func TestTeamScopedSankeyCoverageKeepsAUnitWithNoAllocationRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-9221"
	const repoA, repoB = "92210001-9221-4221-8221-922192219221", "92210002-9221-4221-8221-922192219221"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
	}
	owns := func(team, repo, name string) {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, team, repo, name, at.AddDate(-1, 0, 0), at)
	}
	owns("team-a", repoA, "acme/a")
	owns("team-b", repoB, "acme/b")
	raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{repoA + "#pr1"}, "commits": []string{}, "edges": []string{}})
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	// The unit's own repo_id = repo A (owned by team A); there is NO work_unit_repo_effort row for it.
	exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('u1', ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, at.Add(-24*time.Hour), at, repoA, themes, subs, string(raw), at.Add(time.Hour), org)
	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputRepo},
		Measure:   model.MeasureInputCount,
		DateRange: &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))},
		MaxNodes:  16,
		MaxEdges:  100,
	})
	if err != nil {
		t.Fatal(err)
	}
	team := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}}
	got := resolveSankeyCoverage(ctx, client, org, req, 60, true, team)
	if got == nil {
		t.Fatal("team-scoped sankey coverage is nil: a unit owned by the team with no allocation row fell out of coverage while it is in the nodes and edges")
	}
	// Round 138: the REPOSITORY scope of coverage reads the same fallback column as the chart.
	repoScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{repoA}}}
	if resolveSankeyCoverage(ctx, client, org, req, 60, true, repoScope) == nil {
		t.Fatal("repo-scoped sankey coverage is nil: a unit with its own repo_id and no allocation row fell out of coverage")
	}
	// Round 138: team ids that are all empty name no team: no scope, never invalid SQL (breakdown and evidence quality).
	emptyTeam := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{""}}}
	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	out, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		Filters:    emptyTeam,
		Breakdowns: []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10}},
	})
	if err != nil {
		t.Fatalf("empty team id: %v", err)
	}
	if out.EvidenceQualityStats == nil || out.EvidenceQualityStats.Total != 1 {
		t.Fatalf("empty team id must mean no scope: evidence quality = %+v, want total 1", out.EvidenceQualityStats)
	}
}
