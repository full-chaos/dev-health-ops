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

// CHAOS-9221, round 133 findings, EXECUTED on a real ClickHouse:
//  1. a scoped Sankey coverage read runs (it failed with "Correlated subqueries are not supported");
//  2. a repository filter AND a team filter hold on the SAME repository row: a unit touching the team's
//     repository A and a requested repository B is NOT in "team A + repository B";
//  3. only the LATEST allocation generation of a unit counts: a unit whose latest rows name only repository B is
//     not in repository A's scope through a stale row naming A.
func TestUnitRowsScopeSemanticsOnARealEngine(t *testing.T) {
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

	const org = "org-9221-rows"
	const repoA, repoB = "92210021-9221-4221-8221-922192219221", "92210022-9221-4221-8221-922192219221"
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
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-a', ?, 'acme/a', 'exact', 'native', 1, 100, 0, ?, NULL, ?)`, org, repoA, at.AddDate(-1, 0, 0), at)
	raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	var nilRepo *string
	unit := func(id string) {
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, nilRepo, map[string]float64{"feature_delivery": 1}, map[string]float64{"feature_delivery.roadmap": 1}, string(raw), at.Add(time.Hour), org)
	}
	row := func(id, repo string, computed time.Time) {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES (?, ?, 'churn_loc', 5, 0.5, 'pr_refs', 'run', ?, ?)`, id, repo, computed, org)
	}
	// uAB touches A and B (current generation). uStale: a stale row names A, the LATEST generation names only B.
	unit("uAB")
	row("uAB", repoA, at.Add(time.Hour))
	row("uAB", repoB, at.Add(time.Hour))
	unit("uStale")
	row("uStale", repoA, at.Add(-48*time.Hour))
	row("uStale", repoB, at.Add(time.Hour))

	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	filter := func(level model.ScopeLevelInput, id string, repos ...string) *model.FilterInput {
		f := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: level, Ids: []string{id}}}
		if len(repos) > 0 {
			f.What = &model.WhatFilterInput{Repos: repos}
		}
		return f
	}
	qualityTotal := func(f *model.FilterInput) int {
		t.Helper()
		result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
			Filters:    f,
			Breakdowns: []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.EvidenceQualityStats == nil {
			return 0
		}
		return result.EvidenceQualityStats.Total
	}
	// (3) stale generation: repository A holds uAB only; repository B holds both (uAB and uStale).
	if got := qualityTotal(filter(model.ScopeLevelInputRepo, repoA)); got != 1 {
		t.Errorf("repository A quality total = %d, want 1 (uAB; uStale's latest generation names only B)", got)
	}
	if got := qualityTotal(filter(model.ScopeLevelInputRepo, repoB)); got != 2 {
		t.Errorf("repository B quality total = %d, want 2 (uAB, uStale)", got)
	}
	// (2) team A (owns A) AND repository B on the SAME row: no unit has a row in both A and B... uAB does touch A and B
	// but a row cannot be A and B at once: the intersection of "owned by team A" and "repository B" is EMPTY.
	if got := qualityTotal(filter(model.ScopeLevelInputTeam, "team-a", repoB)); got != 0 {
		t.Errorf("team A + repository B quality total = %d, want 0 (the two conditions must hold on one row)", got)
	}
	if got := qualityTotal(filter(model.ScopeLevelInputTeam, "team-a", repoA)); got != 1 {
		t.Errorf("team A + repository A quality total = %d, want 1 (uAB)", got)
	}
	// (1) a scoped sankey with its coverage runs and answers a coverage object.
	sankey, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		Filters: filter(model.ScopeLevelInputRepo, repoA),
		Sankey: &model.SankeyRequestInput{
			Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputRepo}, Measure: model.MeasureInputCount,
			DateRange: dates, MaxNodes: 100, MaxEdges: 500,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sankey.Sankey == nil || sankey.Sankey.Coverage == nil {
		t.Fatalf("a repository-scoped sankey answered no coverage (the coverage read failed or was swallowed): %+v", sankey.Sankey)
	}
}
