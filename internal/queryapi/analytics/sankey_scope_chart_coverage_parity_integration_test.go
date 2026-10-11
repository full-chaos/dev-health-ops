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

// CHAOS-9221 round 138: the chart and the coverage statement of a sankey read the SAME unit set under every scope kind
// (the one unitRepoExpr), so a third copy of the scope column cannot drift.
func TestSankeyChartAndCoverageReadTheSameUnitsUnderEveryScopeKind(t *testing.T) {
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
	ev := func(pr string) string {
		raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{pr}, "commits": []string{}, "edges": []string{}})
		return string(raw)
	}
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	unit := func(id string, own any, pr string) {
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, own, themes, subs, ev(pr), at.Add(time.Hour), org)
	}
	var nilRepo *string
	unit("own-a", repoA, repoA+"#pr1")   // own repo_id = A, NO allocation row
	unit("alloc-a", nilRepo, "none#pr2") // own repo_id NULL, allocation row -> A
	exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES ('alloc-a', ?, 'churn_loc', 10, 1.0, 'pr_refs', 'run-1', ?, ?)`, repoA, at.Add(time.Hour), org)
	unit("own-b", repoB, repoB+"#pr3") // own repo_id = B, NO allocation row

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
	reqInput := model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputRepo},
		Measure:   model.MeasureInputCount,
		DateRange: &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))},
		MaxNodes:  16,
		MaxEdges:  100,
	}
	scope := func(level model.ScopeLevelInput, ids ...string) *model.FilterInput {
		return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: level, Ids: ids}}
	}
	cases := []struct {
		name      string
		filters   *model.FilterInput
		wantUnits float64 // the units the chart must carry (each unit counts 1 under COUNT)
	}{
		{"team A", scope(model.ScopeLevelInputTeam, "team-a"), 2},
		{"team B", scope(model.ScopeLevelInputTeam, "team-b"), 1},
		{"repository A", scope(model.ScopeLevelInputRepo, repoA), 2},
		{"repository B", scope(model.ScopeLevelInputRepo, repoB), 1},
		{"a team that owns nothing", scope(model.ScopeLevelInputTeam, "team-none"), 0},
	}
	for _, tc := range cases {
		out, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{Filters: tc.filters, Sankey: &reqInput})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var chart float64
		for _, e := range out.Sankey.Edges {
			if e.Source == "THEME:feature_delivery" {
				if e.Value != nil {
					chart += *e.Value
				}
			}
		}
		if chart != tc.wantUnits {
			t.Errorf("%s: chart carries %v units, want %v", tc.name, chart, tc.wantUnits)
		}
		coverage := resolveSankeyCoverage(ctx, client, org, req, 60, true, tc.filters)
		if (coverage != nil) != (chart > 0) {
			t.Errorf("%s: chart carries %v units but coverage present = %v: the two read different unit sets", tc.name, chart, coverage != nil)
		}
	}
}
