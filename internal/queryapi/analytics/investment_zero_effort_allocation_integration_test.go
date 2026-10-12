//go:build integration

package analytics

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9321: a unit with stored effort 0 (or repository efforts summing to 0) is counted once on the repository
// allocation path, so the allocation total equals the unit-count total of the same window and scope.
//
//	u-pos       effort 10: repository A 6, B 4      -> A 0.6,  B 0.4
//	u-zero-task effort 0:  A 0, B 0, C 0            -> 1/3 each
//	u-zero-pr   effort 0:  A 5, B 5                 -> 0.5 each
//	u-zero-sum  effort 10: A 0, C 0                 -> 0.5 each
//	u-over      effort 10: A 15, B 5                -> A 0.75, B 0.25 (repository efforts above the unit effort)
//	u-under     effort 10: A 1, C 3                 -> A 0.25, C 0.75 (repository efforts below the unit effort)
func TestAllocationPathCountsZeroEffortUnitsOnce(t *testing.T) {
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

	const org = "org-9321"
	const repoA, repoB, repoC = "93210001-9321-4321-8321-932193219321", "93210002-9321-4321-8321-932193219321", "93210003-9321-4321-8321-932193219321"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}, {repoC, "acme/c"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-all', ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, r[0], r[1], at.AddDate(-1, 0, 0), at)
	}
	exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('team-all', generateUUIDv4(), 'Team All', [], [], [], 1, ?, ?, ?, 'github', 'team-all')`, at, at, org)
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	unit := func(id, repo string, effort float64) {
		raw, _ := json.Marshal(map[string]any{"issues": []string{"linear:" + id}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, repo, effort, themes, subs, string(raw), at.Add(time.Hour), org)
	}
	row := func(id, repo string, effort float64) {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES (?, ?, 'churn_loc', ?, 0, 'pr_refs', 'run-1', ?, ?)`, id, repo, effort, at.Add(time.Hour), org)
	}
	unit("u-pos", repoA, 10)
	unit("u-zero-task", repoA, 0)
	unit("u-zero-pr", repoA, 0)
	unit("u-zero-sum", repoA, 10)
	unit("u-over", repoA, 10)
	unit("u-under", repoA, 10)
	row("u-pos", repoA, 6)
	row("u-pos", repoB, 4)
	row("u-zero-task", repoA, 0)
	row("u-zero-task", repoB, 0)
	row("u-zero-task", repoC, 0)
	row("u-zero-pr", repoA, 5)
	row("u-zero-pr", repoB, 5)
	row("u-zero-sum", repoA, 0)
	row("u-zero-sum", repoC, 0)
	row("u-over", repoA, 15)
	row("u-over", repoB, 5)
	row("u-under", repoA, 1)
	row("u-under", repoC, 3)

	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	useInvestment := true
	sankeyIn := model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme, model.DimensionInputRepo},
		Measure:   model.MeasureInputCount,
		DateRange: dates,
		MaxNodes:  32,
		MaxEdges:  100,
	}
	scope := func(level model.ScopeLevelInput, ids ...string) *model.FilterInput {
		return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: level, Ids: ids}}
	}
	cases := []struct {
		name    string
		filters *model.FilterInput
		want    float64
	}{
		{"unscoped", nil, 6},
		{"team owning every repository", scope(model.ScopeLevelInputTeam, "team-all"), 6},
		{"every repository", scope(model.ScopeLevelInputRepo, repoA, repoB, repoC), 6},
		{"repository A", scope(model.ScopeLevelInputRepo, repoA), 0.6 + 1.0/3 + 0.5 + 0.5 + 0.75 + 0.25},
		{"repository B", scope(model.ScopeLevelInputRepo, repoB), 0.4 + 1.0/3 + 0.5 + 0.25},
		{"repository C", scope(model.ScopeLevelInputRepo, repoC), 1.0/3 + 0.5 + 0.75},
	}
	for _, tc := range cases {
		out, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Filters:       tc.filters,
			Sankey:        &sankeyIn,
			Breakdowns: []model.BreakdownRequestInput{{
				Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10,
			}},
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var breakdown, theme, repo float64
		if len(out.Breakdowns) != 1 {
			t.Fatalf("%s: breakdowns = %d, want 1", tc.name, len(out.Breakdowns))
		}
		for _, item := range out.Breakdowns[0].Items {
			if item.Value != nil {
				breakdown += *item.Value
			}
		}
		for _, n := range out.Sankey.Nodes {
			if n.Value == nil {
				continue
			}
			switch n.Dimension {
			case "THEME":
				theme += *n.Value
			case "REPO":
				repo += *n.Value
			}
		}
		for label, have := range map[string]float64{"breakdown": breakdown, "sankey THEME nodes": theme, "sankey REPO nodes": repo} {
			if math.Abs(have-tc.want) > 1e-9 {
				t.Errorf("%s: %s = %v, want %v", tc.name, label, have, tc.want)
			}
		}
	}
}
