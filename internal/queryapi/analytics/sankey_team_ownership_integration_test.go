//go:build integration

package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9297: the Investment flow's TEAM node is the owner of the repository of the effort row, against a real
// engine, through the grouped statement (Resolve) AND the per-dimension statements (CompileSankey).
//
//	u-a      effort 10  -> repository A (team-a, named "Team A")                       -> Team A 1.0
//	u-shared effort 10  -> repository S owned by team-a AND team-b (no teams row)      -> 0.5 / 0.5
//	u-none   effort 10  -> repository N, no owner; an attribution row votes team-vote  -> unassigned 1.0
//	u-mixed  effort 10  -> repository A 6, repository N 4                              -> Team A 0.6, unassigned 0.4
//
// Totals: Team A 1.0 + 0.5 + 0.6 = 2.1, team-b 0.5, unassigned 1.4; the unit total is 4.0 on every node dimension.
func TestFlowTeamIsRepositoryOwnership(t *testing.T) {
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

	const org = "org-9297"
	const repoA, repoS, repoN = "92970001-9297-4297-8297-929792979297", "92970002-9297-4297-8297-929792979297", "92970003-9297-4297-8297-929792979297"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoS, "acme/shared"}, {repoN, "acme/none"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
	}
	exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('team-a', generateUUIDv4(), 'Team A', [], [], [], 1, ?, ?, ?, 'github', 'team-a')`, at, at, org)
	owns := func(team, repo, name string) {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, team, repo, name, at.AddDate(-1, 0, 0), at)
	}
	owns("team-a", repoA, "acme/a")
	owns("team-a", repoS, "acme/shared")
	owns("team-b", repoS, "acme/shared")
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	unit := func(id, repo, issue string) {
		raw, _ := json.Marshal(map[string]any{"issues": []string{issue}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', 10, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, repo, themes, subs, string(raw), at.Add(time.Hour), org)
	}
	row := func(id, repo string, effort float64) {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES (?, ?, 'churn_loc', ?, 1, 'pr_refs', 'run-1', ?, ?)`, id, repo, effort, at.Add(time.Hour), org)
	}
	unit("u-a", repoA, "linear:A-1")
	unit("u-shared", repoS, "linear:S-1")
	unit("u-none", repoN, "linear:VOTED-1")
	unit("u-mixed", repoA, "linear:M-1")
	row("u-a", repoA, 10)
	row("u-shared", repoS, 10)
	row("u-none", repoN, 10)
	row("u-mixed", repoA, 6)
	row("u-mixed", repoN, 4)
	exec(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES (?, ?, 'linear:VOTED-1', 'linear', 'team-vote', 'Vote Team', 'native_team', 1, 'high', '', ?)`, org, repoN, at)
	exec(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES (?, ?, 'linear:A-1', 'linear', 'team-vote', 'Vote Team', 'native_team', 1, 'high', '', ?)`, org, repoA, at)

	dates := &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))}
	input := func(path ...model.DimensionInput) model.SankeyRequestInput {
		return model.SankeyRequestInput{Path: path, Measure: model.MeasureInputCount, DateRange: dates, MaxNodes: 32, MaxEdges: 100}
	}
	teamTheme := input(model.DimensionInputTeam, model.DimensionInputTheme, model.DimensionInputRepo)

	nodeValues := func(nodes []model.SankeyNode, dimension string) map[string]float64 {
		out := map[string]float64{}
		for _, n := range nodes {
			if n.Dimension == dimension && n.Value != nil {
				out[n.ID] = *n.Value
			}
		}
		return out
	}
	near := func(have, want float64) bool { return math.Abs(have-want) < 1e-9 }

	// Grouped statement (Resolve) and per-dimension statements (CompileSankey) answer the same.
	grouped, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{Sankey: &teamTheme})
	if err != nil {
		t.Fatal(err)
	}
	req, err := SankeyRequestFromInput(teamTheme)
	if err != nil {
		t.Fatal(err)
	}
	nodesQ, edgesQ, err := CompileSankey(req, org, 60, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	plainNodes, plainEdges, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQ}, edgesQ)
	if err != nil {
		t.Fatal(err)
	}
	if g, p := nodeValues(grouped.Sankey.Nodes, "TEAM"), nodeValues(plainNodes, "TEAM"); !reflect.DeepEqual(g, p) {
		t.Errorf("grouped TEAM nodes %v differ from per-dimension %v", g, p)
	}
	if len(grouped.Sankey.Edges) != len(plainEdges) {
		t.Errorf("grouped carries %d edges, per-dimension %d", len(grouped.Sankey.Edges), len(plainEdges))
	}

	team := nodeValues(grouped.Sankey.Nodes, "TEAM")
	want := map[string]float64{"TEAM:Team A": 2.1, "TEAM:team-b": 0.5, "TEAM:unassigned": 1.4}
	for id, v := range want {
		if !near(team[id], v) {
			t.Errorf("TEAM node %s = %v, want %v (all: %v)", id, team[id], v, team)
		}
	}
	for id := range team {
		if strings.Contains(id, "Vote") || strings.Contains(id, "team-vote") {
			t.Errorf("TEAM node %q is the work-item vote; the flow reads repository ownership", id)
		}
	}
	// A shared repository is split, never counted twice: every node dimension carries the 4.0 units.
	for _, dim := range []string{"TEAM", "THEME", "REPO"} {
		var sum float64
		for _, v := range nodeValues(grouped.Sankey.Nodes, dim) {
			sum += v
		}
		if !near(sum, 4) {
			t.Errorf("%s nodes carry %v units, want 4", dim, sum)
		}
	}
	// The unassigned share of the flow against the coverage of the same request: u-mixed is assigned whole by the
	// coverage (a unit is assigned when one of its repositories is owned) and split by the flow.
	coverage := resolveSankeyCoverage(ctx, client, org, req, 60, true, nil)
	if coverage == nil {
		t.Fatal("coverage is nil")
	}
	flowUnassigned := team["TEAM:unassigned"] / 4
	if !near(flowUnassigned, 0.35) || !near(1-coverage.TeamCoverage, 0.25) {
		t.Errorf("flow unassigned share %v (want 0.35), 1 - teamCoverage %v (want 0.25: the coverage assigns u-mixed whole)", flowUnassigned, 1-coverage.TeamCoverage)
	}

	// A team scope narrows the owners to the team in view: the shared repository is Team A's whole, and no team-b node.
	scoped, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		Sankey:  &teamTheme,
		Filters: &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	scopedTeam := nodeValues(scoped.Sankey.Nodes, "TEAM")
	if len(scopedTeam) != 1 || !near(scopedTeam["TEAM:Team A"], 2.6) {
		t.Errorf("team scope team-a: TEAM nodes %v, want only Team A = 2.6 (u-a 1.0, u-shared 1.0, u-mixed 0.6)", scopedTeam)
	}
}

// A thousand repositories owned by one team stay one node: the owner set is joined server-side and no row of it
// crosses the 1,000-row cap of the query client.
func TestFlowTeamOwnershipAboveTheRowCap(t *testing.T) {
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

	const org = "org-9297-big"
	const repos = 1200
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(fmt.Sprintf(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
SELECT generateUUIDv4(number + 1), concat('acme/r', toString(number)), 'github', ?, ?, ? FROM numbers(%d)`, repos), org, at, at)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
SELECT org_id, 'github', 'team-big', id, repo, 'exact', 'native', 1, 100, 0, ?, NULL, ? FROM repos FINAL WHERE org_id = ?`, at.AddDate(-1, 0, 0), at, org)
	themes := map[string]float64{"feature_delivery": 1}
	subs := map[string]float64{"feature_delivery.roadmap": 1}
	raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('u-big', ?, ?, NULL, 'churn_loc', 1200, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, at.Add(-24*time.Hour), at, themes, subs, string(raw), at.Add(time.Hour), org)
	exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id)
SELECT 'u-big', id, 'churn_loc', 1, 1, 'pr_refs', 'run-1', ?, org_id FROM repos FINAL WHERE org_id = ?`, at.Add(time.Hour), org)

	in := model.SankeyRequestInput{
		Path:      []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure:   model.MeasureInputCount,
		DateRange: &model.DateRangeInput{StartDate: graphqldate.New(at.AddDate(0, 0, -3)), EndDate: graphqldate.New(at.AddDate(0, 0, 3))},
		MaxNodes:  16,
		MaxEdges:  100,
	}
	out, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{Sankey: &in})
	if err != nil {
		t.Fatal(err)
	}
	var have float64
	for _, n := range out.Sankey.Nodes {
		if n.ID == "TEAM:team-big" && n.Value != nil {
			have = *n.Value
		}
	}
	if math.Abs(have-1) > 1e-9 {
		t.Errorf("TEAM:team-big carries %v, want 1.0 (the unit's whole effort across %d owned repositories)", have, repos)
	}
}
