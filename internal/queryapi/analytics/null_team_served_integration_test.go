//go:build integration

package analytics

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestEmptyTeamIsServedAsTheUnassignedTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-14281-empty-team"
	const otherOrg = "org-14281-other"
	const repoA = "11111111-1111-4111-8111-111111111111"
	const repoB = "22222222-2222-4222-8222-222222222222"
	const computed = "2026-01-06 00:00:00"
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "team-a", computed, 5)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "", computed, 3)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoB, "", computed, 4)
	seedWorkItemCompletionGeneration(t, ctx, conn, otherOrg, "2026-01-05", repoA, "team-z", computed, 100)

	dates := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}
	health := false
	result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &health,
		Timeseries: []model.TimeseriesRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, Interval: model.BucketIntervalInputDay, DateRange: dates,
		}},
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10,
		}},
		Sankey: &model.SankeyRequestInput{
			Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
			Measure: model.MeasureInputCount, DateRange: dates, MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
		},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wantTeams := map[string]float64{"team-a": 5, "unassigned": 7}

	t.Run("timeseries", func(t *testing.T) {
		got := map[string]float64{}
		for _, series := range result.Timeseries {
			if _, seen := got[series.DimensionValue]; seen {
				t.Errorf("timeseries serves the TEAM key %q twice", series.DimensionValue)
			}
			if len(series.Buckets) != 1 || series.Buckets[0].Value == nil {
				t.Fatalf("series %q buckets = %+v, want one bucket with a value", series.DimensionValue, series.Buckets)
			}
			got[series.DimensionValue] = *series.Buckets[0].Value
		}
		checkGroups(t, "timeseries", got, wantTeams)
	})
	t.Run("breakdown", func(t *testing.T) {
		if len(result.Breakdowns) != 1 {
			t.Fatalf("breakdowns = %d, want 1", len(result.Breakdowns))
		}
		got := map[string]float64{}
		for _, item := range result.Breakdowns[0].Items {
			if _, seen := got[item.Key]; seen {
				t.Errorf("breakdown serves the TEAM key %q twice", item.Key)
			}
			if item.Value == nil {
				t.Fatalf("breakdown item %q has no value", item.Key)
			}
			got[item.Key] = *item.Value
		}
		checkGroups(t, "breakdown", got, wantTeams)
	})
	t.Run("sankey", func(t *testing.T) {
		if result.Sankey == nil {
			t.Fatal("sankey is nil")
		}
		nodes := map[string]float64{}
		for _, node := range result.Sankey.Nodes {
			if node.Value == nil {
				t.Fatalf("sankey node %q has no value", node.ID)
			}
			nodes[node.ID] = *node.Value
		}
		checkGroups(t, "sankey nodes", nodes, map[string]float64{
			"TEAM:team-a": 5, "TEAM:unassigned": 7, "REPO:" + repoA: 8, "REPO:" + repoB: 4,
		})
		edges := map[string]float64{}
		for _, edge := range result.Sankey.Edges {
			if edge.Value == nil {
				t.Fatalf("sankey edge %s>%s has no value", edge.Source, edge.Target)
			}
			edges[edge.Source+">"+edge.Target] = *edge.Value
		}
		checkGroups(t, "sankey edges", edges, map[string]float64{
			"TEAM:team-a>REPO:" + repoA: 5, "TEAM:unassigned>REPO:" + repoA: 3, "TEAM:unassigned>REPO:" + repoB: 4,
		})
	})
}

func startEmptyTeamFixture(t *testing.T, ctx context.Context, org, otherOrg string) (stdclickhouse.Conn, QueryClient) {
	t.Helper()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	const repoA = "11111111-1111-4111-8111-111111111111"
	const repoB = "22222222-2222-4222-8222-222222222222"
	const computed = "2026-01-06 00:00:00"
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "team-a", computed, 5)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "", computed, 3)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoB, "", computed, 4)
	seedWorkItemCompletionGeneration(t, ctx, conn, otherOrg, "2026-01-05", repoA, "team-z", computed, 100)
	return conn, client
}

// The per-dimension sankey oracle (CompileSankey) names the empty team as the grouped statement does.
func TestEmptyTeamIsTheUnassignedTeamInThePerDimensionSankey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const org, otherOrg = "org-14281-oracle", "org-14281-oracle-other"
	_, client := startEmptyTeamFixture(t, ctx, org, otherOrg)
	const repoA = "11111111-1111-4111-8111-111111111111"
	const repoB = "22222222-2222-4222-8222-222222222222"

	nodesQuery, edgesQueries, err := CompileSankey(SankeyRequest{
		Path: []Dimension{DimensionTeam, DimensionRepo}, Measure: MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08"), MaxNodes: 16, MaxEdges: 100,
	}, org, 120, false, nil)
	if err != nil {
		t.Fatalf("CompileSankey: %v", err)
	}
	nodes, edges, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
	if err != nil {
		t.Fatalf("ExecuteSankeyQueries: %v", err)
	}
	gotNodes := map[string]float64{}
	for _, n := range nodes {
		if n.Value == nil {
			t.Fatalf("node %q has no value", n.ID)
		}
		gotNodes[n.ID] = *n.Value
	}
	checkGroups(t, "oracle nodes", gotNodes, map[string]float64{
		"TEAM:team-a": 5, "TEAM:unassigned": 7, "REPO:" + repoA: 8, "REPO:" + repoB: 4,
	})
	gotEdges := map[string]float64{}
	for _, e := range edges {
		if e.Value == nil {
			t.Fatalf("edge %s>%s has no value", e.Source, e.Target)
		}
		gotEdges[e.Source+">"+e.Target] = *e.Value
	}
	checkGroups(t, "oracle edges", gotEdges, map[string]float64{
		"TEAM:team-a>REPO:" + repoA: 5, "TEAM:unassigned>REPO:" + repoA: 3, "TEAM:unassigned>REPO:" + repoB: 4,
	})
}

// The work-item team breakdown the context fabric serves (TEAM x CYCLE_TIME_HOURS, useInvestment false) keys the
// empty team "unassigned".
func TestEmptyTeamIsTheUnassignedTeamInTheCycleTimeBreakdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const org, otherOrg = "org-14281-cycle", "org-14281-cycle-other"
	_, client := startEmptyTeamFixture(t, ctx, org, otherOrg)
	dates := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}
	health := false
	result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &health,
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCycleTimeHours, DateRange: dates, TopN: 10,
		}},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Breakdowns) != 1 {
		t.Fatalf("breakdowns = %d, want 1", len(result.Breakdowns))
	}
	got := map[string]float64{}
	for _, item := range result.Breakdowns[0].Items {
		if item.Value == nil {
			t.Fatalf("item %q has no value", item.Key)
		}
		got[item.Key] = *item.Value
	}
	checkGroups(t, "cycle time breakdown", got, map[string]float64{"team-a": 2, "unassigned": 2})
}

// A tie at a cut keeps the group the stored key keeps: the empty team sorts before "gh:payments" as its stored key,
// whatever name it is served under.
func TestEmptyTeamTieAtTheCutKeepsTheStoredKeyOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const org, otherOrg = "org-14281-tie", "org-14281-tie-other"
	conn, client := startEmptyTeamFixture(t, ctx, org, otherOrg)
	const repoA = "11111111-1111-4111-8111-111111111111"
	const computed = "2026-01-06 00:00:00"
	// startEmptyTeamFixture seeded team-a 5 and the empty team 7; replace the tie by new rows in a second window.
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-02-05", repoA, "gh:payments", computed, 1)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-02-05", repoA, "", computed, 1)
	seedWorkItemCompletionGeneration(t, ctx, conn, otherOrg, "2026-02-05", repoA, "gh:payments", computed, 50)
	dates := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-02-01"), EndDate: mustGraphQLDate("2026-02-08")}
	health := false
	result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &health,
		Timeseries: []model.TimeseriesRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, Interval: model.BucketIntervalInputDay, DateRange: dates,
		}},
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, DateRange: dates, TopN: 1,
		}},
		Sankey: &model.SankeyRequestInput{
			Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
			Measure: model.MeasureInputCount, DateRange: dates, MaxNodes: 2, MaxEdges: 1, UseInvestment: &health,
		},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var seriesOrder []string
	for _, s := range result.Timeseries {
		seriesOrder = append(seriesOrder, s.DimensionValue)
	}
	if len(seriesOrder) != 2 || seriesOrder[0] != "unassigned" || seriesOrder[1] != "gh:payments" {
		t.Errorf("timeseries order = %v, want [unassigned gh:payments] (the stored key order of base)", seriesOrder)
	}
	if len(result.Breakdowns) != 1 || len(result.Breakdowns[0].Items) != 1 || result.Breakdowns[0].Items[0].Key != "unassigned" {
		t.Errorf("breakdown topN 1 = %+v, want the group base keeps: unassigned", result.Breakdowns)
	}
	if result.Sankey == nil {
		t.Fatal("sankey is nil")
	}
	for _, node := range result.Sankey.Nodes {
		if node.Dimension == "TEAM" && node.ID != "TEAM:unassigned" {
			t.Errorf("sankey TEAM node at the cut = %q, want TEAM:unassigned", node.ID)
		}
	}
	for _, edge := range result.Sankey.Edges {
		if edge.Source != "TEAM:unassigned" {
			t.Errorf("sankey edge at the cut = %s>%s, want source TEAM:unassigned", edge.Source, edge.Target)
		}
	}
}

// A scope on "unassigned" selects the rows with the empty team, and the sankey coverage scopes on the same rows as the
// chart: the scope ["gh:payments","unassigned"] and the stored form ["gh:payments",""] give one coverage.
func TestEmptyTeamScopeAndCoverageSelectTheStoredRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const org, otherOrg = "org-14281-scope", "org-14281-scope-other"
	conn, client := startEmptyTeamFixture(t, ctx, org, otherOrg)
	const repoA = "11111111-1111-4111-8111-111111111111"
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "gh:payments", "2026-01-06 00:00:00", 9)
	dates := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}
	health := false
	run := func(ids []string) *model.AnalyticsResult {
		result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &health,
			Filters:       &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: ids}},
			Breakdowns: []model.BreakdownRequestInput{{
				Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10,
			}},
			Sankey: &model.SankeyRequestInput{
				Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
				Measure: model.MeasureInputCount, DateRange: dates, MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
			},
		})
		if err != nil {
			t.Fatalf("Resolve %v: %v", ids, err)
		}
		return result
	}
	only := run([]string{"unassigned"})
	got := map[string]float64{}
	for _, item := range only.Breakdowns[0].Items {
		got[item.Key] = *item.Value
	}
	checkGroups(t, "breakdown scoped to unassigned", got, map[string]float64{"unassigned": 7})

	byName := run([]string{"gh:payments", "unassigned"})
	byStored := run([]string{"gh:payments", ""})
	if byName.Sankey == nil || byName.Sankey.Coverage == nil || byStored.Sankey == nil || byStored.Sankey.Coverage == nil {
		t.Fatal("sankey coverage is nil")
	}
	if byName.Sankey.Coverage.TeamCoverage != byStored.Sankey.Coverage.TeamCoverage {
		t.Errorf("team coverage by name = %v, by stored key = %v, want equal", byName.Sankey.Coverage.TeamCoverage, byStored.Sankey.Coverage.TeamCoverage)
	}
	chart := map[string]float64{}
	for _, node := range byName.Sankey.Nodes {
		if node.Dimension == "TEAM" {
			chart[node.ID] = *node.Value
		}
	}
	checkGroups(t, "sankey TEAM nodes in scope", chart, map[string]float64{"TEAM:gh:payments": 9, "TEAM:unassigned": 7})
	t.Logf("team coverage %v", byName.Sankey.Coverage.TeamCoverage)
}

// The per-dimension sankey keeps the stored key at its node and edge cuts, as the grouped statement does.
func TestEmptyTeamTieAtTheCutInThePerDimensionSankey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const org, otherOrg = "org-14281-tie-oracle", "org-14281-tie-oracle-other"
	conn, client := startEmptyTeamFixture(t, ctx, org, otherOrg)
	const repoA = "11111111-1111-4111-8111-111111111111"
	const computed = "2026-01-06 00:00:00"
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-02-05", repoA, "gh:payments", computed, 1)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-02-05", repoA, "", computed, 1)
	seedWorkItemCompletionGeneration(t, ctx, conn, otherOrg, "2026-02-05", repoA, "gh:payments", computed, 50)
	for _, path := range [][]Dimension{{DimensionTeam, DimensionRepo}, {DimensionRepo, DimensionTeam}} {
		nodesQuery, edgesQueries, err := CompileSankey(SankeyRequest{
			Path: path, Measure: MeasureCount,
			StartDate: mustGraphQLDate("2026-02-01"), EndDate: mustGraphQLDate("2026-02-08"), MaxNodes: 2, MaxEdges: 1,
		}, org, 120, false, nil)
		if err != nil {
			t.Fatalf("CompileSankey %v: %v", path, err)
		}
		nodes, edges, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
		if err != nil {
			t.Fatalf("ExecuteSankeyQueries %v: %v", path, err)
		}
		for _, n := range nodes {
			if n.Dimension == "TEAM" && n.ID != "TEAM:unassigned" {
				t.Errorf("path %v: TEAM node at the cut = %q, want TEAM:unassigned", path, n.ID)
			}
		}
		for _, e := range edges {
			team := e.Source
			if path[0] == DimensionRepo {
				team = e.Target
			}
			if team != "TEAM:unassigned" {
				t.Errorf("path %v: edge at the cut = %s>%s, want the TEAM:unassigned endpoint", path, e.Source, e.Target)
			}
		}
		if len(nodes) == 0 || len(edges) == 0 {
			t.Errorf("path %v: nodes %d edges %d, want both", path, len(nodes), len(edges))
		}
	}
}
