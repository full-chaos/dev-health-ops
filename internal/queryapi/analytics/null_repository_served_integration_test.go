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

// A work item with no repository is stored with a NULL repo_id (workitemengine). On the health path the timeseries and
// the breakdown serve those rows as ONE unassigned group keyed "", the sum of every such row: no other field of their
// answer would show that share. A repository's key is its UUID text, so the all-zero UUID is a repository of its own.
// The sankey serves no node for them (its edges never kept them): the team nodes keep them and repoCoverage shows the
// share.
func TestNullRepositoryIsOneUnassignedGroupAndNoSankeyNode(t *testing.T) {
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

	const org = "org-14271-null-repo"
	const repoA = "11111111-1111-4111-8111-111111111111"
	const zeroRepo = "00000000-0000-0000-0000-000000000000"
	const computed = "2026-01-06 00:00:00"
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", repoA, "team-a", computed, 5)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", zeroRepo, "team-a", computed, 2)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", "", "team-a", computed, 3)
	seedWorkItemCompletionGeneration(t, ctx, conn, org, "2026-01-05", "", "team-b", computed, 4)

	dates := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}
	health := false
	result, err := Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &health,
		Timeseries: []model.TimeseriesRequestInput{{
			Dimension: model.DimensionInputRepo, Measure: model.MeasureInputCount, Interval: model.BucketIntervalInputDay, DateRange: dates,
		}},
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputRepo, Measure: model.MeasureInputCount, DateRange: dates, TopN: 10,
		}},
		Sankey: &model.SankeyRequestInput{
			Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
			Measure: model.MeasureInputCount, DateRange: dates, MaxNodes: 16, MaxEdges: 100, UseInvestment: &health,
		},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wantGroups := map[string]float64{repoA: 5, zeroRepo: 2, "": 7}

	t.Run("timeseries", func(t *testing.T) {
		got := map[string]float64{}
		for _, series := range result.Timeseries {
			if _, seen := got[series.DimensionValue]; seen {
				t.Errorf("timeseries serves the REPO key %q twice", series.DimensionValue)
			}
			if len(series.Buckets) != 1 || series.Buckets[0].Value == nil {
				t.Fatalf("series %q buckets = %+v, want one bucket with a value", series.DimensionValue, series.Buckets)
			}
			got[series.DimensionValue] = *series.Buckets[0].Value
		}
		checkGroups(t, "timeseries", got, wantGroups)
	})
	t.Run("breakdown", func(t *testing.T) {
		if len(result.Breakdowns) != 1 {
			t.Fatalf("breakdowns = %d, want 1", len(result.Breakdowns))
		}
		got := map[string]float64{}
		for _, item := range result.Breakdowns[0].Items {
			if _, seen := got[item.Key]; seen {
				t.Errorf("breakdown serves the REPO key %q twice", item.Key)
			}
			if item.Value == nil {
				t.Fatalf("breakdown item %q has no value", item.Key)
			}
			got[item.Key] = *item.Value
		}
		checkGroups(t, "breakdown", got, wantGroups)
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
			"TEAM:team-a": 10, "TEAM:team-b": 4, "REPO:" + repoA: 5, "REPO:" + zeroRepo: 2,
		})
		edges := map[string]float64{}
		for _, edge := range result.Sankey.Edges {
			if edge.Value == nil {
				t.Fatalf("sankey edge %s>%s has no value", edge.Source, edge.Target)
			}
			edges[edge.Source+">"+edge.Target] = *edge.Value
		}
		checkGroups(t, "sankey edges", edges, map[string]float64{
			"TEAM:team-a>REPO:" + repoA: 5, "TEAM:team-a>REPO:" + zeroRepo: 2,
		})
		if result.Sankey.Coverage == nil || result.Sankey.Coverage.RepoCoverage != 0.5 {
			t.Errorf("sankey coverage = %+v, want repoCoverage 0.5: two of the four rows have no repository", result.Sankey.Coverage)
		}
	})
}

func checkGroups(t *testing.T, what string, got, want map[string]float64) {
	t.Helper()
	t.Logf("%s served: %v", what, got)
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
		return
	}
	for key, value := range want {
		if have, ok := got[key]; !ok || have != value {
			t.Errorf("%s[%q] = %v (present %v), want %v; all: %v", what, key, have, ok, value, got)
		}
	}
}
