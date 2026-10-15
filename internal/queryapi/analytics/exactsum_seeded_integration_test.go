//go:build integration

package analytics

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const exactSumRuns = 20

// sameEveryRun runs read exactSumRuns times and fails when any answer differs
// from the first. It returns the first answer.
func sameEveryRun[T any](t *testing.T, name string, read func() T) T {
	t.Helper()
	first := read()
	for run := 1; run < exactSumRuns; run++ {
		if got := read(); !reflect.DeepEqual(got, first) {
			t.Fatalf("%s: run %d differs from run 0:\n run 0: %+v\n run %d: %+v", name, run, first, run, got)
		}
	}
	return first
}

// The timeseries, breakdown and Sankey reads of the investment path sum the
// same Float32 weights flowMatrix does. Two themes carry the same multiset of
// k * 2^e weights (74 bits wide, so a Float64 sum of them depends on order) in
// a different part order: every run must give one answer, the two themes must
// tie at the exact total, and the tie-break must order them.
func TestInvestmentReadsGiveOneExactAnswerOnEveryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-exact-sum-reads"
	for part := 0; part < exactSumParts; part++ {
		seedExactSumPart(ctx, t, admin, orgID, "feature_delivery.build", part, part)
		seedExactSumPart(ctx, t, admin, orgID, "quality.testing", part, exactSumParts-1-part)
	}
	exact := exactSumTotal()
	start, end := mustDate(t, "2026-01-01"), mustDate(t, "2026-01-31")

	t.Run("timeseries", func(t *testing.T) {
		query, err := CompileTimeseries(TimeseriesRequest{Dimension: DimensionTheme, Measure: MeasureCount, Interval: BucketIntervalMonth, StartDate: start, EndDate: end}, orgID, 120, true, nil)
		if err != nil {
			t.Fatalf("CompileTimeseries: %v", err)
		}
		series := sameEveryRun(t, "timeseries", func() []model.TimeseriesResult {
			got, err := ExecuteTimeseries(ctx, client, query, "theme", "count")
			if err != nil {
				t.Fatalf("ExecuteTimeseries: %v", err)
			}
			return got
		})
		if len(series) != 2 {
			t.Fatalf("timeseries = %+v, want the two seeded themes", series)
		}
		for _, s := range series {
			if len(s.Buckets) != 1 || s.Buckets[0].Value == nil || !closeToExact(*s.Buckets[0].Value, exact) {
				t.Errorf("timeseries %s = %+v, want one bucket of %.17g", s.DimensionValue, s.Buckets, exact)
			}
		}
	})

	t.Run("breakdown", func(t *testing.T) {
		query, err := CompileBreakdown(BreakdownRequest{Dimension: DimensionTheme, Measure: MeasureCount, StartDate: start, EndDate: end, TopN: 10}, orgID, 120, true, nil)
		if err != nil {
			t.Fatalf("CompileBreakdown: %v", err)
		}
		breakdown := sameEveryRun(t, "breakdown", func() model.BreakdownResult {
			got, err := ExecuteBreakdown(ctx, client, orgID, query, "theme", "count")
			if err != nil {
				t.Fatalf("ExecuteBreakdown: %v", err)
			}
			return got
		})
		if len(breakdown.Items) != 2 || breakdown.Items[0].Key != "feature_delivery" || breakdown.Items[1].Key != "quality" {
			t.Fatalf("breakdown items = %+v, want feature_delivery then quality (a tie, ordered by key)", breakdown.Items)
		}
		for _, item := range breakdown.Items {
			if item.Value == nil || !closeToExact(*item.Value, exact) {
				t.Errorf("breakdown %s = %s, want %.17g", item.Key, flowValueText(item.Value), exact)
			}
		}
	})

	t.Run("sankey", func(t *testing.T) {
		nodesQuery, edgesQueries, err := CompileSankey(SankeyRequest{Path: []Dimension{DimensionTheme, DimensionSubcategory}, Measure: MeasureCount, StartDate: start, EndDate: end, MaxNodes: 20, MaxEdges: 50}, orgID, 120, true, nil)
		if err != nil {
			t.Fatalf("CompileSankey: %v", err)
		}
		type sankeyAnswer struct {
			Nodes []model.SankeyNode
			Edges []model.SankeyEdge
		}
		sankey := sameEveryRun(t, "sankey", func() sankeyAnswer {
			nodes, edges, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
			if err != nil {
				t.Fatalf("ExecuteSankeyQueries: %v", err)
			}
			return sankeyAnswer{nodes, edges}
		})
		if len(sankey.Nodes) != 4 || len(sankey.Edges) != 2 {
			t.Fatalf("sankey = %s / %s, want 4 nodes and 2 edges", flowNodesText(sankey.Nodes), flowEdgesText(sankey.Edges))
		}
		for _, n := range sankey.Nodes {
			if n.Value == nil || !closeToExact(*n.Value, exact) {
				t.Errorf("sankey node %s = %s, want %.17g", n.ID, flowValueText(n.Value), exact)
			}
		}
		for _, e := range sankey.Edges {
			if e.Value == nil || !closeToExact(*e.Value, exact) {
				t.Errorf("sankey edge %s>%s = %s, want %.17g", e.Source, e.Target, flowValueText(e.Value), exact)
			}
		}
	})
}

func repoEffortRepoID(number int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", number)
}

// repoEffortValue is row number's value in series u: k * 2^e, 74 bits wide
// over a series.
func repoEffortValue(u, number int) (k int64, shift uint) {
	return int64((number*7919+u*104729)%1000 + 1), uint((number + u) % 55)
}

const (
	testRateRepos = 2
	testRateDays  = 3200
)

// The non-investment measures average Float64 rows (a pass rate here). Each
// repository carries testRateDays daily rows, 74 bits wide, in exactSumParts
// parts: the breakdown must give one answer every run, at the exact mean.
func TestNonInvestmentAveragesGiveOneExactAnswerOnEveryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-exact-average"
	per := testRateDays / exactSumParts
	for r := 0; r < testRateRepos; r++ {
		for part := 0; part < exactSumParts; part++ {
			crossorg.Exec(ctx, t, admin, fmt.Sprintf(`
INSERT INTO testops_test_metrics_daily (repo_id, day, pass_rate, org_id, computed_at)
SELECT toUUID('%[1]s'), toDate('2017-01-01') + n,
    ((n * 7919 + %[2]d * 104729) %% 1000 + 1) * pow(2, toInt64((n + %[2]d) %% 55) - 30), '%[3]s', toDateTime('2026-01-06 00:00:00')
FROM (SELECT number + %[4]d AS n FROM numbers(%[5]d))`, repoEffortRepoID(r), r, orgID, part*per, per))
		}
	}

	query, err := CompileBreakdown(BreakdownRequest{Dimension: DimensionRepo, Measure: MeasureTestPassRate,
		StartDate: mustDate(t, "2017-01-01"), EndDate: mustDate(t, "2026-01-01"), TopN: 10}, orgID, 120, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}
	got := sameEveryRun(t, "pass-rate breakdown", func() model.BreakdownResult {
		result, err := ExecuteBreakdown(ctx, client, orgID, query, "repo", "test_pass_rate")
		if err != nil {
			t.Fatalf("ExecuteBreakdown: %v", err)
		}
		return result
	})
	if len(got.Items) != testRateRepos {
		t.Fatalf("pass-rate breakdown = %+v, want one item per seeded repository", got.Items)
	}
	for r := 0; r < testRateRepos; r++ {
		total := new(big.Int)
		for n := 0; n < testRateDays; n++ {
			k, shift := repoEffortValue(r, n)
			total.Add(total, new(big.Int).Lsh(big.NewInt(k), shift))
		}
		mean, _ := new(big.Rat).SetFrac(total, new(big.Int).Lsh(big.NewInt(testRateDays), 30)).Float64()
		found := false
		for _, item := range got.Items {
			if item.Key == repoEffortRepoID(r) {
				found = true
				if item.Value == nil || math.Abs(*item.Value-mean*100) > 1e-14*mean*100 {
					t.Errorf("repository %d mean = %s, want %.17g", r, flowValueText(item.Value), mean*100)
				}
			}
		}
		if !found {
			t.Errorf("repository %d missing from %+v", r, got.Items)
		}
	}
}
