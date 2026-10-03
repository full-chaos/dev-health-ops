package capacityforecast

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

func expandBins(bins []model.CapacityDistributionBin) []int {
	var samples []int
	for _, bin := range bins {
		for i := 0; i < bin.Count; i++ {
			samples = append(samples, bin.Value)
		}
	}
	return samples
}

// CHAOS-7624, ruling (c): the singular `capacityForecast` fills the
// distribution from the same in-request simulation its percentiles come from.
// Recomputing the percentiles from the returned bins must give the percentiles
// the response itself carries.
func TestResolveForecastCarriesTheDistributionOfItsOwnSimulation(t *testing.T) {
	targetDate := graphqldate.New(day(t, "2026-10-01"))
	targetItems := 25
	input := &model.CapacityForecastInput{
		TargetItems: &targetItems,
		TargetDate:  &targetDate,
		HistoryDays: 30,
		Simulations: 200,
	}
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}

	got, err := ResolveForecast(context.Background(), client, "org-7", input, day(t, "2026-09-01"))
	if err != nil || got == nil {
		t.Fatalf("ResolveForecast: %v, %v", got, err)
	}
	distribution := got.CompletionDistribution
	if distribution == nil || distribution.Days == nil || distribution.Items == nil {
		t.Fatalf("completionDistribution = %+v, want both modes (both ran)", distribution)
	}

	days, items := expandBins(distribution.Days), expandBins(distribution.Items)
	if len(days) != 200 || len(items) != 200 {
		t.Errorf("runs in the bins = %d days / %d items, want 200 / 200", len(days), len(items))
	}
	// CHAOS-8477: the run total is served, and it is the simulation count the
	// request asked for and the sum of each mode's counts.
	if distribution.Runs != 200 {
		t.Errorf("runs = %d, want the 200 simulations of the request", distribution.Runs)
	}
	d := numerical.IntegerPercentiles(days, []float64{50, 85, 95})
	if d[0] != *got.P50Days || d[1] != *got.P85Days || d[2] != *got.P95Days {
		t.Errorf("percentiles from the day bins = %v, response carries %d/%d/%d", d, *got.P50Days, *got.P85Days, *got.P95Days)
	}
	it := numerical.IntegerPercentiles(items, []float64{50, 15, 5})
	if it[0] != *got.P50Items || it[1] != *got.P85Items || it[2] != *got.P95Items {
		t.Errorf("percentiles from the item bins = %v, response carries %d/%d/%d", it, *got.P50Items, *got.P85Items, *got.P95Items)
	}
}

func TestResolveForecastHasNoItemsDistributionWithoutATargetDate(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}
	got, err := ResolveForecast(context.Background(), client, "org-7", nil, day(t, "2026-09-01"))
	if err != nil || got == nil {
		t.Fatalf("ResolveForecast: %v, %v", got, err)
	}
	if got.CompletionDistribution == nil || got.CompletionDistribution.Days == nil {
		t.Fatalf("completionDistribution = %+v, want the days histogram", got.CompletionDistribution)
	}
	if got.CompletionDistribution.Items != nil {
		t.Errorf("items = %v, want null: the fixed-date mode did not run", got.CompletionDistribution.Items)
	}
}

// The two paths must render ONE distribution for one simulation. With a fixed
// seed the kernel is deterministic, so the singular response and a persisted
// row written from the same kernel result must give the same bins.
//
// The persisted side is built the way the writer narrows (UInt16 days, UInt32
// counts and items); the writer's own narrowing is pinned in package
// remaining. This is the half that proves the two readers agree.
func TestSingularAndPersistedPathsGiveTheSameDistributionForOneSeed(t *testing.T) {
	items := 60
	target := day(t, "2026-10-15")
	result, err := numerical.ForecastCapacity(numerical.ForecastRequest{
		History: numerical.Throughput{
			DailyThroughputs: []int{3, 8, 1, 5, 13, 2, 9, 4, 6, 7, 0, 11},
			DaysOfHistory:    12,
		},
		TargetItems: &items,
		TargetDate:  &target,
		Simulations: 500,
		Seed:        20260702,
	}, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("kernel: %v", err)
	}
	if result.DaysHistogram == nil || result.ItemsHistogram == nil {
		t.Fatal("the kernel gave no histogram for a both-modes request")
	}

	singular := forecastToModel(day(t, "2026-09-01"), nil, nil, 100, items, nil, result)

	var daysValues []uint16
	var daysCounts, itemsValues, itemsCounts []uint32
	for i, value := range result.DaysHistogram.Values {
		daysValues = append(daysValues, uint16(value))
		daysCounts = append(daysCounts, uint32(result.DaysHistogram.Counts[i]))
	}
	for i, value := range result.ItemsHistogram.Values {
		itemsValues = append(itemsValues, uint32(value))
		itemsCounts = append(itemsCounts, uint32(result.ItemsHistogram.Counts[i]))
	}
	row := withDistribution(persistedRow(t, "same-seed", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)),
		daysValues, daysCounts, itemsValues, itemsCounts)
	persisted := resolvePersistedDistribution(t, row)

	if singular.CompletionDistribution == nil || persisted.CompletionDistribution == nil {
		t.Fatalf("singular = %+v, persisted = %+v, want both set",
			singular.CompletionDistribution, persisted.CompletionDistribution)
	}
	if !reflect.DeepEqual(singular.CompletionDistribution, persisted.CompletionDistribution) {
		t.Errorf("singular and persisted distributions differ:\n singular  %+v\n persisted %+v",
			singular.CompletionDistribution, persisted.CompletionDistribution)
	}
	if len(singular.CompletionDistribution.Days) < 2 {
		t.Errorf("only %d day bins: the comparison would pass on a degenerate distribution",
			len(singular.CompletionDistribution.Days))
	}
}

// CHAOS-8477: `runs` is the sum of the counts of the served bins, for the mode
// that simulated. A caller draws the cumulative share of the runs from it and
// must never add the counts up itself.
func TestDistributionRunsIsTheSumOfTheServedCounts(t *testing.T) {
	sum := func(bins []model.CapacityDistributionBin) int {
		total := 0
		for _, bin := range bins {
			total += bin.Count
		}
		return total
	}
	days := &numerical.Histogram{Values: []int{3, 5, 9}, Counts: []int{10, 60, 30}}
	items := &numerical.Histogram{Values: []int{40, 55}, Counts: []int{25, 75}}

	t.Run("both modes", func(t *testing.T) {
		got := distributionToModel(days, items)
		if got == nil {
			t.Fatal("no distribution for two simulated modes")
		}
		if got.Runs != 100 {
			t.Errorf("runs = %d, want 100", got.Runs)
		}
		if got.Runs != sum(got.Days) || got.Runs != sum(got.Items) {
			t.Errorf("runs = %d, the served counts sum to %d (days) and %d (items)", got.Runs, sum(got.Days), sum(got.Items))
		}
	})
	t.Run("the days mode only", func(t *testing.T) {
		got := distributionToModel(days, nil)
		if got == nil || got.Items != nil {
			t.Fatalf("distribution = %+v, want the days mode only", got)
		}
		if got.Runs != 100 || got.Runs != sum(got.Days) {
			t.Errorf("runs = %d, want the 100 of the day counts", got.Runs)
		}
	})
	t.Run("the items mode only", func(t *testing.T) {
		got := distributionToModel(nil, items)
		if got == nil || got.Days != nil {
			t.Fatalf("distribution = %+v, want the items mode only", got)
		}
		if got.Runs != 100 || got.Runs != sum(got.Items) {
			t.Errorf("runs = %d, want the 100 of the item counts", got.Runs)
		}
	})
	t.Run("no mode simulated: no distribution, so no run total", func(t *testing.T) {
		if got := distributionToModel(nil, nil); got != nil {
			t.Fatalf("distribution = %+v, want nil: a run total of 0 would read as a simulation of zero runs", got)
		}
	})
	t.Run("a stored histogram with one count too many: runs follows the served bins", func(t *testing.T) {
		malformed := &numerical.Histogram{Values: []int{3, 5}, Counts: []int{10, 60, 30}}
		got := distributionToModel(malformed, nil)
		if got == nil || len(got.Days) != 2 {
			t.Fatalf("distribution = %+v, want the 2 served bins", got)
		}
		if got.Runs != 70 {
			t.Errorf("runs = %d, want 70: the sum of the 2 served counts, not of the 3 stored ones", got.Runs)
		}
	})
}
