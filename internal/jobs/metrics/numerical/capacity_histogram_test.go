package numerical

import (
	"reflect"
	"sort"
	"testing"
)

func TestHistogramIsAnExactCountOfTheSamples(t *testing.T) {
	samples := []int{5, 3, 5, 9, 3, 3, 5, 5, 0}
	histogram := NewHistogram(samples)

	if want := []int{0, 3, 5, 9}; !reflect.DeepEqual(histogram.Values, want) {
		t.Errorf("values = %v, want %v (ascending, distinct)", histogram.Values, want)
	}
	if want := []int{1, 3, 4, 1}; !reflect.DeepEqual(histogram.Counts, want) {
		t.Errorf("counts = %v, want %v", histogram.Counts, want)
	}
	if histogram.Total() != len(samples) {
		t.Errorf("total = %d, want %d", histogram.Total(), len(samples))
	}
	sorted := append([]int(nil), samples...)
	sort.Ints(sorted)
	if got := histogram.Expand(); !reflect.DeepEqual(got, sorted) {
		t.Errorf("Expand() = %v, want the sorted samples %v", got, sorted)
	}
	if samples[0] != 5 || samples[8] != 0 {
		t.Errorf("NewHistogram modified its input: %v", samples)
	}
}

func TestHistogramOfNothingIsEmptyNotZero(t *testing.T) {
	histogram := NewHistogram(nil)
	if len(histogram.Values) != 0 || len(histogram.Counts) != 0 || histogram.Total() != 0 {
		t.Errorf("empty input gave %+v, want no bins", histogram)
	}
}

// CHAOS-7624: the persisted distribution is only worth having if the
// percentiles can be recomputed from it. This runs the whole kernel against
// every vector captured from the REAL Python producer (the same golden the
// percentile parity test uses) and requires the percentiles taken from the
// histogram to equal the ones Python recorded. A histogram built from the wrong
// samples, or one that lost a run, moves a percentile on some case here.
//
// The recorded vectors hold percentiles, not Python's sample lists, so this is
// a differential check of the distribution's quantiles against Python, not a
// bin-for-bin one. A bin-for-bin oracle needs the generator extended to emit
// its sample lists and its rot-guard recording redone, which is separate work.
func TestCapacityDistributionPercentilesMatchPython(t *testing.T) {
	golden := loadCapacityGolden(t)

	var daysCases, itemsCases int
	for _, test := range golden.Cases {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			request := ForecastRequest{
				History: Throughput{
					DailyThroughputs: test.Throughputs,
					DaysOfHistory:    len(test.Throughputs),
				},
				TargetItems: test.TargetItems,
				Simulations: test.Simulations,
				Seed:        test.Seed,
			}
			if test.TargetDate != nil {
				target := parseDay(t, *test.TargetDate)
				request.TargetDate = &target
			}
			got, err := ForecastCapacity(request, parseDay(t, test.Today))
			if err != nil {
				t.Fatalf("forecast: %v", err)
			}

			if test.TargetItems == nil {
				if got.DaysHistogram != nil {
					t.Errorf("days histogram = %+v, want nil: no fixed-scope run", got.DaysHistogram)
				}
			} else {
				daysCases++
				if got.DaysHistogram == nil {
					t.Fatal("days histogram is nil but the fixed-scope mode ran")
				}
				if total := got.DaysHistogram.Total(); total != test.Simulations {
					t.Errorf("days histogram holds %d runs, want %d", total, test.Simulations)
				}
				q := IntegerPercentiles(got.DaysHistogram.Expand(), []float64{50, 85, 95})
				assertIntPointer(t, "p50_days from histogram", &q[0], test.Expected.P50Days)
				assertIntPointer(t, "p85_days from histogram", &q[1], test.Expected.P85Days)
				assertIntPointer(t, "p95_days from histogram", &q[2], test.Expected.P95Days)
			}

			if got.ItemsHistogram == nil {
				return
			}
			itemsCases++
			if total := got.ItemsHistogram.Total(); total != test.Simulations {
				t.Errorf("items histogram holds %d runs, want %d", total, test.Simulations)
			}
			// The items percentiles are the flipped [50, 15, 5].
			q := IntegerPercentiles(got.ItemsHistogram.Expand(), []float64{50, 15, 5})
			assertIntPointer(t, "p50_items from histogram", &q[0], test.Expected.P50Items)
			assertIntPointer(t, "p85_items from histogram", &q[1], test.Expected.P85Items)
			assertIntPointer(t, "p95_items from histogram", &q[2], test.Expected.P95Items)
		})
	}
	// Anti-vacuity: a golden with no case of a mode would pass for a producer
	// that never builds that histogram.
	if daysCases == 0 || itemsCases == 0 {
		t.Errorf("golden covers %d fixed-scope and %d fixed-date histogram cases, want both > 0", daysCases, itemsCases)
	}
}

func TestForecastCapacityHasNoDistributionForAModeThatDidNotRun(t *testing.T) {
	items := 40
	got, err := ForecastCapacity(ForecastRequest{
		History:     Throughput{DailyThroughputs: []int{3, 8, 1, 5, 13, 2, 9}, DaysOfHistory: 7},
		TargetItems: &items,
		Simulations: 200,
		Seed:        1,
	}, parseDay(t, "2026-08-23"))
	if err != nil {
		t.Fatal(err)
	}
	if got.DaysHistogram == nil {
		t.Error("days histogram is nil for a fixed-scope forecast")
	}
	if got.ItemsHistogram != nil {
		t.Errorf("items histogram = %+v, want nil without a target date", got.ItemsHistogram)
	}

	// A target date that is not in the future runs no simulation: the items
	// percentiles are the documented zeros and there is no distribution.
	past := parseDay(t, "2026-08-23")
	got, err = ForecastCapacity(ForecastRequest{
		History:     Throughput{DailyThroughputs: []int{3, 8, 1, 5, 13, 2, 9}, DaysOfHistory: 7},
		TargetDate:  &past,
		Simulations: 200,
		Seed:        1,
	}, parseDay(t, "2026-08-23"))
	if err != nil {
		t.Fatal(err)
	}
	if got.ItemsHistogram != nil {
		t.Errorf("items histogram = %+v, want nil when no days are available", got.ItemsHistogram)
	}
}
