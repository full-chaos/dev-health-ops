package remaining

import (
	"errors"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// CHAOS-7624 / migration 101: the writer persists the Monte Carlo histogram as
// four parallel arrays. The reader rebuilds a Histogram from them, so the pair
// must round-trip exactly.

func TestNarrowCapacityRowCarriesTheDistribution(t *testing.T) {
	days := numerical.NewHistogram([]int{19, 24, 19, 19, 27})
	items := numerical.NewHistogram([]int{40, 55, 55})
	row := capacityRow{
		BacklogSize: 10,
		Forecast: numerical.ForecastResult{
			HistoryDays: 30, SimulationCount: 5,
			DaysHistogram: &days, ItemsHistogram: &items,
		},
	}

	narrowed, err := narrowCapacityRow(row)
	if err != nil {
		t.Fatalf("narrowCapacityRow: %v", err)
	}

	if want := []uint16{19, 24, 27}; !reflect.DeepEqual(narrowed.daysValues, want) {
		t.Errorf("daysValues = %v, want %v", narrowed.daysValues, want)
	}
	if want := []uint32{3, 1, 1}; !reflect.DeepEqual(narrowed.daysCounts, want) {
		t.Errorf("daysCounts = %v, want %v", narrowed.daysCounts, want)
	}
	if want := []uint32{40, 55}; !reflect.DeepEqual(narrowed.itemsValues, want) {
		t.Errorf("itemsValues = %v, want %v", narrowed.itemsValues, want)
	}
	if want := []uint32{1, 2}; !reflect.DeepEqual(narrowed.itemsCounts, want) {
		t.Errorf("itemsCounts = %v, want %v", narrowed.itemsCounts, want)
	}

	// Rebuilt as the reader rebuilds it, the original samples come back.
	rebuilt := numerical.Histogram{}
	for i, value := range narrowed.daysValues {
		rebuilt.Values = append(rebuilt.Values, int(value))
		rebuilt.Counts = append(rebuilt.Counts, int(narrowed.daysCounts[i]))
	}
	if !reflect.DeepEqual(rebuilt, days) {
		t.Errorf("round trip = %+v, want %+v", rebuilt, days)
	}
}

// A mode that did not simulate is a non-nil EMPTY pair: that is what the column
// stores for "no distribution", and a nil slice would bind as NULL in a column
// that is not Nullable.
func TestNarrowCapacityRowWritesEmptyArraysForAModeThatDidNotRun(t *testing.T) {
	narrowed, err := narrowCapacityRow(capacityRow{
		BacklogSize: 10,
		Forecast:    numerical.ForecastResult{HistoryDays: 30, SimulationCount: 5},
	})
	if err != nil {
		t.Fatalf("narrowCapacityRow: %v", err)
	}
	if narrowed.daysValues == nil || narrowed.daysCounts == nil ||
		narrowed.itemsValues == nil || narrowed.itemsCounts == nil {
		t.Fatalf("a nil slice would bind as NULL: %+v", narrowed)
	}
	if len(narrowed.daysValues)+len(narrowed.daysCounts)+len(narrowed.itemsValues)+len(narrowed.itemsCounts) != 0 {
		t.Errorf("want four empty arrays, got %+v", narrowed)
	}
}

// Fail closed, like every other narrowing in this file: a completion-day count
// beyond UInt16 is refused, not wrapped into a plausible smaller day.
func TestNarrowCapacityRowRefusesAValueThatDoesNotFitItsColumn(t *testing.T) {
	tooManyDays := numerical.NewHistogram([]int{70000})
	_, err := narrowCapacityRow(capacityRow{
		Forecast: numerical.ForecastResult{DaysHistogram: &tooManyDays},
	})
	if !errors.Is(err, ErrCapacityValueOutOfRange) {
		t.Errorf("days value 70000: err = %v, want ErrCapacityValueOutOfRange", err)
	}

	negative := numerical.Histogram{Values: []int{-1}, Counts: []int{1}}
	_, err = narrowCapacityRow(capacityRow{
		Forecast: numerical.ForecastResult{ItemsHistogram: &negative},
	})
	if !errors.Is(err, ErrCapacityValueOutOfRange) {
		t.Errorf("negative items value: err = %v, want ErrCapacityValueOutOfRange", err)
	}
}

// The executor refuses a database that has not applied migration 101, rather
// than failing every insert.
func TestCapacityRequirementsNameTheDistributionColumns(t *testing.T) {
	have := map[string]bool{}
	for _, column := range capacityTableRequirements["capacity_forecasts"].columns {
		have[column] = true
	}
	for _, column := range []string{
		"completion_days_values", "completion_days_counts",
		"completion_items_values", "completion_items_counts",
	} {
		if !have[column] {
			t.Errorf("capacity_forecasts requirement does not name %s", column)
		}
	}
}
