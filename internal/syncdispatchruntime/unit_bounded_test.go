package syncdispatchruntime

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func boundedSizes() []int {
	return []int{0, 1, unitPageSize - 1, unitPageSize, unitPageSize + 1, 10*unitPageSize + 7}
}

func TestForEachUnitPageNeverReadsMoreThanOnePageAndVisitsEveryRowOnce(t *testing.T) {
	for _, n := range boundedSizes() {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("%08d", i)
		}
		maxLimit, maxRows, calls := 0, 0, 0
		var seen []string
		err := forEachUnitPage(unitPageSize,
			func(after string, limit int) ([]string, error) {
				calls++
				if limit > maxLimit {
					maxLimit = limit
				}
				start := sort.SearchStrings(ids, after)
				if after != "" && start < len(ids) && ids[start] == after {
					start++
				}
				end := min(start+limit, len(ids))
				page := ids[start:end]
				maxRows = max(maxRows, len(page))
				return page, nil
			},
			func(id string) string { return id },
			func(page []string) error { seen = append(seen, page...); return nil })
		if err != nil {
			t.Fatal(err)
		}
		if maxLimit > unitPageSize || maxRows > unitPageSize {
			t.Fatalf("N=%d: limit %d rows %d, bound %d", n, maxLimit, maxRows, unitPageSize)
		}
		if n == 0 {
			if len(seen) != 0 || calls != 1 {
				t.Fatalf("N=0: seen %d calls %d", len(seen), calls)
			}
			continue
		}
		if !reflect.DeepEqual(seen, ids) {
			t.Fatalf("N=%d: visited %d rows, not the %d in id order", n, len(seen), n)
		}
		if wantCalls := n/unitPageSize + 1; calls != wantCalls {
			t.Fatalf("N=%d: %d queries, want %d", n, calls, wantCalls)
		}
	}
}

// aggregateFinalizeUnitsOracle is the slice form this file's fold replaced, kept as the reference.
func aggregateFinalizeUnitsOracle(units []finalizeSyncRunUnit) finalizeAggregate {
	var aggregate finalizeAggregate
	for _, unit := range units {
		switch unit.status {
		case syncRunUnitStatusSuccess:
			aggregate.successCount++
		case syncRunUnitStatusFailed:
			aggregate.failedCount++
			if aggregate.errorCategory == "" && unit.errorCategory != "" {
				aggregate.errorCategory = unit.errorCategory
			}
			if aggregate.reason == "" && unit.reason != "" {
				aggregate.reason = unit.reason
			}
			if aggregate.firstFailedUnitError == nil && unit.errorText != nil && *unit.errorText != "" {
				text := *unit.errorText
				aggregate.firstFailedUnitError = &text
			}
		}
	}
	return aggregate
}

func TestFinalizeFoldEqualsTheSliceAggregate(t *testing.T) {
	text := func(s string) *string { return &s }
	for _, n := range boundedSizes() {
		// failures whose first category, first reason and first error text sit on different units, deep in the set
		units := make([]finalizeSyncRunUnit, n)
		for i := range units {
			units[i] = finalizeSyncRunUnit{id: fmt.Sprintf("%08d", i), status: syncRunUnitStatusSuccess}
			if i%3 == 1 {
				units[i].status = syncRunUnitStatusFailed
			}
			if i >= 2*unitPageSize+3 && i%3 == 1 {
				units[i].errorText = text(fmt.Sprintf("synthetic failure %d", i))
			}
			if i >= unitPageSize+5 && i%3 == 1 {
				units[i].reason = fmt.Sprintf("synthetic_reason_%d", i)
			}
			if i >= 3*unitPageSize+1 && i%3 == 1 {
				units[i].errorCategory = fmt.Sprintf("synthetic_category_%d", i)
			}
		}
		var fold finalizeFold
		for _, unit := range units {
			fold.add(unit)
		}
		if want := aggregateFinalizeUnitsOracle(units); !reflect.DeepEqual(fold.aggregate, want) {
			t.Fatalf("N=%d: fold %+v, slice %+v", n, fold.aggregate, want)
		}
		if got := aggregateFinalizeUnits(units); !reflect.DeepEqual(got, aggregateFinalizeUnitsOracle(units)) {
			t.Fatalf("N=%d: aggregateFinalizeUnits drifted from the oracle", n)
		}
	}
}

func workItemUnitsWindowDaysOracle(units []workItemUnitWindow, runStartedAt, now time.Time) ([]time.Time, error) {
	if len(units) == 0 {
		return nil, nil
	}
	distinct := map[time.Time]struct{}{}
	for _, unit := range units {
		days, err := providersync.WorkItemsUnitWindowDays(unit.since, unit.before, runStartedAt)
		if err != nil {
			return nil, err
		}
		for _, day := range days {
			distinct[day] = struct{}{}
		}
		if unit.before == nil {
			for day := utcDay(runStartedAt); !day.After(utcDay(now)); day = day.AddDate(0, 0, 1) {
				distinct[day] = struct{}{}
			}
		}
	}
	out := make([]time.Time, 0, len(distinct))
	for day := range distinct {
		out = append(out, day)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out, nil
}

func TestWorkItemDayAccumulatorHoldsDaysNotWindows(t *testing.T) {
	runStartedAt := time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	for _, n := range boundedSizes() {
		windows := make([]workItemUnitWindow, n)
		accumulator := newWorkItemDayAccumulator(runStartedAt, now)
		for i := range windows {
			since := time.Date(2026, 9, 1+i%10, 0, 0, 0, 0, time.UTC)
			windows[i] = workItemUnitWindow{since: &since}
			if i%2 == 0 {
				before := since.Add(48 * time.Hour)
				windows[i].before = &before
			}
			accumulator.add(windows[i].since, windows[i].before)
		}
		if len(accumulator.distinct) > 40 {
			t.Fatalf("N=%d: %d distinct days kept, want a handful, never one per window", n, len(accumulator.distinct))
		}
		got, err := accumulator.days()
		if err != nil {
			t.Fatal(err)
		}
		want, err := workItemUnitsWindowDaysOracle(windows, runStartedAt, now)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("N=%d: accumulator days differ from the per-window union", n)
		}
	}
}
