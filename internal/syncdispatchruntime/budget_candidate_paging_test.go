package syncdispatchruntime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func candidatePagingSizes() []int {
	return []int{0, unitPageSize, unitPageSize + 1, 10*unitPageSize + 7}
}

// pageStats is what a fake table saw: statements, and the most rows one returned.
type pageStats struct {
	calls   int
	maxRows int
}

// tableLoader serves units, which must be in id order, as keyset pages the way the SQL does.
func tableLoader(units []budgetUnit, stats *pageStats) func(afterID string, limit int) ([]budgetUnit, error) {
	return func(afterID string, limit int) ([]budgetUnit, error) {
		stats.calls++
		var page []budgetUnit
		for _, unit := range units {
			if afterID != "" && unit.id <= afterID {
				continue
			}
			page = append(page, unit)
			if len(page) == limit {
				break
			}
		}
		if len(page) > stats.maxRows {
			stats.maxRows = len(page)
		}
		return page, nil
	}
}

func candidateIDs(n int) []string {
	ids := make([]string, n)
	for index := range ids {
		ids[index] = fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
	}
	return ids
}

func syntheticUnits(n int) []budgetUnit {
	units := make([]budgetUnit, n)
	for index, id := range candidateIDs(n) {
		units[index] = budgetUnit{id: id, orgID: "org-1", syncRunID: "run-1", provider: "github", datasetKey: "commits", costClass: "medium"}
	}
	return units
}

// The candidate pager reads at most a page per statement, sees every candidate once in id order, and an ignored id
// never stops the walk, even when a whole page is ignored.
func TestPageDispatchCandidatesReadsAPageAtATime(t *testing.T) {
	for _, n := range candidatePagingSizes() {
		for _, ignoreFirstPage := range []bool{false, true} {
			t.Run(fmt.Sprintf("N=%d/ignoreFirstPage=%v", n, ignoreFirstPage), func(t *testing.T) {
				units := syntheticUnits(n)
				ignored := map[string]bool{}
				for index, unit := range units {
					if (ignoreFirstPage && index < unitPageSize) || index%7 == 3 {
						ignored[unit.id] = true
					}
				}
				var want []string
				for _, unit := range units {
					if !ignored[unit.id] {
						want = append(want, unit.id)
					}
				}
				var got []string
				maxVisit := 0
				stats := &pageStats{}
				err := pageDispatchCandidates(tableLoader(units, stats), ignored, func(page []budgetUnit) error {
					maxVisit = max(maxVisit, len(page))
					for _, unit := range page {
						got = append(got, unit.id)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("visited %d ids, want the %d unfiltered-minus-ignored ids in order", len(got), len(want))
				}
				if stats.maxRows > unitPageSize || maxVisit > unitPageSize {
					t.Fatalf("a statement returned %d rows and a visit saw %d, more than one page of %d", stats.maxRows, maxVisit, unitPageSize)
				}
				if wantCalls := n/unitPageSize + 1; stats.calls != wantCalls {
					t.Fatalf("statements = %d, want %d", stats.calls, wantCalls)
				}
			})
		}
	}
}

// surplusUnits makes deferred units with ties and missing timestamps, so every clause of the surplus order is used.
func surplusUnits(n int) []budgetUnit {
	units := syntheticUnits(n)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for index := range units {
		switch index % 5 {
		case 0:
		case 1:
			first := base.Add(time.Duration(index%11) * time.Hour)
			units[index].budgetFirstDeferredAt = &first
		default:
			first := base.Add(time.Duration(index%3) * time.Hour)
			blocked := base.Add(time.Duration(index%4) * time.Minute)
			units[index].budgetFirstDeferredAt = &first
			units[index].firstBlockedAt = &blocked
		}
		category := budgetDeferredCategory
		if index%9 == 4 {
			category = "rate_limited"
		}
		units[index].result = map[string]any{"error_category": category}
	}
	return units
}

// The first-16 pick over pages is the first 16 of the full sort the loader used to do, with the same deferred count.
func TestSurplusPickMatchesTheFullSort(t *testing.T) {
	for _, n := range candidatePagingSizes() {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			units := surplusUnits(n)
			ignored := map[string]bool{}
			for index, unit := range units {
				if index%13 == 6 {
					ignored[unit.id] = true
				}
			}
			var deferred []budgetUnit
			for _, unit := range units {
				if !ignored[unit.id] && unit.lastErrorCategory() == budgetDeferredCategory {
					deferred = append(deferred, unit)
				}
			}
			sort.SliceStable(deferred, func(i, j int) bool { return surplusBefore(deferred[i], deferred[j]) })
			considered := budgetSurplusMaxCandidates()
			if len(deferred) > considered {
				deferred = deferred[:considered]
			}
			var want []string
			for _, unit := range deferred {
				want = append(want, unit.id)
			}

			stats := &pageStats{}
			got, err := pickSurplusCandidates(context.Background(), nil, "run-1", tableLoader(units, stats), ignored)
			if err != nil {
				t.Fatal(err)
			}
			var gotIDs []string
			for _, unit := range got {
				gotIDs = append(gotIDs, unit.id)
			}
			if !reflect.DeepEqual(gotIDs, want) {
				t.Fatalf("surplus pick = %v, want the first %d of the full sort %v", gotIDs, considered, want)
			}
			if stats.maxRows > unitPageSize {
				t.Fatalf("a statement returned %d rows, more than one page", stats.maxRows)
			}
		})
	}
}

// The deferred count the truncation log names is every offered unit, however many pages it took.
func TestSurplusPickCountsEveryOffer(t *testing.T) {
	for _, n := range candidatePagingSizes() {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			pick := &surplusPick{limit: 16}
			for _, unit := range surplusUnits(n) {
				pick.offer(unit)
			}
			if pick.offered != n || len(pick.best) > 16 || (n >= 16 && len(pick.best) != 16) {
				t.Fatalf("offered=%d kept=%d, want offered=%d and at most 16 kept", pick.offered, len(pick.best), n)
			}
		})
	}
}

// The invalid-claim fold validates every candidate page by page and hands over only the invalid ones, at most a page
// per visit.
func TestInvalidClaimFoldSeesEveryInvalidCandidate(t *testing.T) {
	for _, n := range candidatePagingSizes() {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			units := syntheticUnits(n)
			var want []string
			for index := range units {
				if index%5 == 2 {
					units[index].datasetKey = "work-item-labels"
					want = append(want, units[index].id)
				}
			}
			var got []string
			maxVisit := 0
			stats := &pageStats{}
			err := pageDispatchCandidates(tableLoader(units, stats), nil, func(page []budgetUnit) error {
				return visitInvalidClaims(page, func(invalid []invalidClaimUnit) error {
					maxVisit = max(maxVisit, len(invalid))
					for _, entry := range invalid {
						got = append(got, entry.unit.id)
					}
					return nil
				})
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid ids = %d, want %d in order", len(got), len(want))
			}
			if maxVisit > unitPageSize {
				t.Fatalf("a visit carried %d invalid units, more than one page", maxVisit)
			}
		})
	}
}

type pagingBudgetEstimator struct{ calls [][]string }

func (fake *pagingBudgetEstimator) DispatchBudgetEstimate(_ context.Context, orgID, _ string, unitIDs []string) (map[string][]budgetEstimate, error) {
	fake.calls = append(fake.calls, append([]string(nil), unitIDs...))
	estimates := map[string][]budgetEstimate{}
	for _, id := range unitIDs {
		bucket := budgetEstimateBucket{Provider: "github", OrgID: orgID, Host: "api.github.com", CredentialFingerprint: "fp-" + id[len(id)-1:], Dimension: "rest_core"}
		estimates[id] = []budgetEstimate{{Bucket: bucket, EstimatedUnits: 3 + int(id[len(id)-1]%4), Confidence: "high", RouteFamily: "work-items"}}
	}
	return estimates, nil
}

// The observe pass over pages emits the same observations, in the same order, as one pass over the whole candidate
// set, because the per-bucket running total is the only thing that carries across pages.
func TestObserveRunPagesMatchesTheUnpagedPass(t *testing.T) {
	t.Setenv("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", "40")
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for _, n := range candidatePagingSizes() {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			units := syntheticUnits(n)

			// The unpaged form: every candidate estimated first, then one loop over all of them.
			reference := &pagingBudgetEstimator{}
			var want []map[string]any
			if len(units) > 0 {
				ids := make([]string, len(units))
				for index, unit := range units {
					ids[index] = unit.id
				}
				estimates := map[string][]budgetEstimate{}
				for _, chunk := range chunkUnitIDs(ids) {
					chunkEstimates, err := estimateChunk(context.Background(), nil, reference, "org-1", "run-1", chunk)
					if err != nil {
						t.Fatal(err)
					}
					for id, perUnit := range chunkEstimates {
						estimates[id] = perUnit
					}
				}
				limits := budgetLimits()
				consumed := map[string]int{}
				for _, unit := range units {
					for _, estimate := range estimates[unit.id] {
						want = append(want, observeEstimate(estimate, unitLogFields("run-1", unit), consumed, limits,
							budgetEnvInt("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", 1_000_000), now, budgetEnvInt("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", 60), true))
					}
				}
			}

			paged := &pagingBudgetEstimator{}
			stats := &pageStats{}
			var got []map[string]any
			err := observeRunPages(context.Background(), nil, paged, nil, "org-1", "run-1", func(visit func([]budgetUnit) error) error {
				return pageDispatchCandidates(tableLoader(units, stats), nil, visit)
			}, now, func(observation map[string]any) { got = append(got, observation) })
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("paged observations differ from the unpaged pass: %d vs %d", len(got), len(want))
			}
			if n > 0 && len(got) == len(want) {
				deferred := 0
				for _, observation := range got {
					if observation["decision"] == "would_defer" {
						deferred++
					}
				}
				if n > unitPageSize && deferred == 0 {
					t.Fatalf("no would_defer decision at N=%d: the running total did not carry across pages", n)
				}
			}
			for _, call := range paged.calls {
				if len(call) > dispatchBudgetEstimateMaxUnitIDs || len(call) > unitPageSize {
					t.Fatalf("an estimate call carried %d ids", len(call))
				}
			}
			if stats.maxRows > unitPageSize {
				t.Fatalf("a statement returned %d rows", stats.maxRows)
			}
		})
	}
}

// Invalid units spread over three pages are terminalized in ONE call after the walk (one global bucket-lock order and
// one run rollup bump), never a call per page; no invalid unit means no call at all.
func TestInvalidClaimsAreTerminalizedOnceAfterTheWalk(t *testing.T) {
	units := syntheticUnits(10*unitPageSize + 7)
	invalidAt := []int{10, unitPageSize + 100, 2*unitPageSize + 250}
	var wantIDs []string
	for _, index := range invalidAt {
		units[index].datasetKey = "work-item-labels"
		wantIDs = append(wantIDs, units[index].id)
	}
	walk := func(units []budgetUnit, pages *int) func(visit func([]invalidClaimUnit) error) error {
		return func(visit func([]invalidClaimUnit) error) error {
			return pageDispatchCandidates(tableLoader(units, &pageStats{}), nil, func(page []budgetUnit) error {
				*pages++
				return visitInvalidClaims(page, visit)
			})
		}
	}

	pages, calls := 0, 0
	var gotIDs []string
	found, terminalized, err := terminalizeInvalidClaims(walk(units, &pages), func(invalid []invalidClaimUnit) (int, error) {
		calls++
		for _, entry := range invalid {
			gotIDs = append(gotIDs, entry.unit.id)
		}
		return len(invalid) - 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pages < 3 {
		t.Fatalf("walk read %d pages, want the invalid units spread over at least 3", pages)
	}
	if calls != 1 {
		t.Fatalf("terminalize called %d times, want once after the walk", calls)
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) || found != len(wantIDs) || terminalized != len(wantIDs)-1 {
		t.Fatalf("ids=%v found=%d terminalized=%d, want %v, %d and %d", gotIDs, found, terminalized, wantIDs, len(wantIDs), len(wantIDs)-1)
	}

	pages, calls = 0, 0
	found, terminalized, err = terminalizeInvalidClaims(walk(syntheticUnits(3*unitPageSize), &pages), func([]invalidClaimUnit) (int, error) {
		calls++
		return 0, nil
	})
	if err != nil || calls != 0 || found != 0 || terminalized != 0 {
		t.Fatalf("no invalid unit: calls=%d found=%d terminalized=%d err=%v, want no call", calls, found, terminalized, err)
	}
}
