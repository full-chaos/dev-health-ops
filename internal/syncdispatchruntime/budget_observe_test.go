package syncdispatchruntime

import (
	"testing"
	"time"
)

func TestObserveEstimateAllowsWithinLimit(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
	estimate := budgetEstimate{Bucket: bucket, RouteFamily: "work-items", EstimatedUnits: 10, Confidence: "high"}
	consumed := map[string]int{}
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	got := observeEstimate(estimate, unitLogFields("run-1", budgetUnit{id: "unit-1"}), consumed, map[string]int{}, 100, observedAt, 60, true)

	if got["decision"] != "would_allow" {
		t.Fatalf("decision=%v want=would_allow", got["decision"])
	}
	if got["suggested_available_at"] != nil {
		t.Fatalf("suggested_available_at=%v want=nil (not deferring)", got["suggested_available_at"])
	}
	if got["projected_units"] != 10 {
		t.Fatalf("projected_units=%v want=10", got["projected_units"])
	}
	budgetKey := budgetKeyFor(bucket, "work-items")
	if consumed[budgetKey] != 10 {
		t.Fatalf("consumed[%s]=%d want=10 (recordConsumption=true must charge the bucket)", budgetKey, consumed[budgetKey])
	}
}

// TestObserveEstimateAllowsExactlyAtTheLimit is the discriminating boundary
// case: Python's predicate is projected_units > limit (strict), so landing
// EXACTLY on the limit must still allow -- a > -> >= mutation passes
// vacuously against any fixture that only tests values strictly above or
// below the limit.
func TestObserveEstimateAllowsExactlyAtTheLimit(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
	estimate := budgetEstimate{Bucket: bucket, RouteFamily: "work-items", EstimatedUnits: 40}
	consumed := map[string]int{budgetKeyFor(bucket, "work-items"): 60}

	got := observeEstimate(estimate, unitLogFields("run-1", budgetUnit{id: "unit-1"}), consumed, map[string]int{}, 100, time.Now(), 60, true)

	if got["decision"] != "would_allow" {
		t.Fatalf("decision=%v want=would_allow -- 60+40=100 == limit, not over it", got["decision"])
	}
}

func TestObserveEstimateDefersOverLimitAndSuggestsATime(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
	estimate := budgetEstimate{Bucket: bucket, RouteFamily: "work-items", EstimatedUnits: 60}
	consumed := map[string]int{budgetKeyFor(bucket, "work-items"): 50}
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	got := observeEstimate(estimate, unitLogFields("run-1", budgetUnit{id: "unit-1"}), consumed, map[string]int{}, 100, observedAt, 60, true)

	if got["decision"] != "would_defer" {
		t.Fatalf("decision=%v want=would_defer (50+60=110 > 100)", got["decision"])
	}
	want := observedAt.Add(60 * time.Second).Format(time.RFC3339Nano)
	if got["suggested_available_at"] != want {
		t.Fatalf("suggested_available_at=%v want=%s", got["suggested_available_at"], want)
	}
}

// TestObserveEstimateDryRunDoesNotChargeTheBucket pins observe_run's whole
// reason for existing as a SEPARATE call path from the real admission loop:
// recordConsumption=false must never mutate consumedByBucket, or a dry-run
// telemetry pass would corrupt the real loop's running totals.
func TestObserveEstimateDryRunDoesNotChargeTheBucket(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
	estimate := budgetEstimate{Bucket: bucket, RouteFamily: "work-items", EstimatedUnits: 10}
	consumed := map[string]int{}
	observedAt := time.Now()

	observeEstimate(estimate, unitLogFields("run-1", budgetUnit{id: "unit-1"}), consumed, map[string]int{}, 100, observedAt, 60, false)

	budgetKey := budgetKeyFor(bucket, "work-items")
	if consumed[budgetKey] != 0 {
		t.Fatalf("consumed[%s]=%d want=0 -- dry-run must not charge the bucket", budgetKey, consumed[budgetKey])
	}
}

func TestObserveEstimateMissingBucketReadsAsZeroConsumption(t *testing.T) {
	// consumedByBucket has no entry for this bucket at all -- Python's
	// defaultdict(int) reads that as 0; a Go map lookup on a missing key
	// already returns the zero value, so this is the same behavior with no
	// special-casing required. Pinned explicitly since a hand-rolled port
	// using a "does the key exist" check would diverge here.
	bucket := budgetEstimateBucket{Provider: "gitlab", Dimension: "graphql"}
	estimate := budgetEstimate{Bucket: bucket, RouteFamily: "commits", EstimatedUnits: 5}
	consumed := map[string]int{}

	got := observeEstimate(estimate, unitLogFields("run-1", budgetUnit{id: "unit-1"}), consumed, map[string]int{}, 100, time.Now(), 60, true)
	if got["projected_units"] != 5 {
		t.Fatalf("projected_units=%v want=5 (0 prior + 5 new)", got["projected_units"])
	}
}

func TestUnitLogFieldsCarriesTheIdentifyingColumns(t *testing.T) {
	unit := budgetUnit{id: "unit-1", sourceID: "source-1", datasetKey: "commits", provider: "github", costClass: "rest_core"}
	got := unitLogFields("run-1", unit)
	want := map[string]any{
		"sync_run_id": "run-1", "unit_id": "unit-1", "source_id": "source-1",
		"dataset_key": "commits", "provider": "github", "cost_class": "rest_core",
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("got[%q]=%v want=%v", key, got[key], value)
		}
	}
}

// TestMergeFieldsDoesNotMutateTheBase pins the reason mergeFields exists at
// all instead of writing extra keys directly into a shared logFields map:
// observeEstimate/admitSurplusRetries call this once per estimate inside a
// loop, and every call must get its own independent map.
func TestMergeFieldsDoesNotMutateTheBase(t *testing.T) {
	base := map[string]any{"a": 1}
	merged1 := mergeFields(base, map[string]any{"b": 2})
	merged2 := mergeFields(base, map[string]any{"b": 3})
	if merged1["b"] != 2 || merged2["b"] != 3 {
		t.Fatalf("merged1[b]=%v merged2[b]=%v -- each merge must be independent", merged1["b"], merged2["b"])
	}
	if _, ok := base["b"]; ok {
		t.Fatal("base map must not be mutated by mergeFields")
	}
}
