package syncdispatchruntime

import "testing"

// TestConcurrencyCapForCostClassNeverExceedsTheBudgetTable pins CHAOS-7434:
// the admission cap of a bucket is the worker budget's limit for its cost
// class, and SYNC_UNIT_CONCURRENCY_PER_BUCKET is only an upper clamp.
func TestConcurrencyCapForCostClassNeverExceedsTheBudgetTable(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	for class, want := range map[string]int{"heavy": 1, "medium": 2, "light": 4, "standard": 8} {
		if got := concurrencyCapForCostClass(class); got != want {
			t.Fatalf("cap(%q)=%d with clamp 8; want %d", class, got, want)
		}
	}
	// A larger clamp can never raise a class past the table.
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1000")
	if got := concurrencyCapForCostClass("heavy"); got != 1 {
		t.Fatalf("cap(heavy)=%d with clamp 1000; the env var must not exceed the budget table", got)
	}
	// A smaller clamp still lowers it.
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
	for _, class := range []string{"heavy", "medium", "light", "standard"} {
		if got := concurrencyCapForCostClass(class); got != 1 {
			t.Fatalf("cap(%q)=%d with clamp 1; want 1", class, got)
		}
	}
}
