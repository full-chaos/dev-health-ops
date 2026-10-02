package providersync

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestBudgetLimitsByCostClassIsTheOneTable pins CHAOS-7807: the worker's budget
// map is exactly the table the dispatch admission cap reads, for the three
// cost classes and nothing else. A missing heavy entry makes the executor
// refuse every heavy write route (ErrInvalidConfiguration); an entry that
// drifts from the table lets admission and the budget disagree again.
func TestBudgetLimitsByCostClassIsTheOneTable(t *testing.T) {
	t.Parallel()
	limits := BudgetLimitsByCostClass()
	if len(limits) != 3 {
		t.Fatalf("BudgetLimitsByCostClass() has %d classes %v; want exactly light, medium, heavy", len(limits), limits)
	}
	for _, class := range []CostClass{CostLight, CostMedium, CostHeavy} {
		want, ok := providerfoundation.CostClassBudgetLimit(string(class))
		if !ok {
			t.Fatalf("class %q is missing from the table", class)
		}
		if got, present := limits[class]; !present || got != want {
			t.Fatalf("limits[%q] = %d, present=%v; want %d (the table value)", class, got, present, want)
		}
	}
	// A caller may keep and change its map: it must not alias the next call's.
	limits[CostHeavy] = 99
	if again := BudgetLimitsByCostClass()[CostHeavy]; again == 99 {
		t.Fatal("BudgetLimitsByCostClass returned a shared map")
	}
}
