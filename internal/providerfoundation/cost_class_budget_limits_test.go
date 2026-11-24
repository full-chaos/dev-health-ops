package providerfoundation

import "testing"

func TestCostClassBudgetLimitIsTheOneTable(t *testing.T) {
	t.Parallel()
	for class, want := range map[string]int{"light": 4, "medium": 2, "heavy": 1, "medium-prefetch": 1} {
		got, ok := CostClassBudgetLimit(class)
		if !ok || got != want {
			t.Fatalf("CostClassBudgetLimit(%q) = %d, %v; want %d, true", class, got, ok, want)
		}
	}
	if limit, ok := CostClassBudgetLimit("standard"); ok || limit != 0 {
		t.Fatalf("unknown class = %d, %v; want 0, false", limit, ok)
	}
	// Only medium has an artifact prefetch sub-budget (CHAOS-14542): heavy and light units never prefetch.
	for _, class := range []string{"light-prefetch", "heavy-prefetch"} {
		if limit, ok := CostClassBudgetLimit(class); ok || limit != 0 {
			t.Fatalf("%s = %d, %v; want 0, false", class, limit, ok)
		}
	}
	if len(costClassBudgetLimits) != 4 {
		t.Fatalf("the table holds %d rows, want 4 (light, medium, heavy, medium-prefetch)", len(costClassBudgetLimits))
	}
}
