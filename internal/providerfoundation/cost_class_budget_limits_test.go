package providerfoundation

import "testing"

func TestCostClassBudgetLimitIsTheOneTable(t *testing.T) {
	t.Parallel()
	for class, want := range map[string]int{"light": 4, "medium": 2, "heavy": 1} {
		got, ok := CostClassBudgetLimit(class)
		if !ok || got != want {
			t.Fatalf("CostClassBudgetLimit(%q) = %d, %v; want %d, true", class, got, ok, want)
		}
	}
	if limit, ok := CostClassBudgetLimit("standard"); ok || limit != 0 {
		t.Fatalf("unknown class = %d, %v; want 0, false", limit, ok)
	}
}
