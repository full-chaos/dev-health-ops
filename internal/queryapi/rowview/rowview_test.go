package rowview

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

func TestPredicatesKeepLegacyRowsAndTheirOwnView(t *testing.T) {
	if got, want := OrgPredicate("work_item_metrics_daily", ""), "(work_item_metrics_daily.row_view IN ('', 'org'))"; got != want {
		t.Errorf("OrgPredicate = %s, want %s", got, want)
	}
	if got, want := ScopePredicate("work_item_user_metrics_daily", "m"), "(m.row_view IN ('', 'scope'))"; got != want {
		t.Errorf("ScopePredicate = %s, want %s", got, want)
	}
}

func TestEveryTableOfThePackageHoldsTheViewColumnAsTheLastKey(t *testing.T) {
	for _, name := range Tables() {
		declared, ok := teamkeytables.ByTable(name)
		if !ok {
			t.Fatalf("table %s is not declared in teamkeytables", name)
		}
		last := declared.Keys[len(declared.Keys)-1]
		if last.Name != Column || last.Kind != teamkeytables.KeyString {
			t.Errorf("table %s: last key column is %+v, want %s as a string", name, last, Column)
		}
	}
	for _, other := range teamkeytables.All() {
		for _, key := range other.Keys {
			if key.Name != Column {
				continue
			}
			if _, ok := tables[other.Table]; !ok {
				t.Errorf("table %s holds %s but is not in the package", other.Table, Column)
			}
		}
	}
}

func TestAPredicateOfATableWithoutTheColumnPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("OrgPredicate of a table with no view column did not panic")
		}
	}()
	_ = OrgPredicate("team_metrics_daily", "")
}
