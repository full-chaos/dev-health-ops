package remaining

import (
	"os"
	"path/filepath"
	"testing"
)

// TestInventoryIsExactBoundedAndSourceBacked guards family count/source
// existence; per-family routing is asserted by
// TestEveryFamilyHasIndependentRollbackAndReviewedReplay below.
func TestInventoryIsExactBoundedAndSourceBacked(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Families) != len(expectedFamilies) {
		t.Fatalf("families=%d want=%d", len(inventory.Families), len(expectedFamilies))
	}
	root := filepath.Join("..", "..", "..", "..")
	for _, family := range inventory.Families {
		for _, source := range family.PythonSources {
			if _, err := os.Stat(filepath.Join(root, source)); err != nil {
				t.Fatalf("%s source %s: %v", family.Name, source, err)
			}
		}
	}
}

// TestEveryFamilyHasIndependentRollbackAndReviewedReplay guards that every
// family in the reviewed inventory keeps its own, non-shared route key so an
// operator can roll one family back to Celery without touching its siblings.
// Every family is now checked in at go_default/river (route: "river",
// rollback_route: "celery"), so the independent-rollback path is live rather
// than dormant; the per-family rollback route must still be independently
// reachable for each family.
func TestEveryFamilyHasIndependentRollbackAndReviewedReplay(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	routeKeys := make(map[string]string)
	for _, family := range inventory.Families {
		if previous := routeKeys[family.RouteKey]; previous != "" {
			t.Fatalf("%s shares rollback key with %s", family.Name, previous)
		}
		routeKeys[family.RouteKey] = family.Name
		if family.Route != "river" || family.RollbackRoute != "celery" || !family.Executable() {
			t.Fatalf("%s is not independently executable: route=%s rollback=%s", family.Name, family.Route, family.RollbackRoute)
		}
	}
}
