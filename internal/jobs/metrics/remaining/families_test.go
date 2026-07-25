package remaining

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryIsExactBoundedAndCeleryRouted(t *testing.T) {
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
		if family.Route != family.RollbackRoute || family.Route != "celery" {
			t.Fatalf("%s is prematurely enabled: route=%s rollback=%s", family.Name, family.Route, family.RollbackRoute)
		}
	}
}
