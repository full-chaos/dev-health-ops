package routeswitch

import "testing"

func TestCatalogSwitchServesExactlyTheRegisteredOperations(t *testing.T) {
	sw := NewCatalogSwitch(map[string]string{"hotspots": "sha256:doc-a", "cognitiveLoad": "sha256:doc-b"})
	for operation, want := range map[string]bool{"hotspots": true, "cognitiveLoad": true, "neverRegistered": false, "mcp:hotspots": false, "": false} {
		if got := sw.Enabled(operation); got != want {
			t.Errorf("Enabled(%q) = %t, want %t", operation, got, want)
		}
	}
}

func TestCatalogSwitchReadsNoInventoryAfterConstruction(t *testing.T) {
	inventory := map[string]string{"hotspots": "sha256:doc-a"}
	sw := NewCatalogSwitch(inventory)
	delete(inventory, "hotspots")
	inventory["late"] = "sha256:doc-c"
	if !sw.Enabled("hotspots") || sw.Enabled("late") {
		t.Fatalf("the switch followed the caller's map: hotspots=%t late=%t, want true false", sw.Enabled("hotspots"), sw.Enabled("late"))
	}
}
