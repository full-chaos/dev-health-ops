package contracts

import (
	"os"
	"strings"
	"testing"
)

func catalogRows(t *testing.T) map[string]bool {
	t.Helper()
	rows := map[string]bool{}
	for _, raw := range readJSONFile(t, "contracts/auth/v1/vocabulary/actions.v1.json")["actions"].([]any) {
		rows[raw.(map[string]any)["action"].(string)] = true
	}
	return rows
}

// Every action a valid endpoint profile names is a catalog row, cached or not.
func TestEveryActionOfAValidProfileIsCatalogued(t *testing.T) {
	rows := catalogRows(t)
	for _, entry := range loadEndpointAuthProfileManifest(t).Accept {
		profile := loadEndpointAuthProfileFixture(t, entry.File).(map[string]any)
		action, _ := profile["action"].(string)
		if action == "" {
			continue
		}
		if !rows[action] {
			t.Errorf("valid example %s names action %s, which is not in actions.v1", entry.File, action)
		}
	}
}

// Dropping any one catalog row makes the gate refuse a cached profile that names it.
func TestRemovedCatalogRowRefusesACachedProfile(t *testing.T) {
	full := catalogActions()
	orig := catalogActions
	t.Cleanup(func() { catalogActions = orig })
	for action, entry := range full {
		if entry.cacheClass == "none" {
			continue
		}
		without := map[string]catalogEntry{}
		for k, v := range full {
			if k != action {
				without[k] = v
			}
		}
		catalogActions = func() map[string]catalogEntry { return without }
		if err := CheckActionCachePolicy(map[string]any{"action": action, "cache_class": entry.cacheClass}); err == nil {
			t.Errorf("cached profile for %s passed with its catalog row removed", action)
		}
	}
}

// The gate has no exemption path: no list of actions that skip the catalog.
func TestGateHasNoExemptionPath(t *testing.T) {
	src, err := os.ReadFile("endpoint_auth_profile_selector_gate.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "uncataloguedCachedActions") {
		t.Error("endpoint_auth_profile_selector_gate.go still holds the cached-action exemption list")
	}
}
