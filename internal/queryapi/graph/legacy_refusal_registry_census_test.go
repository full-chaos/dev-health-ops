package graph

import (
	"slices"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// acp (a), CHAOS-13334: the legacy-refusal registry (pdp.LegacyRefusalRoots) lists, per catalog action, the roots whose
// PDP deny may answer with their own legacy refusal. For each action in it, every root fieldActions maps to that action
// must be in the entry and have a pre-resolver check (fieldPrechecks), and every entry must be such a root. A new root
// on a registry action with no guard fails here; at runtime it keeps the action FORBIDDEN.
func TestLegacyRefusalRegistryCensus(t *testing.T) {
	registry := pdp.LegacyRefusalRoots()
	if len(registry) == 0 {
		t.Fatal("the legacy-refusal registry is empty")
	}
	for action, entry := range registry {
		var mapped []string
		for key, a := range fieldActions {
			if a == action {
				mapped = append(mapped, key)
			}
		}
		sort.Strings(mapped)
		for _, key := range mapped {
			if !slices.Contains(entry, key) {
				t.Errorf("%s maps to registry action %s but is not in its registry entry", key, action)
			}
			if _, has := fieldPrechecks[key]; !has {
				t.Errorf("%s maps to registry action %s but has no pre-resolver check (fieldPrechecks)", key, action)
			}
		}
		for _, key := range entry {
			if fieldActions[key] != action {
				t.Errorf("registry entry %s of %s: fieldActions maps it to %q", key, action, fieldActions[key])
			}
			if _, has := fieldPrechecks[key]; !has {
				t.Errorf("registry entry %s of %s has no pre-resolver check (fieldPrechecks)", key, action)
			}
		}
	}
}
