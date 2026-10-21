package orgs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

func TestRegistryMatchesFrozenPython(t *testing.T) {
	output := frozenPython(t, "registry.golden.json", programoracle.Program{Name: "licensing registry"})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var result struct {
		Standard  []string            `json:"standard"`
		TierOrder []string            `json:"tier_order"`
		Limits    map[string][][3]any `json:"limits"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	if strings.Join(result.Standard, ",") != strings.Join(StandardFeatureKeys, ",") {
		t.Fatalf("standard features: Python %v, Go %v", result.Standard, StandardFeatureKeys)
	}
	if strings.Join(result.TierOrder, ",") != strings.Join(TierOrder, ",") {
		t.Fatalf("tier order: Python %v, Go %v", result.TierOrder, TierOrder)
	}
	if len(result.Limits) != len(TierLimits) {
		t.Fatalf("limits tiers: Python %d, Go %d", len(result.Limits), len(TierLimits))
	}
	for tier, limits := range result.Limits {
		goLimits := TierLimits[tier]
		if len(goLimits) != len(limits) {
			t.Fatalf("%s: Python %d limits, Go %d", tier, len(limits), len(goLimits))
		}
		for index, row := range limits {
			goKind := "NoneType"
			var goNumber any
			switch value := goLimits[index].Value.(type) {
			case int64:
				goKind, goNumber = "int", float64(value)
			case float64:
				goKind, goNumber = "float", value
			}
			if goLimits[index].Key != row[0] || goNumber != row[1] || goKind != row[2] {
				t.Errorf("%s[%d]: Go %s=%v (%s), Python %v=%v (%v)", tier, index, goLimits[index].Key, goNumber, goKind, row[0], row[1], row[2])
			}
		}
	}
}
