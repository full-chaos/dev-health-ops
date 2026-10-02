package licensing

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const pythonRegistryProgram = `
import json
from dev_health_ops.licensing.registry import get_features_for_tier, EXPLICIT_PURCHASE_FEATURES, CANONICAL_INCIDENT_INGESTION_FEATURE
from dev_health_ops.licensing.types import DEFAULT_LIMITS, LicenseTier
out = {"features": {}, "limits": {}}
for tier in LicenseTier:
    out["features"][tier.value] = [[k, v] for k, v in get_features_for_tier(tier).items()]
    limits = DEFAULT_LIMITS[tier]
    out["limits"][tier.value] = [limits.users, limits.repos, limits.api_rate]
out["decision_keys"] = sorted(EXPLICIT_PURCHASE_FEATURES | {CANONICAL_INCIDENT_INGESTION_FEATURE, "customer_push_ingest"})
print(json.dumps(out))
`

// TestTierFeaturesMatchFrozenPython holds the Go registry, limits and
// decision keys to the frozen Python ones, value and order.
func TestTierFeaturesMatchFrozenPython(t *testing.T) {
	output := frozenPython(t, "tier-registry.golden.json", programoracle.Program{Name: "tier registry", Text: pythonRegistryProgram})[0]
	var want struct {
		Features     map[string][][2]any
		Limits       map[string][3]int64
		DecisionKeys []string `json:"decision_keys"`
	}
	if err := json.Unmarshal([]byte(output), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want.Features) != len(TierOrder) {
		t.Fatalf("python tiers = %d, go %d", len(want.Features), len(TierOrder))
	}
	for _, tier := range TierOrder {
		got := FeaturesForTier(tier)
		if len(got) != len(want.Features[tier]) {
			t.Fatalf("%s: %d features, python %d", tier, len(got), len(want.Features[tier]))
		}
		for index, feature := range got {
			if feature.Key != want.Features[tier][index][0] || feature.Enabled != want.Features[tier][index][1] {
				t.Errorf("%s #%d: go %v, python %v", tier, index, feature, want.Features[tier][index])
			}
		}
		limits, _ := DefaultLimits(tier)
		if [3]int64{limits.Users, limits.Repos, limits.APIRate} != want.Limits[tier] {
			t.Errorf("%s limits: go %v, python %v", tier, limits, want.Limits[tier])
		}
	}
	if strings.Join(EntitlementDecisionKeys(), ",") != strings.Join(want.DecisionKeys, ",") {
		t.Errorf("decision keys: go %v, python %v", EntitlementDecisionKeys(), want.DecisionKeys)
	}
}
