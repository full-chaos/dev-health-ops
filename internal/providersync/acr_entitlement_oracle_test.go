package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

// acrEntitlementDecisionForOracle is providersync/incident_entitlement_oracle_test.go's
// twin for the licensing package's agent_context_runtime decision
// (internal/api/licensing). It lives in this package (not
// internal/api/licensing) so it can reuse compareRowsAgainstPythonOracle and
// the shellout-to-Python plumbing this package already built for the SAME
// underlying Python function (feature_policy.decide_feature).
//
// canonicalIncidentFeatureDecision (this package) and licensing.Decision are
// different, independently-defined types on purpose: the two pairs decide
// different feature keys with different EXPLICIT_PURCHASE_FEATURES
// membership, and sharing a decision TYPE across them would invite silently
// reusing one key's oracle cases for the other's tail branch, which is
// exactly the difference this pair exists to prove.
type acrEntitlementDecisionForOracle struct {
	FeatureKey string          `json:"feature_key"`
	Allowed    bool            `json:"allowed"`
	Reason     string          `json:"reason"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Config     *map[string]any `json:"config"`
}

func buildAcrEntitlementDecisionForOracle(
	t *testing.T, input map[string]any,
) acrEntitlementDecisionForOracle {
	t.Helper()
	state := licensing.State{
		Registered: input["registered"].(bool), GloballyEnabled: input["globally_enabled"].(bool),
		MinTier: input["min_tier"].(string), OrgTier: input["org_tier"].(string),
		EvaluatedAt: time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	}
	if raw := input["org_override"]; raw != nil {
		value := raw.(map[string]any)
		state.OrgOverride = &licensing.Override{Enabled: value["enabled"].(bool)}
		if encoded, ok := value["expires_at"].(string); ok && encoded != "" {
			parsed, err := time.Parse(time.RFC3339Nano, encoded)
			if err != nil {
				t.Fatal(err)
			}
			state.OrgOverride.ExpiresAt = &parsed
		}
	}
	if raw := input["license_override"]; raw != nil {
		value := raw.(bool)
		state.LicenseOverride = &value
	}
	decision := licensing.Decide("agent_context_runtime", state)
	return acrEntitlementDecisionForOracle{
		FeatureKey: decision.FeatureKey, Allowed: decision.Allowed, Reason: decision.Reason,
	}
}

func acrEntitlementOracleCases() []oracleCase {
	base := func() map[string]any {
		return map[string]any{
			"registered": true, "globally_enabled": true,
			"min_tier": "community", "org_tier": "community",
			"org_override": nil, "license_override": nil,
		}
	}
	with := func(values map[string]any) map[string]any {
		result := base()
		for key, value := range values {
			result[key] = value
		}
		return result
	}
	return []oracleCase{
		// The money case: agent_context_runtime is an explicit-purchase
		// feature, so registered + globally enabled + tier-eligible, with NO
		// override at all, must still be closed -- unlike
		// canonical_incident_ingestion's identically-shaped "tier_enabled"
		// case in incident_entitlement_oracle_test.go, which is Allowed=true.
		// A variant of licensing.Decide missing the explicit-purchase branch
		// (falling straight to the tier check like the incident feature
		// does) would report Allowed=true here and fail this exact case
		// against the live Python oracle.
		{ID: "no_override_is_explicit_purchase_required", Input: base()},
		{ID: "enterprise_org_tier_still_explicit_purchase_required", Input: with(map[string]any{
			"min_tier": "community", "org_tier": "enterprise",
		})},
		{ID: "global_kill_switch", Input: with(map[string]any{"globally_enabled": false})},
		{ID: "missing_feature", Input: with(map[string]any{"registered": false})},
		{ID: "invalid_min_tier", Input: with(map[string]any{"min_tier": "invalid"})},
		{ID: "org_override_enabled", Input: with(map[string]any{
			"org_override": map[string]any{"enabled": true, "expires_at": nil},
		})},
		{ID: "org_override_disabled", Input: with(map[string]any{
			"org_override": map[string]any{"enabled": false, "expires_at": nil},
		})},
		{ID: "expired_override_falls_back_to_explicit_purchase_required", Input: with(map[string]any{
			"org_override": map[string]any{"enabled": true, "expires_at": "2026-07-23T12:00:00Z"},
		})},
		{ID: "license_enabled", Input: with(map[string]any{"license_override": true})},
		{ID: "license_disabled", Input: with(map[string]any{"license_override": false})},
	}
}

func TestGenericOracleMatchesLivePythonForAcrEntitlement(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "acr/entitlements/agent-context-runtime", acrEntitlementOracleCases(),
		buildAcrEntitlementDecisionForOracle, nil,
	)
}
