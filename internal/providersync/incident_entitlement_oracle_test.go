package providersync

import (
	"testing"
	"time"
)

func buildIncidentEntitlementDecisionForOracle(
	t *testing.T, input map[string]any,
) canonicalIncidentFeatureDecision {
	t.Helper()
	state := canonicalIncidentFeatureState{
		Registered:      input["registered"].(bool),
		GloballyEnabled: input["globally_enabled"].(bool),
		MinTier:         input["min_tier"].(string), OrgTier: input["org_tier"].(string),
		EvaluatedAt: time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	}
	if raw := input["org_override"]; raw != nil {
		value := raw.(map[string]any)
		state.OrgOverride = &canonicalIncidentFeatureOverride{Enabled: value["enabled"].(bool)}
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
	return decideCanonicalIncidentFeature(state)
}

func incidentEntitlementOracleCases() []oracleCase {
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
		{ID: "tier_enabled", Input: base()},
		{ID: "tier_denied", Input: with(map[string]any{"min_tier": "enterprise", "org_tier": "team"})},
		{ID: "global_kill_switch", Input: with(map[string]any{"globally_enabled": false})},
		{ID: "missing_feature", Input: with(map[string]any{"registered": false})},
		{ID: "invalid_min_tier", Input: with(map[string]any{"min_tier": "invalid"})},
		{ID: "org_override_enabled", Input: with(map[string]any{
			"min_tier": "enterprise", "org_override": map[string]any{"enabled": true, "expires_at": nil},
		})},
		{ID: "org_override_disabled", Input: with(map[string]any{
			"org_override": map[string]any{"enabled": false, "expires_at": nil},
		})},
		{ID: "expired_override_falls_back", Input: with(map[string]any{
			"org_override": map[string]any{"enabled": false, "expires_at": "2026-07-23T12:00:00Z"},
		})},
		{ID: "license_enabled", Input: with(map[string]any{
			"min_tier": "enterprise", "license_override": true,
		})},
		{ID: "license_disabled", Input: with(map[string]any{"license_override": false})},
	}
}

func TestGenericOracleMatchesLivePythonForIncidentEntitlement(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "jira/incidents/entitlement", incidentEntitlementOracleCases(),
		buildIncidentEntitlementDecisionForOracle, nil,
	)
}
