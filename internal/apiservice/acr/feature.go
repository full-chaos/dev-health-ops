// Package acr serves the two internal routes acr's entitlement client calls
// directly (bypassing ingress, CHAOS-6266): GET /api/v1/internal/acr/health
// and GET /api/v1/internal/acr/entitlements/{org_id}. Under R340 the route
// carries no bearer/mint check -- the Python body's credential lookup and
// audit trail (api/internal/acr.py:48-113) are call-site auth, not part of
// the entitlement decision itself, and are not ported.
package acr

import "time"

// DecideAgentContextRuntimeFeatureForOracle exposes
// decideAgentContextRuntimeFeature through primitive types, as this
// package's only test-facing seam: a cross-package live-Python oracle test
// (internal/providersync/acr_entitlement_oracle_test.go, reusing that
// package's existing oracle harness rather than duplicating it) needs to
// call the real decision function, but the internal featureState/
// featureDecision shapes stay unexported production API surface.
func DecideAgentContextRuntimeFeatureForOracle(
	registered, globallyEnabled bool,
	minTier, orgTier string,
	orgOverrideEnabled *bool, orgOverrideExpiresAt *time.Time,
	licenseOverride *bool,
	evaluatedAt time.Time,
) (allowed bool, reason string) {
	state := featureState{
		Registered: registered, GloballyEnabled: globallyEnabled,
		MinTier: minTier, OrgTier: orgTier,
		LicenseOverride: licenseOverride, EvaluatedAt: evaluatedAt,
	}
	if orgOverrideEnabled != nil {
		state.OrgOverride = &featureOverride{Enabled: *orgOverrideEnabled, ExpiresAt: orgOverrideExpiresAt}
	}
	decision := decideAgentContextRuntimeFeature(state)
	return decision.Allowed, decision.Reason
}

// AgentContextRuntimeFeatureKey is the one feature this package decides.
// Python: dev_health_ops.licensing.registry.py, "agent_context_runtime".
const AgentContextRuntimeFeatureKey = "agent_context_runtime"

// featureState is the complete policy input for one org's
// agent_context_runtime decision, loaded from a single query (see
// postgresState). It mirrors providersync.canonicalIncidentFeatureState in
// shape (same source tables), but the two are independent types: this
// package must decide agent_context_runtime's EXPLICIT_PURCHASE_REQUIRED
// path (see decideAgentContextRuntimeFeature), which does not exist on the
// canonical_incident_ingestion key that package decides.
type featureState struct {
	Registered      bool
	GloballyEnabled bool
	MinTier         string
	OrgTier         string
	OrgOverride     *featureOverride
	LicenseOverride *bool
	EvaluatedAt     time.Time
}

type featureOverride struct {
	Enabled   bool
	ExpiresAt *time.Time
}

// featureDecision mirrors Python's FeatureDecision dataclass field-for-field
// (feature_key, allowed, reason, expires_at, config) so a live-Python oracle
// test can compare it directly via asdict(decision) on the Python side.
// Config is never populated: neither this package's query nor Python's own
// FeatureOverrideSnapshot construction in the ported call path sets it (org
// overrides carry only is_enabled/expires_at here), matching the
// canonical_incident_ingestion oracle's own cases.
type featureDecision struct {
	FeatureKey string          `json:"feature_key"`
	Allowed    bool            `json:"allowed"`
	Reason     string          `json:"reason"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Config     *map[string]any `json:"config"`
}

// decideAgentContextRuntimeFeature ports feature_policy.decide_feature
// (src/dev_health_ops/licensing/feature_policy.py) for exactly the
// agent_context_runtime key.
//
// The one behaviour this key needs that providersync's
// decideCanonicalIncidentFeature does not: agent_context_runtime is a member
// of EXPLICIT_PURCHASE_FEATURES (licensing/registry.py), so
// is_explicit_purchase_feature(...) is True for it, and decide_feature's
// final branch is therefore ALWAYS
// closed(EXPLICIT_PURCHASE_REQUIRED) rather than a tier check -- an org at
// or above the feature's min_tier is NOT entitled by tier alone; only an org
// override or a license override can enable it. Getting this branch wrong
// (falling through to "tier allowed -> true", the shape that IS correct for
// canonical_incident_ingestion) would make every community-tier org
// wrongly entitled, since min_tier is "community" (registry.py) and every
// org clears that bar.
//
// _TIER_BOUND_OVERRIDE_FEATURES and ORG_OVERRIDE_ONLY_FEATURES (feature_policy.py)
// do not contain agent_context_runtime, so an active org or license override
// enables it unconditionally, with no tier gate and no "override-only"
// expiry refusal -- same as canonical_incident_ingestion, which is why that
// half of the port is a direct copy of the reasoning (not the code; the two
// packages intentionally do not share this function, matching
// providersync's own per-feature specialization of the generic decision).
func decideAgentContextRuntimeFeature(state featureState) featureDecision {
	closed := func(reason string) featureDecision {
		return featureDecision{FeatureKey: AgentContextRuntimeFeatureKey, Reason: reason}
	}
	// org_tier is loaded (featureState.OrgTier) but never read past this
	// point: tier_allowed is irrelevant to agent_context_runtime's decision
	// (see the doc comment above) -- unlike min_tier, whose PARSEABILITY
	// still gates is_storage_valid regardless of feature key.
	_, minOK := licenseTierIndex(state.MinTier)
	if !minOK {
		return closed("invalid_feature_state")
	}
	if !state.Registered {
		return closed("feature_not_registered")
	}
	if !state.GloballyEnabled {
		return closed("global_disabled")
	}
	if state.OrgOverride != nil {
		expired := state.OrgOverride.ExpiresAt != nil &&
			!state.OrgOverride.ExpiresAt.After(state.EvaluatedAt)
		if !expired {
			if !state.OrgOverride.Enabled {
				return closed("org_override_disabled")
			}
			return featureDecision{
				FeatureKey: AgentContextRuntimeFeatureKey, Allowed: true,
				Reason: "enabled_by_org_override", ExpiresAt: state.OrgOverride.ExpiresAt,
			}
		}
		// Expired and not org-override-only (ORG_OVERRIDE_ONLY_FEATURES is
		// empty today for every feature key, agent_context_runtime included):
		// Python falls through past the expiry, exactly as below.
	}
	if state.LicenseOverride != nil {
		if !*state.LicenseOverride {
			return closed("license_override_disabled")
		}
		return featureDecision{
			FeatureKey: AgentContextRuntimeFeatureKey, Allowed: true,
			Reason: "enabled_by_license_override",
		}
	}
	// is_explicit_purchase_feature(agent_context_runtime) is always True
	// (registry.py EXPLICIT_PURCHASE_FEATURES): unlike
	// decideCanonicalIncidentFeature, this path never checks org tier
	// against min tier and never returns "enabled_by_tier".
	return closed("explicit_purchase_required")
}

// licenseTierIndex mirrors dev_health_ops.licensing.types.TIER_ORDER's
// index lookup. An unrecognised tier string is an invalid-storage state in
// Python (LicenseTier(str(...)) raising ValueError), not a valid low tier.
func licenseTierIndex(value string) (int, bool) {
	switch value {
	case "community":
		return 0, true
	case "team":
		return 1, true
	case "enterprise":
		return 2, true
	default:
		return 0, false
	}
}

// pythonJSONTruth mirrors Python's bool(...) cast applied to a raw JSON
// value decoded from org_licenses.features_override (gating.py:
// "{str(key): bool(value) for key, value in raw_license_overrides.items()}").
// Python's bool() on a decoded JSON value is falsy only for None, "", 0/0.0,
// and an empty list/dict -- every other value, including the string "false",
// is truthy.
func pythonJSONTruth(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	case []any:
		return len(typed) != 0
	case map[string]any:
		return len(typed) != 0
	default:
		return false
	}
}
