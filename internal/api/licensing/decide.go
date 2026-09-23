// Package licensing ports dev_health_ops.licensing.feature_policy.decide_feature
// (and the registry membership sets it consults) into Go, generically over
// any feature key the Postgres feature_flags/org_feature_overrides/
// org_licenses/organizations tables can describe. It is the ONE feature-
// decision engine dho api's own routes consult -- CHAOS-6244 (the acr
// entitlement route) is its first caller; every later route needing a
// feature decision imports this package rather than re-deriving the engine.
//
// providersync.decideCanonicalIncidentFeature (internal/providersync/
// incident_entitlement.go) is a SEPARATE, pre-existing port of the same
// Python function, used by the sync-worker's dispatch-time gate. The two
// are not merged here: that package's callers, its own generated-golden
// test corpus, and its own oracle case set are unrelated to the api
// Service's route layer, and unifying them is out of this package's scope.
package licensing

import "time"

// explicitPurchaseFeatures mirrors dev_health_ops.licensing.registry.py's
// EXPLICIT_PURCHASE_FEATURES frozenset. A feature key in this set is NEVER
// enabled by tier or registration alone -- only an active org override or
// license override can enable it (see Decide's final branch). Getting a
// feature's membership here wrong the "not a member" direction makes an
// unpurchased org wrongly entitled the moment its tier clears the feature's
// min_tier.
var explicitPurchaseFeatures = map[string]bool{
	"agent_context_runtime":          true,
	"ask_dev":                        true,
	"ask_dev_contextual_entrypoints": true,
	"ask_dev_wave_3_1":               true,
}

// tierBoundOverrideFeatures mirrors feature_policy.py's
// _TIER_BOUND_OVERRIDE_FEATURES: for a feature key in this set, an active
// org or license override still requires org_tier >= min_tier -- the
// override does not bypass the tier gate, only the explicit-purchase gate.
var tierBoundOverrideFeatures = map[string]bool{
	"customer_push_ingest": true,
}

// orgOverrideOnlyFeatures mirrors registry.py's ORG_OVERRIDE_ONLY_FEATURES,
// empty in production today (no feature key is currently org-override-only).
// Kept as a real set, not deleted, because decide_feature's own logic
// branches on it unconditionally -- an empty set here is a fact about
// today's registry content, not a reason to skip the branches that consult
// it.
var orgOverrideOnlyFeatures = map[string]bool{}

// State is the complete policy input for one (org, feature) decision,
// mirroring Python's FeatureDecisionContext (minus feature_key, which
// Decide takes as its own parameter so State stays reusable across calls in
// a batch).
type State struct {
	Registered      bool
	GloballyEnabled bool
	MinTier         string
	OrgTier         string
	OrgOverride     *Override
	LicenseOverride *bool
	EvaluatedAt     time.Time
}

// Override is one org's active or expired override row for a feature.
type Override struct {
	Enabled   bool
	ExpiresAt *time.Time
}

// Decision mirrors Python's FeatureDecision dataclass field-for-field
// (feature_key, allowed, reason, expires_at, config), so a live-Python
// oracle test can compare it directly against asdict(decision).
type Decision struct {
	FeatureKey string          `json:"feature_key"`
	Allowed    bool            `json:"allowed"`
	Reason     string          `json:"reason"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Config     *map[string]any `json:"config"`
}

// Reason values mirror feature_policy.FeatureDecisionReason's members
// exactly (the StrEnum values, snake_case).
const (
	ReasonEnabledByOrgOverride     = "enabled_by_org_override"
	ReasonEnabledByLicenseOverride = "enabled_by_license_override"
	ReasonEnabledByTier            = "enabled_by_tier"
	ReasonFeatureNotRegistered     = "feature_not_registered"
	ReasonGlobalDisabled           = "global_disabled"
	ReasonInvalidFeatureState      = "invalid_feature_state"
	ReasonOrgOverrideExpired       = "org_override_expired"
	ReasonOrgOverrideDisabled      = "org_override_disabled"
	ReasonOrgOverrideRequired      = "org_override_required"
	ReasonLicenseOverrideDisabled  = "license_override_disabled"
	ReasonExplicitPurchaseRequired = "explicit_purchase_required"
	ReasonTierRequired             = "tier_required"
)

// Decide ports feature_policy.decide_feature exactly, for featureKey and
// the given policy State. Pure and DB-free so both a unit test and a
// live-Python oracle test can drive it directly with hand-built State
// values.
func Decide(featureKey string, state State) Decision {
	closed := func(reason string) Decision {
		return Decision{FeatureKey: featureKey, Reason: reason}
	}
	minTier, minOK := tierIndex(state.MinTier)
	if !minOK {
		return closed(ReasonInvalidFeatureState)
	}
	if !state.Registered {
		return closed(ReasonFeatureNotRegistered)
	}
	if !state.GloballyEnabled {
		return closed(ReasonGlobalDisabled)
	}

	orgTier, orgOK := tierIndex(state.OrgTier)
	if !orgOK {
		orgTier = 0
	}
	tierAllowed := orgTier >= minTier

	if state.OrgOverride != nil {
		expired := state.OrgOverride.ExpiresAt != nil &&
			!state.OrgOverride.ExpiresAt.After(state.EvaluatedAt)
		if expired && orgOverrideOnlyFeatures[featureKey] {
			return closed(ReasonOrgOverrideExpired)
		}
		if !expired {
			if !state.OrgOverride.Enabled {
				return closed(ReasonOrgOverrideDisabled)
			}
			if tierBoundOverrideFeatures[featureKey] && !tierAllowed {
				return closed(ReasonTierRequired)
			}
			return Decision{
				FeatureKey: featureKey, Allowed: true,
				Reason: ReasonEnabledByOrgOverride, ExpiresAt: state.OrgOverride.ExpiresAt,
			}
		}
		// Expired and not org-override-only: falls through, exactly as
		// Python's decide_feature does (the override_expired branch only
		// returns early for an org-override-only feature key).
	}

	if state.LicenseOverride != nil {
		if !*state.LicenseOverride {
			return closed(ReasonLicenseOverrideDisabled)
		}
		if orgOverrideOnlyFeatures[featureKey] {
			return closed(ReasonOrgOverrideRequired)
		}
		if tierBoundOverrideFeatures[featureKey] && !tierAllowed {
			return closed(ReasonTierRequired)
		}
		return Decision{
			FeatureKey: featureKey, Allowed: true,
			Reason: ReasonEnabledByLicenseOverride,
		}
	}

	if explicitPurchaseFeatures[featureKey] {
		return closed(ReasonExplicitPurchaseRequired)
	}
	if tierAllowed {
		return Decision{FeatureKey: featureKey, Allowed: true, Reason: ReasonEnabledByTier}
	}
	return closed(ReasonTierRequired)
}

// tierIndex mirrors dev_health_ops.licensing.types.TIER_ORDER's index
// lookup. An unrecognised tier string is an invalid-storage state in Python
// (LicenseTier(str(...)) raising ValueError), not a valid low tier.
func tierIndex(value string) (int, bool) {
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

// jsonTruth mirrors Python's bool(...) cast applied to a raw JSON value
// decoded from org_licenses.features_override (gating.py:
// "{str(key): bool(value) for key, value in raw_license_overrides.items()}").
// Python's bool() on a decoded JSON value is falsy only for None, "",
// 0/0.0, and an empty list/dict -- every other value, including the string
// "false", is truthy.
func jsonTruth(value any) bool {
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
