package licensing

import (
	"testing"
	"time"
)

var evaluatedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func baseState() State {
	return State{
		Registered: true, GloballyEnabled: true,
		MinTier: "community", OrgTier: "community",
		EvaluatedAt: evaluatedAt,
	}
}

// TestDecideExplicitPurchaseFeatureNeverEnabledByTierAlone is the money
// test for CHAOS-6244: agent_context_runtime (an EXPLICIT_PURCHASE_FEATURES
// member) must be CLOSED with no override, even though its min_tier is
// COMMUNITY and every org clears that bar -- the exact branch
// providersync's decideCanonicalIncidentFeature does not need, because
// canonical_incident_ingestion is NOT an explicit-purchase feature.
func TestDecideExplicitPurchaseFeatureNeverEnabledByTierAlone(t *testing.T) {
	for _, key := range []string{"agent_context_runtime", "ask_dev", "ask_dev_contextual_entrypoints", "ask_dev_wave_3_1"} {
		t.Run(key, func(t *testing.T) {
			decision := Decide(key, baseState())
			if decision.Allowed || decision.Reason != ReasonExplicitPurchaseRequired {
				t.Fatalf("Decide(%q, base) = {Allowed:%v Reason:%q}, want closed explicit_purchase_required",
					key, decision.Allowed, decision.Reason)
			}
		})
	}
}

// TestDecideNonExplicitPurchaseFeatureEnabledByTier proves the CONTRAST:
// a feature key that is NOT in EXPLICIT_PURCHASE_FEATURES (an arbitrary,
// unregistered-in-the-maps key, standing in for canonical_incident_ingestion
// or any ordinary tiered feature) reaches enabled_by_tier under the
// IDENTICAL base state that closes an explicit-purchase feature above --
// this is the divergence the whole package split exists to get right.
func TestDecideNonExplicitPurchaseFeatureEnabledByTier(t *testing.T) {
	decision := Decide("canonical_incident_ingestion", baseState())
	if !decision.Allowed || decision.Reason != ReasonEnabledByTier {
		t.Fatalf("decision = %+v, want enabled_by_tier", decision)
	}
}

func TestDecideFullMatrix(t *testing.T) {
	tests := []struct {
		name       string
		featureKey string
		state      func() State
		allowed    bool
		reason     string
	}{
		{
			name: "not registered", featureKey: "agent_context_runtime",
			state:   func() State { state := baseState(); state.Registered = false; return state },
			allowed: false, reason: ReasonFeatureNotRegistered,
		},
		{
			name: "invalid min_tier", featureKey: "agent_context_runtime",
			state:   func() State { state := baseState(); state.MinTier = "bogus"; return state },
			allowed: false, reason: ReasonInvalidFeatureState,
		},
		{
			name: "globally disabled", featureKey: "agent_context_runtime",
			state:   func() State { state := baseState(); state.GloballyEnabled = false; return state },
			allowed: false, reason: ReasonGlobalDisabled,
		},
		{
			name: "org override enabled, explicit-purchase feature", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				state.OrgOverride = &Override{Enabled: true}
				return state
			},
			allowed: true, reason: ReasonEnabledByOrgOverride,
		},
		{
			name: "org override disabled", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				state.OrgOverride = &Override{Enabled: false}
				return state
			},
			allowed: false, reason: ReasonOrgOverrideDisabled,
		},
		{
			name: "expired org override falls back to explicit purchase required", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				expiry := evaluatedAt.Add(-time.Hour)
				state.OrgOverride = &Override{Enabled: true, ExpiresAt: &expiry}
				return state
			},
			allowed: false, reason: ReasonExplicitPurchaseRequired,
		},
		{
			name: "org override expiring exactly at evaluation time counts as expired", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				state.OrgOverride = &Override{Enabled: true, ExpiresAt: &evaluatedAt}
				return state
			},
			allowed: false, reason: ReasonExplicitPurchaseRequired,
		},
		{
			name: "org override not yet expired", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				expiry := evaluatedAt.Add(time.Hour)
				state.OrgOverride = &Override{Enabled: true, ExpiresAt: &expiry}
				return state
			},
			allowed: true, reason: ReasonEnabledByOrgOverride,
		},
		{
			name: "license override enabled, no org override", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				value := true
				state.LicenseOverride = &value
				return state
			},
			allowed: true, reason: ReasonEnabledByLicenseOverride,
		},
		{
			name: "license override disabled", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				value := false
				state.LicenseOverride = &value
				return state
			},
			allowed: false, reason: ReasonLicenseOverrideDisabled,
		},
		{
			name: "expired org override falls back to license override", featureKey: "agent_context_runtime",
			state: func() State {
				state := baseState()
				expiry := evaluatedAt.Add(-time.Hour)
				state.OrgOverride = &Override{Enabled: false, ExpiresAt: &expiry}
				value := true
				state.LicenseOverride = &value
				return state
			},
			allowed: true, reason: ReasonEnabledByLicenseOverride,
		},
		// tierBoundOverrideFeatures: an org override on customer_push_ingest
		// still requires org_tier >= min_tier -- the override enables the
		// EXPLICIT-PURCHASE gate, never the tier gate.
		{
			name: "tier-bound override feature: org override enabled but tier too low", featureKey: "customer_push_ingest",
			state: func() State {
				state := baseState()
				state.MinTier, state.OrgTier = "enterprise", "community"
				state.OrgOverride = &Override{Enabled: true}
				return state
			},
			allowed: false, reason: ReasonTierRequired,
		},
		{
			name: "tier-bound override feature: org override enabled and tier allowed", featureKey: "customer_push_ingest",
			state: func() State {
				state := baseState()
				state.OrgOverride = &Override{Enabled: true}
				return state
			},
			allowed: true, reason: ReasonEnabledByOrgOverride,
		},
		{
			name: "tier-bound override feature: license override enabled but tier too low", featureKey: "customer_push_ingest",
			state: func() State {
				state := baseState()
				state.MinTier, state.OrgTier = "enterprise", "community"
				value := true
				state.LicenseOverride = &value
				return state
			},
			allowed: false, reason: ReasonTierRequired,
		},
		{
			name: "ordinary tiered feature: tier too low", featureKey: "canonical_incident_ingestion",
			state: func() State {
				state := baseState()
				state.MinTier, state.OrgTier = "enterprise", "team"
				return state
			},
			allowed: false, reason: ReasonTierRequired,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := Decide(test.featureKey, test.state())
			if decision.Allowed != test.allowed || decision.Reason != test.reason {
				t.Fatalf("Decide(%q, ...) = {Allowed:%v Reason:%q}, want {Allowed:%v Reason:%q}",
					test.featureKey, decision.Allowed, decision.Reason, test.allowed, test.reason)
			}
			if decision.FeatureKey != test.featureKey {
				t.Fatalf("FeatureKey = %q, want %q", decision.FeatureKey, test.featureKey)
			}
		})
	}
}

func TestJSONTruth(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{"nil", nil, false},
		{"false", false, false},
		{"true", true, true},
		{"empty string", "", false},
		{"non-empty string", "false", true},
		{"zero float", float64(0), false},
		{"non-zero float", float64(1), true},
		{"empty slice", []any{}, false},
		{"non-empty slice", []any{1}, true},
		{"empty map", map[string]any{}, false},
		{"non-empty map", map[string]any{"a": 1}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := jsonTruth(test.value); got != test.want {
				t.Fatalf("jsonTruth(%#v) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
