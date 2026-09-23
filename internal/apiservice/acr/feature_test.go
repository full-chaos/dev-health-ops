package acr

import (
	"testing"
	"time"
)

var evaluatedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func baseState() featureState {
	return featureState{
		Registered: true, GloballyEnabled: true,
		MinTier: "community", OrgTier: "community",
		EvaluatedAt: evaluatedAt,
	}
}

func TestDecideAgentContextRuntimeFeature(t *testing.T) {
	tests := []struct {
		name    string
		state   func() featureState
		allowed bool
		reason  string
	}{
		{
			name: "no override at all is EXPLICIT_PURCHASE_REQUIRED, never enabled by tier",
			// The state every community-tier org that never bought or was
			// granted the feature is in: registered, globally enabled,
			// min_tier <= org_tier -- and still closed. This is the branch
			// providersync's decideCanonicalIncidentFeature does not need
			// (canonical_incident_ingestion is not an explicit-purchase
			// feature): getting it wrong here means every org is wrongly
			// entitled.
			state:   baseState,
			allowed: false, reason: "explicit_purchase_required",
		},
		{
			name: "enterprise org tier still EXPLICIT_PURCHASE_REQUIRED",
			state: func() featureState {
				state := baseState()
				state.OrgTier = "enterprise"
				return state
			},
			allowed: false, reason: "explicit_purchase_required",
		},
		{
			name: "not registered",
			state: func() featureState {
				state := baseState()
				state.Registered = false
				return state
			},
			allowed: false, reason: "feature_not_registered",
		},
		{
			name: "invalid min_tier is INVALID_FEATURE_STATE even though unused elsewhere",
			state: func() featureState {
				state := baseState()
				state.MinTier = "bogus"
				return state
			},
			allowed: false, reason: "invalid_feature_state",
		},
		{
			name: "globally disabled",
			state: func() featureState {
				state := baseState()
				state.GloballyEnabled = false
				return state
			},
			allowed: false, reason: "global_disabled",
		},
		{
			name: "org override enabled",
			state: func() featureState {
				state := baseState()
				state.OrgOverride = &featureOverride{Enabled: true}
				return state
			},
			allowed: true, reason: "enabled_by_org_override",
		},
		{
			name: "org override disabled",
			state: func() featureState {
				state := baseState()
				state.OrgOverride = &featureOverride{Enabled: false}
				return state
			},
			allowed: false, reason: "org_override_disabled",
		},
		{
			name: "expired org override falls back to explicit purchase required",
			state: func() featureState {
				state := baseState()
				expiry := evaluatedAt.Add(-time.Hour)
				state.OrgOverride = &featureOverride{Enabled: true, ExpiresAt: &expiry}
				return state
			},
			allowed: false, reason: "explicit_purchase_required",
		},
		{
			name: "org override expiring exactly at evaluation time counts as expired",
			state: func() featureState {
				state := baseState()
				state.OrgOverride = &featureOverride{Enabled: true, ExpiresAt: &evaluatedAt}
				return state
			},
			allowed: false, reason: "explicit_purchase_required",
		},
		{
			name: "org override not yet expired",
			state: func() featureState {
				state := baseState()
				expiry := evaluatedAt.Add(time.Hour)
				state.OrgOverride = &featureOverride{Enabled: true, ExpiresAt: &expiry}
				return state
			},
			allowed: true, reason: "enabled_by_org_override",
		},
		{
			name: "license override enabled, no org override",
			state: func() featureState {
				state := baseState()
				value := true
				state.LicenseOverride = &value
				return state
			},
			allowed: true, reason: "enabled_by_license_override",
		},
		{
			name: "license override disabled",
			state: func() featureState {
				state := baseState()
				value := false
				state.LicenseOverride = &value
				return state
			},
			allowed: false, reason: "license_override_disabled",
		},
		{
			name: "expired org override falls back to license override",
			state: func() featureState {
				state := baseState()
				expiry := evaluatedAt.Add(-time.Hour)
				state.OrgOverride = &featureOverride{Enabled: false, ExpiresAt: &expiry}
				value := true
				state.LicenseOverride = &value
				return state
			},
			allowed: true, reason: "enabled_by_license_override",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := decideAgentContextRuntimeFeature(test.state())
			if decision.Allowed != test.allowed || decision.Reason != test.reason {
				t.Fatalf("decideAgentContextRuntimeFeature() = {Allowed:%v Reason:%q}, want {Allowed:%v Reason:%q}",
					decision.Allowed, decision.Reason, test.allowed, test.reason)
			}
			if decision.FeatureKey != AgentContextRuntimeFeatureKey {
				t.Fatalf("FeatureKey = %q, want %q", decision.FeatureKey, AgentContextRuntimeFeatureKey)
			}
		})
	}
}

func TestPythonJSONTruth(t *testing.T) {
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
			if got := pythonJSONTruth(test.value); got != test.want {
				t.Fatalf("pythonJSONTruth(%#v) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
