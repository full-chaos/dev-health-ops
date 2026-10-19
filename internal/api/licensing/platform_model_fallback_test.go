package licensing

import (
	"testing"
	"time"
)

// platform_model_fallback is a platform-admin grant: only an active, enabled
// org override opens it. No tier opens it (whatever its min_tier), a license
// features_override does not, and an expired override closes it.
func TestPlatformModelFallbackOpensOnlyOnAnActiveOrgOverride(t *testing.T) {
	const key = "platform_model_fallback"
	yes := true
	past := evaluatedAt.Add(-time.Hour)
	future := evaluatedAt.Add(time.Hour)
	for _, tc := range []struct {
		name    string
		state   func(State) State
		allowed bool
		reason  string
	}{
		{name: "enterprise tier, no override", state: func(s State) State { s.OrgTier = "enterprise"; return s }, reason: ReasonExplicitPurchaseRequired},
		{name: "community tier, no override", state: func(s State) State { return s }, reason: ReasonExplicitPurchaseRequired},
		{name: "license features_override true", state: func(s State) State { s.OrgTier = "enterprise"; s.LicenseOverride = &yes; return s }, reason: ReasonOrgOverrideRequired},
		{name: "expired org override", state: func(s State) State {
			s.OrgTier = "enterprise"
			s.OrgOverride = &Override{Enabled: true, ExpiresAt: &past}
			return s
		}, reason: ReasonOrgOverrideExpired},
		{name: "org override off", state: func(s State) State { s.OrgOverride = &Override{Enabled: false}; return s }, reason: ReasonOrgOverrideDisabled},
		{name: "active org override on community tier", state: func(s State) State { s.OrgOverride = &Override{Enabled: true}; return s }, allowed: true, reason: ReasonEnabledByOrgOverride},
		{name: "org override with a future expiry", state: func(s State) State { s.OrgOverride = &Override{Enabled: true, ExpiresAt: &future}; return s }, allowed: true, reason: ReasonEnabledByOrgOverride},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision := Decide(key, tc.state(baseState()))
			if decision.Allowed != tc.allowed || decision.Reason != tc.reason {
				t.Fatalf("decision = %+v, want allowed=%t reason=%s", decision, tc.allowed, tc.reason)
			}
		})
	}
}
