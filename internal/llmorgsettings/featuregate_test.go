package llmorgsettings

import (
	"testing"
	"time"
)

func boolPtr(b bool) *bool { return &b }

// TestDecideByoLLM covers evaluate_org_feature_sync's precedence
// (decide_feature) as narrowed to byo_llm in decideByoLLM's own doc
// comment: registered/enabled gates, then org override, then license
// override, then tier -- matching team-lead's requested precedence matrix
// (flag off / override on / override off / tier below floor / tier at
// floor / no license) plus the "unregistered" backward-compat state
// byo_llm_flag_state's caller treats as ungated.
func TestDecideByoLLM(t *testing.T) {
	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	cases := []struct {
		name  string
		state byoLLMFeatureState
		want  string
	}{
		{
			name:  "feature row absent -> unregistered (backward compat, ungated)",
			state: byoLLMFeatureState{registered: false, minTier: "community", orgTier: "team"},
			want:  flagStateUnregistered,
		},
		{
			name: "invalid stored min_tier -> disabled",
			state: byoLLMFeatureState{
				registered: true, minTier: "not-a-tier", globallyEnabled: true, orgTier: "enterprise",
			},
			want: flagStateDisabled,
		},
		{
			name: "globally disabled -> disabled even at enterprise tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: false, orgTier: "enterprise",
			},
			want: flagStateDisabled,
		},
		{
			name: "team tier, no override, no license -> enabled by tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
			},
			want: flagStateEnabled,
		},
		{
			name: "community tier, no override -> tier required, disabled",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
			},
			want: flagStateDisabled,
		},
		{
			name: "org override enabled wins over insufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
				overrideEnabled: boolPtr(true),
			},
			want: flagStateEnabled,
		},
		{
			name: "org override disabled wins over sufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				overrideEnabled: boolPtr(false),
			},
			want: flagStateDisabled,
		},
		{
			name: "expired org override falls through to tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
				overrideEnabled: boolPtr(false), overrideExpires: &past,
			},
			want: flagStateEnabled,
		},
		{
			name: "org override not yet expired still applies",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
				overrideEnabled: boolPtr(false), overrideExpires: &future,
			},
			want: flagStateDisabled,
		},
		{
			name: "license override enabled wins over insufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
				licenseOverride: boolPtr(true),
			},
			want: flagStateEnabled,
		},
		{
			name: "license override disabled wins over sufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				licenseOverride: boolPtr(false),
			},
			want: flagStateDisabled,
		},
		{
			name: "org override beats license override",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				overrideEnabled: boolPtr(true), licenseOverride: boolPtr(false),
			},
			want: flagStateEnabled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideByoLLM(tc.state, now)
			if got != tc.want {
				t.Fatalf("decideByoLLM() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecideByoLLM_FloorBypassSanity documents WHY byoLLMFlagState cannot
// simply call decideByoLLM alone: decideByoLLM (== decide_feature, no
// external floor) allows a COMMUNITY-tier org through on a positive org
// override, same as evaluate_org_feature_sync would for any ordinary
// feature. byo_llm's TEAM-tier floor is a SEPARATE, EARLIER check in
// byoLLMFlagState (mirroring feature_flag_state's own min_tier
// short-circuit) specifically so this case does NOT enable byo_llm for a
// COMMUNITY org. The floor check plus the real DB read together are
// covered end-to-end by the bigboy integration test (seeds this exact
// override-on/community-tier row and asserts the org is refused).
func TestDecideByoLLM_FloorBypassSanity(t *testing.T) {
	state := byoLLMFeatureState{
		registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
		overrideEnabled: boolPtr(true),
	}
	if got := decideByoLLM(state, time.Now()); got != flagStateEnabled {
		t.Fatalf("decideByoLLM (no floor) = %q, want enabled -- this is exactly why "+
			"byoLLMFlagState needs its own floor check ahead of it", got)
	}
}
