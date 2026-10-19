package llmorgsettings

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

func TestDecideTable(t *testing.T) {
	cases := []struct {
		name string
		in   Inputs
		kind Kind
		code string
	}{
		{"B1", Inputs{Saved: true, ConfigUsable: true, ServiceModelOn: true}, Own, ""},
		{"B1 with the grant never reads the platform", Inputs{Saved: true, ConfigUsable: true, ServiceModelOn: true, Fallback: true}, Own, ""},
		{"B1-off", Inputs{Saved: true, ConfigUsable: true}, Refuse, CodeAIFeaturesTurnedOff},
		{"B1-off outranks a closed gate", Inputs{Saved: true, GateClosed: true}, Refuse, CodeAIFeaturesTurnedOff},
		{"B1'", Inputs{Saved: true, ServiceModelOn: true}, Refuse, CodeUnavailable},
		{"B2", Inputs{Saved: true, GateClosed: true, ServiceModelOn: true}, Refuse, CodeOwnModelGateClosed},
		{"B2 with the grant", Inputs{Saved: true, GateClosed: true, ServiceModelOn: true, Fallback: true}, Refuse, CodeOwnModelGateClosed},
		{"B3", Inputs{ServiceModelOn: true, Fallback: true}, Platform, ""},
		{"B4", Inputs{ServiceModelOn: true}, Refuse, CodeServiceModelNotEntitled},
		{"B5", Inputs{}, Refuse, CodeNothing},
		{"fallback alone is not enough", Inputs{Fallback: true}, Refuse, CodeNothing},
	}
	for _, c := range cases {
		got := Decide(c.in)
		if got.Kind != c.kind || got.Code != c.code {
			t.Errorf("%s: got %+v, want kind %d code %q", c.name, got, c.kind, c.code)
		}
		if got.Kind == Refuse && got.Status != http.StatusForbidden && got.Status != http.StatusServiceUnavailable {
			t.Errorf("%s: refusal status %d", c.name, got.Status)
		}
	}
}

type fakeLicensing map[string]licensing.Decision

func (f fakeLicensing) Decide(_ context.Context, _, key string) (licensing.Decision, error) {
	d, ok := f[key]
	if !ok {
		return licensing.Decision{}, errors.New("read failed")
	}
	return d, nil
}

type fakeConfig struct {
	cfg   OrgConfig
	found bool
	err   error
}

func (f fakeConfig) OrgConfig(context.Context, string) (OrgConfig, bool, error) {
	return f.cfg, f.found, f.err
}

func saved(v bool) func(context.Context, string) (bool, error) {
	return func(context.Context, string) (bool, error) { return v, nil }
}

func TestPgSourceReadsTheEffectiveFlagAndTheClosedGate(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	off := licensing.Decision{Allowed: false, Reason: licensing.ReasonOrgOverrideDisabled}
	plan := licensing.Decision{Allowed: false, Reason: licensing.ReasonTierRequired}
	gate := func(lic fakeLicensing, savedBYO bool, cfg fakeConfig) Decision {
		return Gate{Source: PgSource{Licensing: lic, Saved: saved(savedBYO), Config: cfg}}.Decide(context.Background(), "org")
	}
	own := fakeConfig{cfg: OrgConfig{Provider: "anthropic"}, found: true}
	if d := gate(fakeLicensing{ACRServiceModelFeatureKey: plan, PlatformModelFallbackFeatureKey: plan}, true, own); d.Kind != Own {
		t.Errorf("a saved config turns acr_service_model on: %+v", d)
	}
	if d := gate(fakeLicensing{ACRServiceModelFeatureKey: off, PlatformModelFallbackFeatureKey: grant}, true, own); d.Code != CodeAIFeaturesTurnedOff {
		t.Errorf("an org override OFF refuses a saved config: %+v", d)
	}
	closed := fakeConfig{cfg: OrgConfig{GateClosed: true}}
	if d := gate(fakeLicensing{ACRServiceModelFeatureKey: plan, PlatformModelFallbackFeatureKey: grant}, true, closed); d.Code != CodeOwnModelGateClosed {
		t.Errorf("a saved config behind a closed gate refuses even with the grant: %+v", d)
	}
	if d := gate(fakeLicensing{ACRServiceModelFeatureKey: plan, PlatformModelFallbackFeatureKey: grant}, false, fakeConfig{}); d.Code != CodeNothing {
		t.Errorf("no config and a plan without acr_service_model: %+v", d)
	}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	if d := gate(both, false, fakeConfig{}); d.Kind != Platform {
		t.Errorf("no config with both grants uses the platform: %+v", d)
	}
	if d := gate(fakeLicensing{ACRServiceModelFeatureKey: grant}, false, fakeConfig{}); d.Status != http.StatusServiceUnavailable {
		t.Errorf("an unreadable grant is not a grant: %+v", d)
	}
	unreadable := fakeConfig{err: ErrStoredSettingUnreadable}
	if d := gate(both, false, unreadable); d.Status != http.StatusServiceUnavailable {
		t.Errorf("an unreadable saved config refuses as unavailable, never the platform: %+v", d)
	}
}
