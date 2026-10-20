package llmorgsettings

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A saved configuration that cannot serve a call is a refusal with a reason,
// never the org's own model, never the platform model, never a 5xx.
func TestSavedConfigThatCannotServeIsARefusalWithAReason(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	for name, c := range map[string]struct {
		cfg    fakeConfig
		reason string
	}{
		"provider without a key":      {fakeConfig{cfg: OrgConfig{Provider: "openai"}, found: true}, "missing_credentials"},
		"provider with an empty key":  {fakeConfig{cfg: OrgConfig{Provider: "anthropic", APIKey: secrets.NewHidden("")}, found: true}, "missing_credentials"},
		"base url the guard refuses":  {fakeConfig{cfg: OrgConfig{Provider: "openai", APIKey: secrets.NewHidden("k"), BaseURL: "http://169.254.169.254/v1"}, found: true}, "invalid_base_url"},
		"provider the service lacks":  {fakeConfig{cfg: OrgConfig{Provider: "no-such-provider", APIKey: secrets.NewHidden("k")}, found: true}, "unknown_provider"},
		"stored value cannot be read": {fakeConfig{err: ErrStoredSettingUnreadable}, "unreadable_row"},
	} {
		t.Run(name, func(t *testing.T) {
			d := Gate{Source: PgSource{Licensing: both, Queryer: saved(true), Config: c.cfg}}.Decide(context.Background(), "org")
			if d.Kind != Refuse || d.Status != http.StatusForbidden || d.Code != c.reason {
				t.Fatalf("decision = %+v, want a 403 refusal with detail %q", d, c.reason)
			}
		})
	}
	usable := fakeConfig{cfg: OrgConfig{Provider: "anthropic", APIKey: secrets.NewHidden("k")}, found: true}
	if d := (Gate{Source: PgSource{Licensing: both, Queryer: saved(true), Config: usable}}).Decide(context.Background(), "org"); d.Kind != Own {
		t.Fatalf("a complete saved config must stay the own model: %+v", d)
	}
}

// A database fault reading the row is infrastructure: 503, not a verdict on the config.
func TestConfigReadFaultIsUnavailable(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	d := Gate{Source: PgSource{Licensing: both, Queryer: saved(true), Config: fakeConfig{err: errors.New("connection reset")}}}.Decide(context.Background(), "org")
	if d.Kind != Refuse || d.Status != http.StatusServiceUnavailable {
		t.Fatalf("decision = %+v, want 503", d)
	}
}
