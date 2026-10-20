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
	for name, cfg := range map[string]fakeConfig{
		"provider without a key":      {cfg: OrgConfig{Provider: "openai"}, found: true},
		"provider with an empty key":  {cfg: OrgConfig{Provider: "anthropic", APIKey: secrets.NewHidden("")}, found: true},
		"base url the guard refuses":  {cfg: OrgConfig{Provider: "openai", APIKey: secrets.NewHidden("k"), BaseURL: "http://169.254.169.254/v1"}, found: true},
		"provider the service lacks":  {cfg: OrgConfig{Provider: "no-such-provider", APIKey: secrets.NewHidden("k")}, found: true},
		"stored value cannot be read": {err: ErrStoredSettingUnreadable},
	} {
		t.Run(name, func(t *testing.T) {
			d := Gate{Source: PgSource{Licensing: both, Queryer: saved(true), Config: cfg}}.Decide(context.Background(), "org")
			if d.Kind != Refuse || d.Status != http.StatusForbidden || d.Code != "own_model_unusable" {
				t.Fatalf("decision = %+v, want a 403 refusal own_model_unusable", d)
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
