package llmorgsettings

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Only the inputs a decision needs are read: an org on a usable own model never waits on the
// platform_model_fallback lookup, a path that needs it still fails closed.
func TestUsableOwnModelIsAdmittedWithoutTheFallbackRead(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	noFallbackRow := fakeLicensing{ACRServiceModelFeatureKey: grant} // a missing key fails its lookup
	usable := fakeConfig{cfg: OrgConfig{Provider: "anthropic", APIKey: secrets.NewHidden("k")}, found: true}
	d := Gate{Source: PgSource{Licensing: noFallbackRow, Queryer: saved(true), Config: usable}}.Decide(context.Background(), "org")
	if d.Kind != Own {
		t.Fatalf("usable own model with a failing fallback lookup: %+v, want Own", d)
	}
	d = Gate{Source: PgSource{Licensing: noFallbackRow, Queryer: saved(false), Config: fakeConfig{}}}.Decide(context.Background(), "org")
	if d.Kind != Refuse || d.Status != http.StatusServiceUnavailable {
		t.Fatalf("no saved config with a failing fallback lookup: %+v, want 503", d)
	}
}

// Own versus platform routing is visible at Info: one line per admit decision with the org, the decision and
// the model source, never a credential or a base_url.
func TestAdmitDecisionIsLoggedAtInfoWithoutCredentials(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	for name, c := range map[string]struct {
		saved bool
		cfg   fakeConfig
		want  []string
	}{
		"own":      {true, fakeConfig{cfg: OrgConfig{Provider: "openai", APIKey: secrets.NewHidden("sk-secret-key"), BaseURL: "https://byo.example.com/v1"}, found: true}, []string{"org_id=org-1", "decision=own", "model_source=org_byo"}},
		"platform": {false, fakeConfig{}, []string{"org_id=org-1", "decision=platform", "model_source=platform"}},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo}))
			d := Gate{Source: PgSource{Licensing: both, Queryer: saved(c.saved), Config: c.cfg}, Logger: logger}.Decide(context.Background(), "org-1")
			if d.Kind == Refuse {
				t.Fatalf("decision = %+v", d)
			}
			for _, want := range c.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("log %q lacks %q", out.String(), want)
				}
			}
			for _, secret := range []string{"sk-secret-key", "byo.example.com"} {
				if strings.Contains(out.String(), secret) {
					t.Errorf("log carries %q", secret)
				}
			}
		})
	}
}
