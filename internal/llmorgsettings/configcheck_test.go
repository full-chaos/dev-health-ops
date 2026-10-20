package llmorgsettings

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The gate and the org status route ask one question: the gate admits a saved config exactly when
// CheckConfig says active, and CheckConfig is the status route's two stages in order.
func TestGateAndStatusAgreeOnWhatACannotServeConfigIs(t *testing.T) {
	grant := licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}
	both := fakeLicensing{ACRServiceModelFeatureKey: grant, PlatformModelFallbackFeatureKey: grant}
	cases := []struct {
		name                      string
		provider, apiKey, baseURL string
		want                      ConfigReason
	}{
		{"complete", "anthropic", "k", "", ConfigActive},
		{"no key", "openai", "", "", ConfigMissingCredentials},
		{"empty key with a url", "anthropic", "", "https://api.example.com/v1", ConfigMissingCredentials},
		{"base url refused", "openai", "k", "http://169.254.169.254/v1", ConfigInvalidBaseURL},
		{"unknown provider", "no-such-provider", "k", "", ConfigUnknownProvider},
		{"auto", "auto", "k", "", ConfigNotConfigured},
		{"empty provider", "", "k", "", ConfigNotConfigured},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := CheckConfig(context.Background(), c.provider, c.apiKey, c.baseURL)
			if err != nil || got != c.want {
				t.Fatalf("CheckConfig = %q, %v, want %q", got, err, c.want)
			}
			name, reason := CheckProvider(c.provider)
			staged := reason
			if reason == ConfigActive {
				if staged, err = CheckCredentials(context.Background(), name, c.apiKey, c.baseURL); err != nil {
					t.Fatal(err)
				}
			}
			if staged != got {
				t.Fatalf("staged check = %q, CheckConfig = %q", staged, got)
			}
			cfg := fakeConfig{cfg: OrgConfig{Provider: c.provider, APIKey: secrets.NewHidden(c.apiKey), BaseURL: c.baseURL}, found: true}
			d := Gate{Source: PgSource{Licensing: both, Queryer: saved(true), Config: cfg}}.Decide(context.Background(), "org")
			if (d.Kind == Own) != (got == ConfigActive) {
				t.Fatalf("gate decision %+v disagrees with CheckConfig %q", d, got)
			}
			if got != ConfigActive && d.Code != string(got) {
				t.Fatalf("gate detail %q, status reason %q", d.Code, got)
			}
		})
	}
}
