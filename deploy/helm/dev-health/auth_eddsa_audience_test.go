package devhealth_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// The chart renders AUTH_JWT_AUDIENCE=dev-health-api under the eddsa access token class, and the session routes
// accept an EdDSA token of audience auth-service only. Started like that, every signed-in user would be refused at
// every session route, so auth-service refuses to start until the issuer mints the audience the routes accept.
func TestEdDSAChartEnvIsRefusedUntilTheIssuerMintsTheSessionAudience(t *testing.T) {
	docs := renderPhase(t, 2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example")
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment")
	}
	lookup := containerLookup(t, service, "auth-service")
	if audience, _ := lookup("AUTH_JWT_AUDIENCE"); audience != "dev-health-api" {
		t.Fatalf("the chart renders AUTH_JWT_AUDIENCE=%q under eddsa, want dev-health-api", audience)
	}
	_, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: lookup})
	if err == nil || !strings.Contains(err.Error(), "CHAOS-10456") || !strings.Contains(err.Error(), "AUTH_JWT_AUDIENCE") {
		t.Fatalf("auth-service started on the chart's eddsa settings, or refused without naming the audience and CHAOS-10456: %v", err)
	}

	// The same settings with the audience the session routes accept start.
	withSessionAudience := func(name string) (string, bool) {
		if name == "AUTH_JWT_AUDIENCE" {
			return "auth-service", true
		}
		return lookup(name)
	}
	if _, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: withSessionAudience}); err != nil {
		t.Fatalf("eddsa with audience auth-service is refused: %v", err)
	}
}
