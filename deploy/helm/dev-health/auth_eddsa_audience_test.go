package devhealth_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// The chart renders AUTH_JWT_AUDIENCE=dev-health-api under the eddsa access token class: the API token's audience.
// auth-service starts on it and mints the session routes' token of audience auth-service beside it. A hand-set
// auth-service audience, which every go-api and query-api row would refuse, is refused at start.
func TestEdDSAChartEnvStartsOnTheAPIAudience(t *testing.T) {
	docs := renderPhase(t, 2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example")
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment")
	}
	lookup := containerLookup(t, service, "auth-service")
	if audience, _ := lookup("AUTH_JWT_AUDIENCE"); audience != "dev-health-api" {
		t.Fatalf("the chart renders AUTH_JWT_AUDIENCE=%q under eddsa, want dev-health-api", audience)
	}
	cfg, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: lookup})
	if err != nil {
		t.Fatalf("auth-service refuses the chart's eddsa settings: %v", err)
	}
	if !cfg.Session.Enabled || cfg.Session.AccessClass != authconfig.AccessClassEdDSA {
		t.Fatalf("eddsa render: sessions %v, class %q", cfg.Session.Enabled, cfg.Session.AccessClass)
	}

	withSessionAudience := func(name string) (string, bool) {
		if name == "AUTH_JWT_AUDIENCE" {
			return "auth-service", true
		}
		return lookup(name)
	}
	_, err = authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: withSessionAudience})
	if err == nil || !strings.Contains(err.Error(), "AUTH_JWT_AUDIENCE") {
		t.Fatalf("eddsa with audience auth-service: %v; want a refusal naming AUTH_JWT_AUDIENCE", err)
	}
}
