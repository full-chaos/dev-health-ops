package authconfig

import (
	"strings"
	"testing"
)

// Under the eddsa class AUTH_JWT_AUDIENCE is the API token's audience (go-api and query-api accept dev-health-api
// only); the session routes' token of audience auth-service is minted beside it. So eddsa with the session routes
// starts on the API audience (the chart's value, and the default), and an auth-service audience, which every go-api
// and query-api row would refuse, is refused at start.
func TestEdDSAClassMintsTheAPIAudienceAndTheSessionRouteTokenBeside(t *testing.T) {
	for _, audience := range []string{"", "dev-health-api"} {
		env := eddsaEnv()
		delete(env, EnvAccessTokenAudience)
		if audience != "" {
			env[EnvAccessTokenAudience] = audience
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Errorf("eddsa with audience %q: %v", audience, err)
			continue
		}
		if !cfg.Session.Enabled || cfg.Session.AccessAudience != "dev-health-api" {
			t.Errorf("eddsa with audience %q: sessions %v, audience %q", audience, cfg.Session.Enabled, cfg.Session.AccessAudience)
		}
	}
	env := eddsaEnv()
	env[EnvAccessTokenAudience] = "auth-service"
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvAccessTokenAudience) || strings.Contains(err.Error(), "CHAOS-") {
		t.Errorf("eddsa with audience auth-service: Load err %v; want a refusal naming %s", err, EnvAccessTokenAudience)
	}
	for _, audience := range []string{"", "dev-health-api", "auth-service"} {
		env := sessionEnv(testAccessSecret, testSuccessorKey)
		if audience != "" {
			env[EnvAccessTokenAudience] = audience
		}
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
			t.Errorf("hs256_compat with audience %q: %v", audience, err)
		}
	}
}

// With no session secrets no session route is mounted and no token is minted, so the audience rule has nothing to
// guard.
func TestTheAudienceRuleDoesNotApplyWithoutTheSessionRoutes(t *testing.T) {
	env := eddsaEnv()
	delete(env, EnvAccessTokenSecret)
	delete(env, EnvSessionSuccessorKey)
	env[EnvAccessTokenAudience] = "auth-service"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("eddsa without the session routes: %v", err)
	}
	if cfg.Session.Enabled {
		t.Fatal("the session routes are enabled without their secrets")
	}
}
