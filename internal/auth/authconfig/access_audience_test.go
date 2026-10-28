package authconfig

import (
	"strings"
	"testing"
)

// The session routes accept an EdDSA access token of audience auth-service only. Under the eddsa class with the
// session routes mounted, a minted audience the routes refuse would end every signed-in user's session routes, so
// Load refuses it and names the ticket that switches the issuer.
func TestEdDSAClassWithTheSessionRoutesNeedsTheSessionRouteAudience(t *testing.T) {
	for _, audience := range []string{"", "dev-health-api"} {
		env := eddsaEnv()
		delete(env, EnvAccessTokenAudience)
		if audience != "" {
			env[EnvAccessTokenAudience] = audience
		}
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), "CHAOS-10456") || !strings.Contains(err.Error(), EnvAccessTokenAudience) {
			t.Errorf("audience %q: Load err %v; want a refusal naming %s and CHAOS-10456", audience, err, EnvAccessTokenAudience)
		}
	}
	env := eddsaEnv()
	env[EnvAccessTokenAudience] = "auth-service"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
		t.Errorf("eddsa with audience auth-service: %v", err)
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
