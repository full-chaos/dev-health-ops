package authconfig

import (
	"strings"
	"testing"
)

// No auth-service token carries an ACR audience. AUTH_JWT_AUDIENCE names the
// one audience the session minter signs. acr-api and acr-mcp have a separate
// mint (ACP-ADR-13 decision 3, CHAOS-12248), so Load refuses either value,
// with the session routes mounted or not.
func TestNoAuthServiceConfigurationMintsAnACRAudience(t *testing.T) {
	classes := map[string]func() map[string]string{
		"eddsa": eddsaEnv,
	}
	for _, audience := range []string{"acr-api", "acr-mcp"} {
		for class, base := range classes {
			for _, setting := range []string{EnvAccessTokenAudience} {
				mounted := base()
				mounted[setting] = audience
				unmounted := base()
				delete(unmounted, EnvRateLimitKey)
				delete(unmounted, EnvSessionSuccessorKey)
				unmounted[setting] = audience
				for routes, env := range map[string]map[string]string{"session routes": mounted, "no session routes": unmounted} {
					_, err := Load(Spec{LookupEnv: lookupFrom(env)})
					if err == nil || !strings.Contains(err.Error(), setting) {
						t.Errorf("%s, %s, %s=%s: Load err %v; want a refusal naming %s", class, routes, setting, audience, err, setting)
					}
				}
			}
		}
	}
}
