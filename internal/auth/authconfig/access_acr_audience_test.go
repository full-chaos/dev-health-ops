package authconfig

import (
	"strings"
	"testing"
)

// AUTH_JWT_AUDIENCE names the one audience the session minter signs. The ACR
// audiences acr-api and acr-mcp have a separate mint (ACP-ADR-13 decision 3,
// CHAOS-12248), so Load refuses them under the eddsa class, with the session
// routes mounted or not. Without the session routes only the user-token
// audience check stands between the value and a running minter.
func TestNoAuthServiceConfigurationMintsAnACRAudience(t *testing.T) {
	for _, audience := range []string{"acr-api", "acr-mcp"} {
		mounted := eddsaEnv()
		mounted[EnvAccessTokenAudience] = audience
		unmounted := eddsaEnv()
		delete(unmounted, EnvAccessTokenSecret)
		delete(unmounted, EnvSessionSuccessorKey)
		unmounted[EnvAccessTokenAudience] = audience
		for name, env := range map[string]map[string]string{"session routes": mounted, "no session routes": unmounted} {
			_, err := Load(Spec{LookupEnv: lookupFrom(env)})
			if err == nil || !strings.Contains(err.Error(), EnvAccessTokenAudience) {
				t.Errorf("%s, audience %s: Load err %v; want a refusal naming %s", name, audience, err, EnvAccessTokenAudience)
			}
		}
	}
}
