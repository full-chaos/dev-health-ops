package config

import (
	"strings"
	"testing"
)

// TestAPIIgnoresJWTSecretKey: the api reads no JWT_SECRET_KEY, so a value left in the environment, short or
// equal to a state key, never stops the load.
func TestAPIIgnoresJWTSecretKey(t *testing.T) {
	t.Parallel()

	state := strings.Repeat("s", 40)
	for name, extra := range map[string]map[string]string{
		"short":             {"JWT_SECRET_KEY": "short"},
		"sso equals jwt":    {"SSO_STATE_SECRET": state, "JWT_SECRET_KEY": state},
		"github equals jwt": {"GITHUB_APP_STATE_SECRET": state, "JWT_SECRET_KEY": state},
	} {
		values := map[string]string{"POSTGRES_URI": "postgresql://app:app@db.internal:5432/appdb"}
		for k, v := range extra {
			values[k] = v
		}
		if _, err := Load(Spec{Service: APIServiceName, LookupEnv: lookup(values)}); err != nil {
			t.Errorf("%s: %v, want the load to ignore JWT_SECRET_KEY", name, err)
		}
	}
}
