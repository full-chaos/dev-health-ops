package authconfig

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Under the eddsa class AUTH_JWT_AUDIENCE is the audience the service mints, so go-api's HS256 token needs its
// own audience: AUTH_LEGACY_JWT_AUDIENCE, go-api's default when unset.
func TestLegacyJWTAudienceDefaultsToGoAPIsAndTakesAnExactValue(t *testing.T) {
	hs256 := func(env map[string]string) {
		delete(env, EnvAccessTokenClass)
		delete(env, EnvTokenIssuerURL)
	}
	for _, c := range []struct {
		name string
		set  func(map[string]string)
		want string
	}{
		{"eddsa unset", func(map[string]string) {}, contracts.PlatformAPIAudience},
		{"eddsa set", func(env map[string]string) { env[EnvLegacyJWTAudience] = "go-api-custom" }, "go-api-custom"},
		{"hs256 unset", hs256, contracts.PlatformAPIAudience},
		{"hs256 set", func(env map[string]string) { hs256(env); env[EnvLegacyJWTAudience] = "go-api-custom" }, "go-api-custom"},
	} {
		env := eddsaEnv()
		c.set(env)
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if cfg.Session.LegacyAudience != c.want {
			t.Errorf("%s: LegacyAudience %q, want %q", c.name, cfg.Session.LegacyAudience, c.want)
		}
	}
}

func TestLegacyJWTAudienceSetButBlankRefusesToStart(t *testing.T) {
	for _, value := range []string{"", " ", "\t", "\n", " go-api-custom", "go-api-custom ", "go-api-custom\n"} {
		env := eddsaEnv()
		env[EnvLegacyJWTAudience] = value
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), EnvLegacyJWTAudience) {
			t.Errorf("%q: Load err %v; want a refusal naming %s", value, err, EnvLegacyJWTAudience)
		}
	}
}
