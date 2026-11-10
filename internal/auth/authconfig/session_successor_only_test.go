package authconfig

import (
	"strings"
	"testing"
)

const testRateLimitKey = "session-config-rate-limit-key-0123456789"

func successorOnlyEnv(class string) map[string]string {
	env := eddsaEnv()
	env[EnvAccessTokenClass] = class
	env[EnvRateLimitKey] = testRateLimitKey
	return env
}

func TestEdDSASessionsNeedOnlyTheSuccessorKey(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(successorOnlyEnv(AccessClassEdDSA))})
	if err != nil {
		t.Fatalf("eddsa with the successor key and the rate-limit key is refused: %v", err)
	}
	if !cfg.Session.Enabled || cfg.Session.SuccessorKey.Reveal() != testSuccessorKey {
		t.Fatalf("enabled %v; want sessions on", cfg.Session.Enabled)
	}
	if cfg.Session.RateLimitKey.Reveal() != testRateLimitKey {
		t.Fatal("the rate-limit key did not resolve")
	}
}

func TestEdDSASessionRefusalsNameTheSuccessorKeyOnly(t *testing.T) {
	for name, set := range map[string]func(map[string]string){
		"lockout key":      func(env map[string]string) { env[EnvLoginLockoutKey] = testSuccessorKey },
		"social link":      func(env map[string]string) { env[EnvSocialLinkRoutes] = "true" },
		"rate-limit key":   func(env map[string]string) { env[EnvRateLimitKey] = testRateLimitKey },
		"membership route": func(env map[string]string) { env[EnvMembershipRoutes] = "true" },
	} {
		env := eddsaEnv()
		delete(env, EnvRateLimitKey)
		delete(env, EnvSessionSuccessorKey)
		set(env)
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), "needs the session secrets ("+EnvSessionSuccessorKey+")") {
			t.Errorf("%s: got %v, want it to name %s only", name, err, EnvSessionSuccessorKey)
		}
	}
}
