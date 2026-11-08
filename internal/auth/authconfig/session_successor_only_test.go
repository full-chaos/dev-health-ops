package authconfig

import (
	"strings"
	"testing"
)

const testRateLimitKey = "session-config-rate-limit-key-0123456789"

func successorOnlyEnv(class string) map[string]string {
	env := eddsaEnv()
	delete(env, EnvAccessTokenSecret)
	env[EnvAccessTokenClass] = class
	env[EnvRateLimitKey] = testRateLimitKey
	return env
}

func TestEdDSASessionsNeedOnlyTheSuccessorKey(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(successorOnlyEnv(AccessClassEdDSA))})
	if err != nil {
		t.Fatalf("eddsa with the successor key and no %s is refused: %v", EnvAccessTokenSecret, err)
	}
	if !cfg.Session.Enabled || cfg.Session.AccessSecret.Configured() || cfg.Session.SuccessorKey.Reveal() != testSuccessorKey {
		t.Fatalf("enabled %v, access secret configured %v; want sessions on with no access secret",
			cfg.Session.Enabled, cfg.Session.AccessSecret.Configured())
	}
	if cfg.Session.RateLimitKey.Reveal() != testRateLimitKey {
		t.Fatal("the rate-limit key did not resolve")
	}
}

func TestEdDSASessionsWithoutTheAccessSecretNeedTheRateLimitKey(t *testing.T) {
	env := successorOnlyEnv(AccessClassEdDSA)
	delete(env, EnvRateLimitKey)
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvRateLimitKey) {
		t.Fatalf("got %v, want an error naming %s", err, EnvRateLimitKey)
	}
	if strings.Contains(err.Error(), testSuccessorKey) {
		t.Fatal("the error quotes a secret")
	}
}

func TestEdDSAAccessSecretAloneIsRefused(t *testing.T) {
	env := eddsaEnv()
	delete(env, EnvSessionSuccessorKey)
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvSessionSuccessorKey) {
		t.Fatalf("got %v, want an error naming %s", err, EnvSessionSuccessorKey)
	}
}

func TestHS256CompatSessionsStillNeedBothSecrets(t *testing.T) {
	_, err := Load(Spec{LookupEnv: lookupFrom(successorOnlyEnv(AccessClassHS256Compat))})
	if err == nil || !strings.Contains(err.Error(), EnvAccessTokenSecret) {
		t.Fatalf("got %v, want an error naming %s", err, EnvAccessTokenSecret)
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
		delete(env, EnvAccessTokenSecret)
		delete(env, EnvSessionSuccessorKey)
		set(env)
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), "needs the session secrets ("+EnvSessionSuccessorKey+")") {
			t.Errorf("%s: got %v, want it to name %s only", name, err, EnvSessionSuccessorKey)
		}
	}
}

func TestEdDSAAccessSecretSetStillMustDifferFromTheRateLimitKey(t *testing.T) {
	env := eddsaEnv()
	env[EnvRateLimitKey] = testAccessSecret
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvRateLimitKey) || strings.Contains(err.Error(), testAccessSecret) {
		t.Fatalf("got %v, want an error naming %s, not its value", err, EnvRateLimitKey)
	}
}
