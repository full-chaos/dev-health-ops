package authconfig

import (
	"encoding/base64"
	"strings"
	"testing"
)

// The HS256 key and its claims are retired: auth-service verifies no HS256 token, so these names are read by no
// setting. They stay literals here because no constant names them any more.
const (
	retiredHS256Key         = "AUTH_JWT_SECRET_KEY"
	retiredHS256Issuer      = "AUTH_JWT_ISSUER"
	retiredHS256Audience    = "AUTH_LEGACY_JWT_AUDIENCE"
	retiredKeyRateLimitKey  = "retired-hs256-key-rate-limit-key-0123456789"
	retiredKeyLeftoverValue = "short"
)

var retiredKeySuccessorKey = base64.RawURLEncoding.EncodeToString([]byte("retired-hs256-key-successor-0032"))

// retiredKeyEnv is the eddsa session environment with the successor key and the rate-limit key, and no HS256 key.
func retiredKeyEnv() map[string]string {
	env := baseEnv()
	env[EnvValkeyURI] = testValkeyURI
	env[EnvAccessTokenClass] = AccessClassEdDSA
	env[EnvAccessTokenAudience] = "dev-health-api"
	env[EnvSessionSuccessorKey] = retiredKeySuccessorKey
	env[EnvRateLimitKey] = retiredKeyRateLimitKey
	return env
}

// A leftover HS256 key in the environment never stops the start and never turns a session route on or off (product
// rule 3, D6465): any value, short, alone, or equal to the rate-limit key, loads as if unset.
func TestTheRetiredHS256KeyIsIgnoredAtLoad(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"short value beside the session secrets": func() map[string]string {
			env := retiredKeyEnv()
			env[retiredHS256Key] = retiredKeyLeftoverValue
			return env
		}(),
		"equal to the rate-limit key": func() map[string]string {
			env := retiredKeyEnv()
			env[retiredHS256Key] = retiredKeyRateLimitKey
			return env
		}(),
	} {
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Errorf("%s: refused at load: %v", name, err)
			continue
		}
		if !cfg.Session.Enabled || cfg.Session.RateLimitKey.Reveal() != retiredKeyRateLimitKey {
			t.Errorf("%s: sessions enabled %v, rate-limit key from %s %v; want both", name, cfg.Session.Enabled, EnvRateLimitKey,
				cfg.Session.RateLimitKey.Reveal() == retiredKeyRateLimitKey)
		}
	}
}

// The HS256 key alone turns nothing on: without the successor key the session routes stay off and the start is not
// refused.
func TestTheRetiredHS256KeyAloneLoadsWithTheSessionRoutesOff(t *testing.T) {
	env := baseEnv()
	env[retiredHS256Key] = strings.Repeat("leftover-hs256-", 3)
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("the HS256 key alone is refused at load: %v", err)
	}
	if cfg.Session.Enabled {
		t.Fatal("the HS256 key alone turned the session routes on")
	}
}

// AUTH_RATE_LIMIT_KEY is optional (product rule 3): with the session routes on and no rate-limit key the start is not
// refused, whatever the retired HS256 key holds; the limiter then keys from the successor key (authapi.NewSessionLimits).
func TestTheSessionRoutesStartWithoutTheRateLimitKey(t *testing.T) {
	for name, hs256 := range map[string]string{"no HS256 key": "", "a leftover HS256 key": strings.Repeat("leftover-hs256-", 3)} {
		env := retiredKeyEnv()
		delete(env, EnvRateLimitKey)
		if hs256 != "" {
			env[retiredHS256Key] = hs256
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Errorf("%s: the session routes without %s are refused at load: %v", name, EnvRateLimitKey, err)
			continue
		}
		if !cfg.Session.Enabled || cfg.Session.RateLimitKey.Configured() || cfg.Session.SuccessorKey.Reveal() != retiredKeySuccessorKey {
			t.Errorf("%s: sessions enabled %v, rate-limit key configured %v; want sessions on, no rate-limit key, the successor key",
				name, cfg.Session.Enabled, cfg.Session.RateLimitKey.Configured())
		}
	}
}

// The HS256 claim settings are read by nothing: a set-but-empty or padded value loads.
func TestTheRetiredHS256ClaimSettingsAreIgnoredAtLoad(t *testing.T) {
	for _, name := range []string{retiredHS256Issuer, retiredHS256Audience} {
		for _, value := range []string{"", " padded "} {
			env := retiredKeyEnv()
			env[name] = value
			if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
				t.Errorf("%s=%q is refused at load: %v", name, value, err)
			}
		}
	}
}
