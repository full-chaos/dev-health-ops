package authconfig

import (
	"strings"
	"testing"
	"time"
)

// AUTH_ACCESS_TOKEN_LIFETIME is the eddsa access token's exp - iat: unset is the 60-minute ceiling (EdDSA ships at
// 60 minutes, ADR-03), and a whole number of minutes from 5 to 60 is accepted, so the 10-minute target is one setting.
func TestAccessTokenLifetimeIsWholeMinutesFromFiveToSixty(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(eddsaEnv())})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Session.AccessLifetime != 60*time.Minute {
		t.Fatalf("unset lifetime %s, want 60m", cfg.Session.AccessLifetime)
	}
	for value, want := range map[string]time.Duration{"10m": 10 * time.Minute, "5m": 5 * time.Minute, "60m": time.Hour, "1h": time.Hour} {
		env := eddsaEnv()
		env[EnvAccessTokenLifetime] = value
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil || cfg.Session.AccessLifetime != want {
			t.Errorf("%s: lifetime %s, err %v; want %s", value, cfg.Session.AccessLifetime, err, want)
		}
	}
	for _, value := range []string{"4m", "61m", "90s", "10m30s", "0", "-10m", "abc", " 10m", "10m ", ""} {
		env := eddsaEnv()
		env[EnvAccessTokenLifetime] = value
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvAccessTokenLifetime) {
			t.Errorf("%q: Load err %v; want a refusal naming %s", value, err, EnvAccessTokenLifetime)
		}
	}
}

// Under hs256_compat the access token is go-api's HS256 token with go-api's 60 minutes: a lifetime setting there is
// not used, and it does not stop the service from starting (D6465).
func TestAccessTokenLifetimeIsIgnoredUnderHS256Compat(t *testing.T) {
	env := sessionEnv(testAccessSecret, testSuccessorKey)
	env[EnvAccessTokenLifetime] = "10m"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("hs256_compat with a lifetime set: %v", err)
	}
	if cfg.Session.AccessLifetime != 60*time.Minute {
		t.Fatalf("hs256_compat lifetime %s, want go-api's 60m", cfg.Session.AccessLifetime)
	}
}
