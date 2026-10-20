package authconfig

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const testAccessSecret = "session-config-test-key-0123456789abcdef"

var testSuccessorKey = base64.RawURLEncoding.EncodeToString([]byte("session-config-successor-key-032"))

func sessionEnv(access, successor string) map[string]string {
	env := baseEnv()
	if access != "" {
		env[EnvAccessTokenSecret] = access
	}
	if successor != "" {
		env[EnvSessionSuccessorKey] = successor
	}
	return env
}

func TestSessionRoutesAreOffWithoutTheirSecrets(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Session.Enabled {
		t.Fatal("session routes enabled with no secret set")
	}
	if cfg.Session.AccessIssuer != "dev-health-ops" || cfg.Session.AccessAudience != "dev-health-api" {
		t.Fatalf("iss/aud defaults = %q/%q, want go-api's dev-health-ops/dev-health-api",
			cfg.Session.AccessIssuer, cfg.Session.AccessAudience)
	}
}

func TestSessionSecretsLoadTogetherAndNeverReachTheStartupLine(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(sessionEnv(testAccessSecret, testSuccessorKey))})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Session.Enabled || cfg.Session.AccessSecret.Reveal() != testAccessSecret ||
		cfg.Session.SuccessorKey.Reveal() != testSuccessorKey {
		t.Fatal("session secrets did not resolve")
	}
	rendered := fmt.Sprint(cfg) + fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...))
	for _, secret := range []string{testAccessSecret, testSuccessorKey} {
		if strings.Contains(rendered, secret) {
			t.Fatal("a session secret reached a printed form of the configuration")
		}
	}
}

func TestSessionSecretsAreRefusedAloneOrMalformed(t *testing.T) {
	short := strings.Repeat("k", 31)
	cases := map[string]map[string]string{
		"access secret alone":      sessionEnv(testAccessSecret, ""),
		"successor key alone":      sessionEnv("", testSuccessorKey),
		"access secret too short":  sessionEnv(short, testSuccessorKey),
		"successor key not base64": sessionEnv(testAccessSecret, "!!not-base64url!!"),
		"successor key 31 bytes":   sessionEnv(testAccessSecret, base64.RawURLEncoding.EncodeToString(make([]byte, 31))),
		"successor key padded":     sessionEnv(testAccessSecret, base64.URLEncoding.EncodeToString(make([]byte, 32))),
	}
	for name, env := range cases {
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil {
			t.Errorf("%s: Load accepted it", name)
			continue
		}
		for _, secret := range []string{testAccessSecret, testSuccessorKey, short} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: the error quotes a secret", name)
			}
		}
	}
}

func TestSessionSecretsAndTheirFilesAreMutuallyExclusive(t *testing.T) {
	env := sessionEnv(testAccessSecret, testSuccessorKey)
	env[EnvAccessTokenSecret+"_FILE"] = "/run/secrets/jwt"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal("both AUTH_JWT_SECRET_KEY and its _FILE were accepted")
	}
}
