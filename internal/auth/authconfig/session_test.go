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

var testLockoutKey = base64.RawURLEncoding.EncodeToString([]byte("login-config-lockout-key-0032-ab"))

func compatEnv() map[string]string {
	env := sessionEnv(testAccessSecret, testSuccessorKey)
	env[EnvCompatRoutes] = "true"
	return env
}

func TestTheLoginRouteNeedsTheLockoutKeyAndTheSessionSecrets(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(compatEnv())})
	if err != nil || cfg.Session.LoginEnabled || !cfg.Session.CompatRoutes {
		t.Fatalf("without the lockout key: err %v, login enabled %v", err, cfg.Session.LoginEnabled)
	}
	env := compatEnv()
	env[EnvLoginLockoutKey] = testLockoutKey
	cfg, err = Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || !cfg.Session.LoginEnabled || cfg.Session.LockoutKey.Reveal() != testLockoutKey {
		t.Fatalf("with the lockout key: err %v, login enabled %v", err, cfg.Session.LoginEnabled)
	}
	if strings.Contains(fmt.Sprint(cfg)+fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...)), testLockoutKey) {
		t.Fatal("the lockout key reached a printed form of the configuration")
	}
	for name, env := range map[string]map[string]string{
		"lockout key without the session secrets": {EnvLoginLockoutKey: testLockoutKey},
		"lockout key without the compat routes":   {EnvLoginLockoutKey: testLockoutKey, EnvCompatRoutes: "false"},
		"lockout key 31 bytes":                    {EnvLoginLockoutKey: base64.RawURLEncoding.EncodeToString(make([]byte, 31))},
		"lockout key padded":                      {EnvLoginLockoutKey: base64.URLEncoding.EncodeToString(make([]byte, 32))},
	} {
		full := baseEnv()
		if name != "lockout key without the session secrets" {
			full = compatEnv()
		}
		for k, v := range env {
			full[k] = v
		}
		_, err := Load(Spec{LookupEnv: lookupFrom(full)})
		if err == nil {
			t.Errorf("%s: Load accepted it", name)
		} else if strings.Contains(err.Error(), env[EnvLoginLockoutKey]) {
			t.Errorf("%s: the error quotes the key", name)
		}
	}
}

func TestTrustedProxiesRefuseAWildcardAndAreCountedOnTheStartupLine(t *testing.T) {
	env := baseEnv()
	env[EnvTrustedProxies] = "*"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal(`AUTH_SERVICE_TRUSTED_PROXIES="*" was accepted`)
	}
	env[EnvTrustedProxies] = "10.0.0.0/8,10.0.0.5"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...)), "trusted_proxy_count=2") {
		t.Fatal("the startup line does not count the trusted proxies")
	}
}

func TestSocialClientSecretsNeverReachTheStartupLine(t *testing.T) {
	clientSecret := strings.Repeat("s", 24)
	env := baseEnv()
	env["SOCIAL_GITHUB_CLIENT_ID"] = "gh-client"
	env["SOCIAL_GITHUB_CLIENT_SECRET"] = clientSecret
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Social["github"].ID != "gh-client" || cfg.Social["github"].Secret.Reveal() != clientSecret {
		t.Fatal("the github client did not resolve")
	}
	if strings.Contains(fmt.Sprint(cfg)+fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...)), clientSecret) {
		t.Fatal("a social client secret reached a printed form of the configuration")
	}
}

func TestTheCompatRoutesAreOffByDefaultAndNeedTheSessionSecrets(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(sessionEnv(testAccessSecret, testSuccessorKey))})
	if err != nil || cfg.Session.CompatRoutes {
		t.Fatalf("default: err %v, compat routes %v", err, cfg.Session.CompatRoutes)
	}
	env := baseEnv()
	env[EnvCompatRoutes] = "true"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal("compat routes without the session secrets were accepted")
	}
	env = compatEnv()
	env[EnvCompatRoutes] = "yes please"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal("a compat routes value that is not a boolean was accepted")
	}
}
