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

const testValkeyURI = "redis://auth-ratelimit:test-only-password@valkey:6379/1"

func sessionEnv(access, successor string) map[string]string {
	env := baseEnv()
	env[EnvValkeyURI] = testValkeyURI
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
	return sessionEnv(testAccessSecret, testSuccessorKey)
}

func TestTheLoginRouteNeedsTheLockoutKeyAndTheSessionSecrets(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(compatEnv())})
	if err != nil || cfg.Session.LoginEnabled {
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

func TestTheCompatSwitchIsGone(t *testing.T) {
	for _, option := range Options() {
		if option.Env == "AUTH_SERVICE_COMPAT_ROUTES" {
			t.Fatal("AUTH_SERVICE_COMPAT_ROUTES is still a registered option")
		}
	}
	env := compatEnv()
	env["AUTH_SERVICE_COMPAT_ROUTES"] = "false"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || !cfg.Session.Enabled {
		t.Fatalf("a leftover AUTH_SERVICE_COMPAT_ROUTES value changed the load: err %v", err)
	}
	if strings.Contains(fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...)), "compat_routes_enabled") {
		t.Fatal("the safe attributes still report the switch")
	}
}

func TestTheSocialLinkRoutesAreOffUnlessSetToTrueWithTheSessionSecrets(t *testing.T) {
	cases := map[string]struct {
		env     map[string]string
		enabled bool
		wantErr bool
	}{
		"unset":                 {env: compatEnv()},
		"blank":                 {env: withLink(compatEnv(), " ")},
		"false":                 {env: withLink(compatEnv(), "false")},
		"true":                  {env: withLink(compatEnv(), "true"), enabled: true},
		"true padded":           {env: withLink(compatEnv(), " true "), enabled: true},
		"1 is refused":          {env: withLink(compatEnv(), "1"), wantErr: true},
		"TRUE is refused":       {env: withLink(compatEnv(), "TRUE"), wantErr: true},
		"true without secrets":  {env: withLink(baseEnv(), "true"), wantErr: true},
		"false without secrets": {env: withLink(baseEnv(), "false")},
	}
	for name, tc := range cases {
		cfg, err := Load(Spec{LookupEnv: lookupFrom(tc.env)})
		if (err != nil) != tc.wantErr || cfg.Session.SocialLinkRoutes != tc.enabled {
			t.Errorf("%s: err %v, enabled %v; want err %v, enabled %v", name, err, cfg.Session.SocialLinkRoutes, tc.wantErr, tc.enabled)
		}
	}
}

func withLink(env map[string]string, value string) map[string]string {
	env[EnvSocialLinkRoutes] = value
	return env
}
