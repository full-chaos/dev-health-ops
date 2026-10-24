package authruntime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ssoEnvironment adds the federated sign-in settings to env. The values are
// made up for the tests here.
func ssoEnvironment(env map[string]string) map[string]string {
	env[authconfig.EnvSSOStateKey] = strings.Repeat("sso-state-test-key-", 2)
	env[authconfig.EnvAppBaseURL] = "https://app.example.test"
	env[authconfig.EnvSettingsEncryptionKey] = strings.Repeat("sso-fernet-test-key-", 2)
	return env
}

func ssoPatterns(t *testing.T, mutate func(map[string]string)) map[string]bool {
	t.Helper()
	env := ssoEnvironment(compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "sso-test",
	}))
	mutate(env)
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := sessionRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, nil, mustLimits(t))
	if err != nil {
		t.Fatalf("sessionRoutes: %v", err)
	}
	out := map[string]bool{}
	for _, route := range routes {
		for _, family := range []string{"/oidc/", "/oauth/", "/saml/"} {
			if strings.Contains(route.Pattern, family) {
				out[route.Method+" "+route.Pattern] = true
			}
		}
	}
	return out
}

func countFamily(patterns map[string]bool, family string) int {
	n := 0
	for pattern := range patterns {
		if strings.Contains(pattern, family) {
			n++
		}
	}
	return n
}

func TestSSORoutesMountPerProtocolAndOnlyWithEverySetting(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(map[string]string)
		oidc, saml int
		oauth      int
	}{
		{"all settings", func(map[string]string) {}, 2, 3, 3},
		{"no state key", func(env map[string]string) { delete(env, authconfig.EnvSSOStateKey) }, 0, 3, 0},
		{"no base url", func(env map[string]string) { delete(env, authconfig.EnvAppBaseURL) }, 0, 0, 0},
		{"no provider secret key", func(env map[string]string) { delete(env, authconfig.EnvSettingsEncryptionKey) }, 0, 0, 0},
		{"nothing", func(env map[string]string) {
			delete(env, authconfig.EnvSSOStateKey)
			delete(env, authconfig.EnvAppBaseURL)
			delete(env, authconfig.EnvSettingsEncryptionKey)
		}, 0, 0, 0},
	}
	for _, tc := range cases {
		patterns := ssoPatterns(t, tc.mutate)
		if got := countFamily(patterns, "/oidc/"); got != tc.oidc {
			t.Errorf("%s: %d oidc routes, want %d", tc.name, got, tc.oidc)
		}
		if got := countFamily(patterns, "/saml/"); got != tc.saml {
			t.Errorf("%s: %d saml routes, want %d", tc.name, got, tc.saml)
		}
		if got := countFamily(patterns, "/oauth/"); got != tc.oauth {
			t.Errorf("%s: %d oauth routes, want %d", tc.name, got, tc.oauth)
		}
	}
}

func TestAnUnusableBaseURLRefusesStartup(t *testing.T) {
	env := ssoEnvironment(compatEnvironment(map[string]string{authconfig.EnvDatabaseURI: "postgres://auth@127.0.0.1:1/devhealth"}))
	env[authconfig.EnvAppBaseURL] = "not a url"
	if _, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal("a base URL that is not absolute http(s) was accepted")
	} else if strings.Contains(err.Error(), "not a url") {
		t.Fatal("the configuration error quotes the supplied value")
	}
}

// TestFederatedSignInHasNoJITSeam: JIT provisioning stays off. Neither service
// is given a membership seam, so a sign-in that resolves no account is refused
// and writes nothing (the services' own refusal tests prove the refusal).
func TestFederatedSignInHasNoJITSeam(t *testing.T) {
	raw, err := os.ReadFile("sso.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		code, _, _ := strings.Cut(line, "//")
		if strings.Contains(code, "JIT:") || strings.Contains(code, "JIT =") {
			t.Fatalf("sso.go sets a membership seam: %q", strings.TrimSpace(line))
		}
	}
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(ssoEnvironment(map[string]string{
		authconfig.EnvDatabaseURI: "postgres://auth@127.0.0.1:1/devhealth", authconfig.EnvSigningKeyFile: "/nonexistent", authconfig.EnvSigningKeyID: "sso-test",
	}))})
	if err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Pool: pool, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	service, err := samlService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, identities)
	if err != nil {
		t.Fatal(err)
	}
	if service.JIT != nil {
		t.Fatal("the SAML service has a membership seam")
	}
}
