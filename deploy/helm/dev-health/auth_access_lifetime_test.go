package devhealth_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// authService.accessToken.lifetime renders AUTH_ACCESS_TOKEN_LIFETIME for the eddsa class only, and auth-service
// starts on it; unset renders nothing (the 60-minute default); under hs256_compat it is refused at render.
func TestAccessTokenLifetimeRendersForEdDSAOnly(t *testing.T) {
	eddsa := []string{"--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example"}
	docs := renderPhase(t, 2, append(eddsa, "--set", "authService.accessToken.lifetime=10m")...)
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment")
	}
	lookup := containerLookup(t, service, "auth-service")
	if value, set := lookup(authconfig.EnvAccessTokenLifetime); !set || value != "10m" {
		t.Fatalf("rendered %s=%q (set %v), want 10m", authconfig.EnvAccessTokenLifetime, value, set)
	}
	cfg, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: lookup})
	if err != nil || cfg.Session.AccessLifetime.Minutes() != 10 {
		t.Fatalf("auth-service on the rendered lifetime: %v, %s", err, cfg.Session.AccessLifetime)
	}

	plain := authDoc(renderPhase(t, 2, eddsa...), "Deployment", "a-dev-health-auth-service")
	if _, set := containerLookup(t, plain, "auth-service")(authconfig.EnvAccessTokenLifetime); set {
		t.Fatal("an unset lifetime rendered the variable")
	}

	for name, args := range map[string][]string{
		"hs256_compat with a lifetime": {"--set", "authService.accessToken.class=" + authconfig.AccessClassHS256Compat, "--set", "authService.accessToken.lifetime=10m"},
		"eddsa with 4m":                append(append([]string{}, eddsa...), "--set", "authService.accessToken.lifetime=4m"),
		"eddsa with 61m":               append(append([]string{}, eddsa...), "--set", "authService.accessToken.lifetime=61m"),
		"eddsa with 90s":               append(append([]string{}, eddsa...), "--set", "authService.accessToken.lifetime=90s"),
	} {
		_, out, err := renderAuth(t, phaseArgs(2, args...)...)
		if err == nil || !strings.Contains(err.Error()+out, "authService.accessToken.lifetime") {
			t.Errorf("%s: rendered or refused without naming the key: %v", name, err)
		}
	}
}
