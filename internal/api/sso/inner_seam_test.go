package sso

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Inner names exactly the five sign-in routes, each with its own full
// pattern; the admin routes never reach it.
func TestInnerSeamNamesTheSignInRoutesWithTheirOwnPatterns(t *testing.T) {
	auth := &policy.Authenticator{}
	called := map[string]int{}
	routes := Routes(Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(auth, slog.New(slog.DiscardHandler)), Logger: slog.New(slog.DiscardHandler),
		Inner: func(pattern string, local http.Handler) http.Handler {
			if local == nil {
				t.Errorf("Inner got no local handler for %s", pattern)
			}
			called[pattern]++
			return local
		}})
	want := []string{
		"/api/v1/auth/saml/{provider_id}/metadata", "/api/v1/auth/saml/{provider_id}/initiate", "/api/v1/auth/saml/{provider_id}/acs",
		"/api/v1/auth/oidc/{provider_id}/authorize", "/api/v1/auth/oidc/{provider_id}/callback",
	}
	if len(called) != len(want) {
		t.Fatalf("Inner named %v, want %v", called, want)
	}
	mounted := map[string]bool{}
	for _, route := range routes {
		mounted[route.Pattern] = true
	}
	for _, pattern := range want {
		if called[pattern] != 1 {
			t.Errorf("%s: Inner called %d times, want 1", pattern, called[pattern])
		}
		if !mounted[pattern] {
			t.Errorf("%s: named to Inner but not mounted", pattern)
		}
	}
}
