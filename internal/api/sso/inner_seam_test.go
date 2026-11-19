package sso

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Inner names exactly the sign-in routes, each with its own full pattern; the
// admin routes never reach it. The OAuth pair's two authorize routes (POST by
// id, GET by type) share auth-service's pattern, so it is named twice.
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
		"/api/v1/auth/oauth/{provider_id}/authorize", "/api/v1/auth/oauth/{provider_id}/callback",
	}
	wantCalls := map[string]int{"/api/v1/auth/oauth/{provider_id}/authorize": 2}
	pairPattern := "/api/v1/auth/oauth/{first}/{second}"
	if len(called) != len(want) {
		t.Fatalf("Inner named %v, want %v", called, want)
	}
	mounted := map[string]bool{}
	for _, route := range routes {
		mounted[route.Pattern] = true
	}
	for _, pattern := range want {
		want := max(wantCalls[pattern], 1)
		if called[pattern] != want {
			t.Errorf("%s: Inner called %d times, want %d", pattern, called[pattern], want)
		}
		if strings.HasPrefix(pattern, "/api/v1/auth/oauth/") {
			pattern = pairPattern
		}
		if !mounted[pattern] {
			t.Errorf("%s: named to Inner but not mounted", pattern)
		}
	}
}
