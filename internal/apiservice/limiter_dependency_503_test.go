package apiservice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// failingLimitStore fails every counter call of the limit named failID with err, and admits all others.
type failingLimitStore struct {
	failID string
	err    error
}

func (s failingLimitStore) Increment(_ context.Context, hit httpapi.Hit) (int64, error) {
	if hit.Limit.ID == s.failID {
		return 0, s.err
	}
	return 1, nil
}

func (s failingLimitStore) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	return 0, nil
}
func (failingLimitStore) Backend() string { return "redis" }

type limiterRoute struct {
	name, method, path, body, limitID string
	authenticated                     bool
}

func limiterRoutes() []limiterRoute {
	const mail = `{"email":"a@example.com"}`
	return []limiterRoute{
		{"login", http.MethodPost, "/api/v1/auth/login", `{"email":"a@example.com","password":"pw"}`, "auth_login_ip", false},
		{"refresh", http.MethodPost, "/api/v1/auth/refresh", `{"refresh_token":"t"}`, "auth_refresh", false},
		{"validate", http.MethodPost, "/api/v1/auth/validate", `{"token":"t"}`, "auth_validate", false},
		{"register", http.MethodPost, "/api/v1/auth/register", `{"email":"new@example.com","password":"password-1234"}`, "auth_register", false},
		{"verify", http.MethodGet, "/api/v1/auth/verify?token=t", "", "auth_verify", false},
		{"resend", http.MethodPost, "/api/v1/auth/resend-verification", mail, "auth_resend_verification", false},
		{"resend address", http.MethodPost, "/api/v1/auth/resend-verification", mail, "auth_resend_verification_addr", false},
		{"forgot", http.MethodPost, "/api/v1/auth/forgot-password", mail, "auth_forgot_password", false},
		{"forgot address", http.MethodPost, "/api/v1/auth/forgot-password", mail, "auth_forgot_password_addr", false},
		{"change password", http.MethodPost, "/api/v1/auth/change-password", `{"current_password":"old-password","new_password":"new-password-1"}`, "auth_change_password", true},
		{"admin invite", http.MethodPost, "/api/v1/admin/orgs/" + probeOrg.String() + "/invites", `{"email":"invitee@example.com"}`, "admin_org_invite", true},
		{"admin password", http.MethodPost, "/api/v1/admin/users/" + probeMember.String() + "/password", `{"admin_password":"admin-password","password":"new-password-1"}`, "admin_password", true},
	}
}

// A limiter whose counter store cannot answer is a dependency failure the client may retry (503, Retry-After);
// any other store error stays the unhandled-error 500.
func TestLimiterRoutesAnswerTheStoreFailureByItsKind(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://limiter@127.0.0.1:1/limiter?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, newProbeStore(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	token := probeHS256(t, probeKey, jwt.MapClaims{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3, "email": "user@example.test"})

	for _, route := range limiterRoutes() {
		for _, c := range []struct {
			kind   string
			err    error
			status int
		}{
			{"typed dependency failure", fmt.Errorf("rate limit script: %w", valkey.ErrClosing), http.StatusServiceUnavailable},
			{"other store error", errors.New("counter store refused"), http.StatusInternalServerError},
		} {
			t.Run(route.name+"/"+c.kind, func(t *testing.T) {
				deps := Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer,
					Limits: failingLimitStore{failID: route.limitID, err: c.err}}
				cfg := apiTestConfig()
				cfg.CORSAllowedOrigins = []string{"https://app.example"}
				server, err := NewServer(cfg, quietLogger(), Routes(deps, quietLogger()))
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Origin", "https://app.example")
				if route.authenticated {
					request.Header.Set("Authorization", "Bearer "+token)
				}
				got := serve(server.Handler(), request)
				if got.Code != c.status {
					t.Fatalf("status %d, want %d: %s", got.Code, c.status, got.Body.String())
				}
				retry := got.Header().Get("Retry-After")
				if (c.status == http.StatusServiceUnavailable) != (retry != "") {
					t.Fatalf("Retry-After %q on status %d", retry, got.Code)
				}
			})
		}
	}
}
