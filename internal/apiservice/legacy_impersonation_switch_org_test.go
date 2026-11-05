package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// endableSessionStore is legacySessionStore whose session row can end.
type endableSessionStore struct {
	legacySessionStore
	ended atomic.Bool
}

func (s *endableSessionStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if s.ended.Load() {
		return nil, nil
	}
	return s.legacySessionStore.ActiveImpersonation(ctx, admin)
}

const switchOrgRefusal = `{"detail":"Not allowed in an impersonation session"}`

// TestSwitchOrgRefusesTheImpersonatingAdminBearer: POST /api/v1/auth/switch-org
// refuses a caller whose go-api bearer has a live impersonation row with 403,
// before auth-service is called, in every flag state; after the row ends the
// same caller is served (forwarded when the sessions flag is on).
func TestSwitchOrgRefusesTheImpersonatingAdminBearer(t *testing.T) {
	states := []struct {
		name                 string
		sessions, impersonat bool
	}{
		{"sessions on, impersonation on", true, true},
		{"sessions on, impersonation off", true, false},
		{"sessions off", false, false},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			key := strings.Repeat("fixture-key-", 3)
			verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
			if err != nil {
				t.Fatal(err)
			}
			signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
			if err != nil {
				t.Fatal(err)
			}
			store := &endableSessionStore{legacySessionStore: legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}}
			auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			rows, err := goAPIScopeRows()
			if err != nil {
				t.Fatal(err)
			}
			stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTeapot)
				_, _ = w.Write([]byte(ownerUpstreamBody))
			})
			upstream, _ := url.Parse(stub.URL)
			proxy := AuthProxyConfig{Upstream: upstream, Impersonation: state.impersonat}
			if state.sessions {
				proxy.On = map[AuthAggregate]bool{AuthSessions: true}
			}
			protected, err := newProtection(apiTestConfig(), nil, false, proxy, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			scope := policy.NewScope(auth, quietLogger())
			scope.DeclaredRows(rows)
			scope.RefuseLegacyImpersonation(protected.scope.RefusesLegacyImpersonation())
			pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer,
				AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
			server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			access, err := signer.Access(edgetoken.AccessClaims{UserID: store.admin.String(), Email: "admin@example.com", OrgID: uuid.NewString(),
				Role: "member", IsSuperuser: true, TokenVersion: 1}, time.Now().UTC(), uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			serve := func() *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(switchOrgBody))
				request.RemoteAddr = "192.0.2.10:4711"
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+access)
				recorder := httptest.NewRecorder()
				server.Handler().ServeHTTP(recorder, request)
				return recorder
			}

			refused := serve()
			if refused.Code != http.StatusForbidden || refused.Body.String() != switchOrgRefusal {
				t.Errorf("legacy caller with a live row: %d %s, want 403 %s", refused.Code, refused.Body.String(), switchOrgRefusal)
			}
			if got := len(stub.requests()); got != 0 {
				t.Errorf("auth-service saw %d calls for the refused caller, want 0", got)
			}

			store.ended.Store(true)
			served := serve()
			if served.Body.String() == switchOrgRefusal {
				t.Errorf("control: the caller whose row ended was refused: %d %s", served.Code, served.Body.String())
			}
			wantCalls := 0
			if state.sessions {
				wantCalls = 1
				if served.Code != http.StatusTeapot || served.Body.String() != ownerUpstreamBody {
					t.Errorf("control: %d %s, want auth-service's 418", served.Code, served.Body.String())
				}
			}
			if got := len(stub.requests()); got != wantCalls {
				t.Errorf("control: auth-service saw %d calls, want %d", got, wantCalls)
			}
		})
	}
}
