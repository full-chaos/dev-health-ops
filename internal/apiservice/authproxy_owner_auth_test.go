package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// countingScopeStore is scopeStore that counts every read go-api's authenticator makes.
type countingScopeStore struct {
	scopeStore
	reads *atomic.Int64
}

func (s countingScopeStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.reads.Add(1)
	return s.scopeStore.UserState(ctx, id)
}

func (s countingScopeStore) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	s.reads.Add(1)
	return s.scopeStore.Membership(ctx, user, org)
}

func (s countingScopeStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	s.reads.Add(1)
	return s.scopeStore.OrgMembership(ctx, user, org)
}

func (s countingScopeStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.reads.Add(1)
	return s.scopeStore.ActiveImpersonation(ctx, admin)
}

type ownerFixture struct {
	handler http.Handler
	reads   *atomic.Int64
	store   scopeStore
}

// newOwnerFixture is the api listener as configureWith builds it: go-api's routes behind the proxy, the scope layers,
// and the scope exemption of cfg.
func newOwnerFixture(t *testing.T, cfg AuthProxyConfig) *ownerFixture {
	t.Helper()
	verifier, err := edgetoken.New(proxyTestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(proxyTestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	reads := &atomic.Int64{}
	store := scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}
	auth, err := policy.NewAuthenticator(verifier, countingScopeStore{scopeStore: store, reads: reads}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer,
		AuthProxy: cfg, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
	scope := policy.NewScope(auth, quietLogger())
	server, err := newServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{proxyOrigin}}, quietLogger(), routes,
		nil, cfg.ScopeExemptRoutes(), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return &ownerFixture{handler: server.Handler(), reads: reads, store: store}
}

func (f *ownerFixture) changePassword(t *testing.T, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/change-password", strings.NewReader(body))
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Origin", proxyOrigin)
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

const ownerUpstreamBody = `{"detail":"from auth-service"}`

// A bearer addressed to auth-service (aud auth-service): go-api has no key that verifies it and must not try.
const sessionRouteBearer = "header-segment.payload-segment.EXAMPLESIGNATURE"

// With the credentials aggregate on, change-password is auth-service's route: go-api forwards it unread whatever the
// bearer (none, go-api's own, one addressed to auth-service, a broken one) and whatever the body, answers with
// auth-service's answer, and reads nothing itself (no verifier, no store, no limiter).
func TestChangePasswordIsForwardedUnreadUnderTheCredentialsFlag(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(ownerUpstreamBody))
	})
	f := newOwnerFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	valid := `{"current_password": "old", "new_password": "long-enough-1"}`
	for name, c := range map[string]struct{ bearer, body string }{
		"no bearer":            {"", valid},
		"go-api access token":  {proxyBearer(t), valid},
		"session-route bearer": {sessionRouteBearer, valid},
		"broken bearer":        {"not-a-token", valid},
		"body not json":        {sessionRouteBearer, `{"current_password": `},
		"superuser token":      {ownerSuperuserBearer(t, f.store.admin), valid},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(stub.requests())
			recorder := f.changePassword(t, c.bearer, c.body)
			if recorder.Code != http.StatusTeapot || recorder.Body.String() != ownerUpstreamBody {
				t.Fatalf("answer %d %s, want auth-service's 418", recorder.Code, recorder.Body.String())
			}
			if len(stub.requests()) != before+1 {
				t.Fatal("the request did not reach auth-service")
			}
			seen := stub.last(t)
			if seen.body != c.body || seen.path != "/api/v1/auth/change-password" {
				t.Fatalf("forwarded %s %q, want the client's bytes", seen.path, seen.body)
			}
			want := ""
			if c.bearer != "" {
				want = "Bearer " + c.bearer
			}
			if got := seen.header.Get("Authorization"); got != want {
				t.Fatalf("forwarded Authorization %q, want %q", got, want)
			}
			if recorder.Header().Get("X-Impersonating") != "" {
				t.Fatal("go-api's impersonation layer ran on a route it forwards unread")
			}
		})
	}
	if got := f.reads.Load(); got != 0 {
		t.Fatalf("go-api made %d store reads for a route auth-service authenticates", got)
	}
}

// go-api's change-password limiter keys on the guard's user; without the guard every caller would share one bucket,
// so under the flag go-api limits nothing here and auth-service limits per principal.
func TestChangePasswordHasNoGoAPILimiterUnderTheCredentialsFlag(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	f := newOwnerFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for i := 0; i < 30; i++ {
		if recorder := f.changePassword(t, sessionRouteBearer, `{"current_password": "old", "new_password": "long-enough-1"}`); recorder.Code != http.StatusOK {
			t.Fatalf("request %d = %d %s: go-api still limits the forwarded route", i+1, recorder.Code, recorder.Body.String())
		}
	}
	if got := len(stub.requests()); got != 30 {
		t.Fatalf("auth-service saw %d requests, want 30", got)
	}
}

// With the credentials aggregate off nothing changes: go-api's guard answers a missing or foreign bearer itself, the
// scope layers run, and nothing is forwarded. This is also the mutant cell for an exemption that ignores the flag.
func TestChangePasswordKeepsGoAPIsChainWithTheCredentialsFlagOff(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	env := allOn(stub.URL)
	env[AuthProxyFlagEnv(AuthCredentials)] = "false"
	cfg := mustProxyConfig(t, env)
	if len(cfg.ScopeExemptRoutes()) != 0 {
		t.Fatalf("scope-exempt routes %v with the credentials aggregate off", cfg.ScopeExemptRoutes())
	}
	f := newOwnerFixture(t, cfg)
	for name, bearer := range map[string]string{"no bearer": "", "session-route bearer": sessionRouteBearer} {
		recorder := f.changePassword(t, bearer, `{"current_password": "old", "new_password": "long-enough-1"}`)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s: %d %s, want go-api's own 401", name, recorder.Code, recorder.Body.String())
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("auth-service saw %d requests with the flag off", len(seen))
	}
	// The scope layers still run for the route: a superuser's impersonation is stamped.
	recorder := f.changePassword(t, ownerSuperuserBearer(t, f.store.admin), `{"current_password": "old", "new_password": "long-enough-1"}`)
	if recorder.Header().Get("X-Impersonating") != "true" {
		t.Fatalf("flag off: the impersonation layer did not run (%d %v)", recorder.Code, recorder.Header())
	}
}

// The exemption is exact: only POST on the exact path, and only the owner-authenticated routes.
func TestScopeExemptRoutesAreExactAndFlagBound(t *testing.T) {
	cfg := mustProxyConfig(t, allOn("http://127.0.0.1:1"))
	exempt := cfg.ScopeExemptRoutes()
	if len(exempt) != 1 || !exempt["POST /api/v1/auth/change-password"] {
		t.Fatalf("scope-exempt routes %v, want only POST /api/v1/auth/change-password", exempt)
	}
	marked := false
	probe := markScopeExempt(exempt)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		marked, _ = r.Context().Value(signatureOnlyKey{}).(bool)
	}))
	for target, want := range map[string]bool{
		"POST /api/v1/auth/change-password":   true,
		"GET /api/v1/auth/change-password":    false,
		"POST /api/v1/auth/change-password/":  false,
		"POST /api/v1/auth/change%2Dpassword": false,
		"POST /api/v1/auth/me":                false,
	} {
		method, path, _ := strings.Cut(target, " ")
		probe.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
		if marked != want {
			t.Errorf("%s: exempt %v, want %v", target, marked, want)
		}
	}
}

// ownerSuperuserBearer is go-api's HS256 access token for the fixture's superuser, who holds an active impersonation.
func ownerSuperuserBearer(t *testing.T, admin uuid.UUID) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": admin.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": true, "iss": "dev-health-ops",
		"aud": "dev-health-api", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(proxyTestKey))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// go-api's own refusals with the credentials aggregate off, byte for byte. auth-service answers the same requests
// with the same bytes once go-api forwards the route (internal/auth/authapi/change_password_proxy_parity_test.go),
// so a client sees no change when the flag moves the refusal from one owner to the other.
func TestChangePasswordGoAPIRefusalsArePinned(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	env := allOn(stub.URL)
	env[AuthProxyFlagEnv(AuthCredentials)] = "false"
	f := newOwnerFixture(t, mustProxyConfig(t, env))
	for name, want := range map[string]struct {
		bearer, body string
	}{
		"no token":  {"", `{"detail":{"message":"Not authenticated"}}`},
		"bad token": {"not-a-token", `{"detail":{"message":"Invalid or expired token"}}`},
		"bad jws":   {"eyJhbGciOiJIUzI1NiJ9.e30.EXAMPLE", `{"detail":{"message":"Invalid or expired token"}}`},
	} {
		r := f.changePassword(t, want.bearer, `{"current_password": "old", "new_password": "long-enough-1"}`)
		if r.Code != http.StatusUnauthorized || r.Body.String() != want.body || r.Header().Get("WWW-Authenticate") != "Bearer" ||
			r.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s: %d %s %v", name, r.Code, r.Body.String(), r.Header())
		}
	}
}
