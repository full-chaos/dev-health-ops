package apiservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// endableSessionStore is legacySessionStore whose session row can end. A
// non-nil platform replaces the admin's platform actions in the users row.
type endableSessionStore struct {
	legacySessionStore
	ended    atomic.Bool
	platform *rule.Platform
}

func (s *endableSessionStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	state, found, err := s.legacySessionStore.UserState(ctx, id)
	if id == s.admin && s.platform != nil {
		state.Platform = s.platform
	}
	return state, found, err
}

// revokedImpersonate is the admin's platform set after admin.impersonate was
// revoked: another platform action is still held.
func revokedImpersonate() *rule.Platform {
	platform := rule.PlatformOf(rule.ActionAdminBilling)
	return &platform
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
// before auth-service is called, in every flag state, also after the caller's
// admin.impersonate was revoked (the refusal follows the live row, not the
// action); after the row ends the same caller is served (forwarded when the
// sessions flag is on).
func TestSwitchOrgRefusesTheImpersonatingAdminBearer(t *testing.T) {
	states := []struct {
		name                          string
		sessions, impersonat, revoked bool
	}{
		{"sessions on, impersonation on", true, true, false},
		{"sessions on, impersonation off", true, false, false},
		{"sessions off", false, false, false},
		{"admin.impersonate revoked, sessions on, impersonation on", true, true, true},
		{"admin.impersonate revoked, sessions on, impersonation off", true, false, true},
		{"admin.impersonate revoked, sessions off", false, false, true},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			tokens := edtokens.New(t)
			store := &endableSessionStore{legacySessionStore: legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}}
			if state.revoked {
				store.platform = revokedImpersonate()
			}
			auth := tokens.AuthenticatorOver(t, store, quietLogger())
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
			routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
				AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
			server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			access := tokens.MustMint(t, edtokens.Claims{UserID: store.admin, OrgID: uuid.NewString(), TokenVersion: 1})
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

// eddsaAdminStore is the probe store with a live session for the superuser
// probeAdminUser, whose go-api impersonation row can end.
type eddsaAdminStore struct {
	*probeStore
	ended atomic.Bool
}

func (s *eddsaAdminStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if s.ended.Load() {
		return nil, nil
	}
	return s.probeStore.ActiveImpersonation(ctx, admin)
}

// TestSwitchOrgRefusesTheImpersonatingEdDSABearer is the same cell for the
// admin's own routed auth-service user access token (EdDSA, aud
// dev-health-api: the shape newRoutedFixture and credential_class_matrix_test
// mint), which is the admin bearer under AUTH_PROXY_SESSIONS in production.
func TestSwitchOrgRefusesTheImpersonatingEdDSABearer(t *testing.T) {
	states := []struct {
		name                          string
		sessions, impersonat, revoked bool
	}{
		{"sessions on, impersonation on", true, true, false},
		{"sessions on, impersonation off", true, false, false},
		{"sessions off", false, false, false},
		{"admin.impersonate revoked, sessions on, impersonation on", true, true, true},
		{"admin.impersonate revoked, sessions on, impersonation off", true, false, true},
		{"admin.impersonate revoked, sessions off", false, false, true},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			adminSID := uuid.MustParse("00000000-0000-4000-8000-0000000000c9")
			probe := newProbeStore()
			probe.members[[2]uuid.UUID{probeAdminUser, probeOrg}] = "member"
			probe.sessions[adminSID] = policy.SessionRow{PrincipalID: probeAdminUser, LegacyTokenVersion: i64ptr(0)}
			if state.revoked {
				admin := probe.users[probeAdminUser]
				admin.state.Platform = revokedImpersonate()
				probe.users[probeAdminUser] = admin
			}
			store := &eddsaAdminStore{probeStore: probe}
			auth, err := policy.NewAuthenticator(store, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			minter := newEdDSAMinter(t)
			eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
			if err != nil {
				t.Fatal(err)
			}
			validator, err := accessverify.New(accessverify.Config{Rows: realRows(t), EdDSA: eddsa,
				Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(), Now: func() time.Time { return probeNow }})
			if err != nil {
				t.Fatal(err)
			}
			if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
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
			routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
				AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
			server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			bearer := minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(adminSID), "org_id": probeOrg.String()})
			serve := func() *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(switchOrgBody))
				request.RemoteAddr = "192.0.2.10:4711"
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+bearer)
				recorder := httptest.NewRecorder()
				server.Handler().ServeHTTP(recorder, request)
				return recorder
			}

			refused := serve()
			if refused.Code != http.StatusForbidden || refused.Body.String() != switchOrgRefusal {
				t.Errorf("EdDSA admin with a live row: %d %s, want 403 %s", refused.Code, refused.Body.String(), switchOrgRefusal)
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

// failingReadStore is legacySessionStore with a failing users-row read or session read.
type failingReadStore struct {
	legacySessionStore
	userErr, sessionErr error
}

func (s *failingReadStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	if s.userErr != nil {
		return policy.UserState{}, false, s.userErr
	}
	return s.legacySessionStore.UserState(ctx, id)
}

func (s *failingReadStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if s.sessionErr != nil {
		return nil, s.sessionErr
	}
	return s.legacySessionStore.ActiveImpersonation(ctx, admin)
}

const switchOrgUnavailable = `{"detail":{"message":"Database temporarily unavailable"}}`

// TestSwitchOrgAnswers503WhenACallerReadFails: with the sessions proxy on, any
// failed read in the refusal (the users row, the impersonation session) is a
// dependency failure: 503 with go-api's unavailable body, nothing forwarded.
func TestSwitchOrgAnswers503WhenACallerReadFails(t *testing.T) {
	cases := map[string]func(*failingReadStore){
		"users row read fails": func(s *failingReadStore) { s.userErr = errors.New("users read: connection reset") },
		"session read fails":   func(s *failingReadStore) { s.sessionErr = errors.New("session read: connection reset") },
	}
	for name, fail := range cases {
		t.Run(name, func(t *testing.T) {
			tokens := edtokens.New(t)
			store := &failingReadStore{legacySessionStore: legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}}
			fail(store)
			auth := tokens.AuthenticatorOver(t, store, quietLogger())
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
			proxy := AuthProxyConfig{Upstream: upstream, Impersonation: true, On: map[AuthAggregate]bool{AuthSessions: true}}
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
			routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
				AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
			server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			access := tokens.MustMint(t, edtokens.Claims{UserID: store.admin, OrgID: uuid.NewString(), TokenVersion: 1})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(switchOrgBody))
			request.RemoteAddr = "192.0.2.10:4711"
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+access)
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(recorder.Body.String()) != switchOrgUnavailable {
				t.Errorf("%s: %d %s, want 503 %s", name, recorder.Code, recorder.Body.String(), switchOrgUnavailable)
			}
			if got := len(stub.requests()); got != 0 {
				t.Errorf("auth-service saw %d calls, want 0", got)
			}
		})
	}
}

// TestSwitchOrgRetiredHS256Bearer pins main's answer to the admin's retired
// HS256 access token beside a live impersonation row: go-api verifies no
// HS256 token, so with the sessions flag on switch-org forwards it unread
// once and the caller gets auth-service's answer; with the flag off go-api's
// own route answers 401 and nothing is forwarded.
func TestSwitchOrgRetiredHS256Bearer(t *testing.T) {
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
			store := &endableSessionStore{legacySessionStore: legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}}
			auth := edtokens.New(t).AuthenticatorOver(t, store, quietLogger())
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
			routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
				AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
			server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			bearer := probeHS256(t, probeKey, map[string]any{"sub": store.admin.String(), "org_id": uuid.NewString(), "role": "member", "is_superuser": true, "tv": 1})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(switchOrgBody))
			request.RemoteAddr = "192.0.2.10:4711"
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+bearer)
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			seen := stub.requests()
			if state.sessions {
				if recorder.Code != http.StatusTeapot || recorder.Body.String() != ownerUpstreamBody {
					t.Errorf("sessions on: %d %s, want auth-service's 418", recorder.Code, recorder.Body.String())
				}
				if len(seen) != 1 || seen[0].header.Get("Authorization") != "Bearer "+bearer {
					t.Errorf("sessions on: auth-service saw %d calls, want 1 carrying the bearer unchanged", len(seen))
				}
			} else {
				if recorder.Code != http.StatusUnauthorized || strings.TrimSpace(recorder.Body.String()) != genericRefusal {
					t.Errorf("sessions off: %d %s, want 401 %s", recorder.Code, recorder.Body.String(), genericRefusal)
				}
				if len(seen) != 0 {
					t.Errorf("sessions off: auth-service saw %d calls, want 0", len(seen))
				}
			}
			t.Logf("%s: %d %s", state.name, recorder.Code, strings.TrimSpace(recorder.Body.String()))
		})
	}
}
