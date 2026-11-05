package acrcredentials

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

const unitKey = "acr-credentials-unit-test-key-acr-credentials-unit-key"

// users is the policy store: every user exists and is active; superusers and live impersonation sessions are
// planted per test. The route's own membership read goes to members, never to this store.
type users struct {
	mu         sync.Mutex
	superusers map[uuid.UUID]bool
	sessions   map[uuid.UUID]*policy.Impersonation
}

func (u *users) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return policy.UserState{IsActive: true, IsSuperuser: u.superusers[id]}, true, nil
}
func (*users) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "member", true, nil
}
func (*users) OrgMembership(context.Context, uuid.UUID, uuid.UUID) (string, bool, bool, error) {
	return "member", true, true, nil
}
func (u *users) ActiveImpersonation(_ context.Context, id uuid.UUID) (*policy.Impersonation, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.sessions[id], nil
}
func (*users) ReadsPlatformRoles() bool { return false }

type memberRow struct {
	role  string
	found bool
	err   error
}

type members struct {
	mu    sync.Mutex
	rows  map[[2]uuid.UUID]memberRow
	calls int
}

func (m *members) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	row := m.rows[[2]uuid.UUID{user, org}]
	return row.role, row.found, row.err
}

type revokeCall struct{ org, credential, actorType, actorID string }

type revoker struct {
	mu    sync.Mutex
	calls []revokeCall
	err   error
}

func (r *revoker) Revoke(_ context.Context, org, credential, actorType, actorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, revokeCall{org, credential, actorType, actorID})
	return r.err
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fixture struct {
	t        *testing.T
	store    *users
	members  *members
	revoker  *revoker
	auth     *policy.Authenticator
	handler  http.Handler
	scopeRun bool
}

func newFixture(t *testing.T, scopeRun bool) *fixture {
	t.Helper()
	verifier, err := edgetoken.New(unitKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, store: &users{superusers: map[uuid.UUID]bool{}, sessions: map[uuid.UUID]*policy.Impersonation{}},
		members: &members{rows: map[[2]uuid.UUID]memberRow{}}, revoker: &revoker{}, scopeRun: scopeRun}
	f.auth, err = policy.NewAuthenticator(verifier, f.store, quiet())
	if err != nil {
		t.Fatal(err)
	}
	f.build(Deps{Guard: policy.NewGuard(f.auth, quiet()), Members: f.members, Revoker: f.revoker, Logger: quiet()})
	return f
}

// build mounts Routes on a mux behind the api listener's request-wide Scope layers (Principal, OrgScope,
// Impersonation), as apiservice composes them.
func (f *fixture) build(deps Deps) {
	routes := Routes(deps)
	if len(routes) != 1 || routes[0].Method != http.MethodPost || routes[0].Pattern != RevokePattern || !routes[0].EmptyBody {
		f.t.Fatalf("routes = %+v", routes)
	}
	mux := http.NewServeMux()
	mux.Handle(routes[0].Method+" "+routes[0].Pattern, routes[0].Handler)
	var handler http.Handler = mux
	if f.scopeRun {
		layers := policy.NewScope(f.auth, quiet()).Layers()
		for i := len(layers) - 1; i >= 0; i-- {
			handler = layers[i](handler)
		}
	}
	f.handler = handler
}

func (f *fixture) sign(user uuid.UUID, org string, extra jwt.MapClaims) string {
	f.t.Helper()
	claims := jwt.MapClaims{
		"sub": user.String(), "type": "access", "org_id": org, "role": "member", "is_superuser": false,
		"iss": "dev-health-ops", "aud": "dev-health-api", "exp": time.Now().Add(time.Hour).Unix(), "tv": 0,
	}
	for k, v := range extra {
		claims[k] = v
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(unitKey))
	if err != nil {
		f.t.Fatal(err)
	}
	return token
}

func (f *fixture) post(token, credential string) *httptest.ResponseRecorder {
	f.t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/acr/credentials/"+credential+"/revoke", nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

func expect(t *testing.T, got *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if got.Code != status || got.Body.String() != body {
		t.Fatalf("answer %d %s, want %d %s", got.Code, got.Body.String(), status, body)
	}
}

const (
	credOne        = "cred_A-1"
	bodyNotFound   = `{"detail":"Credential not found"}`
	bodyConflict   = `{"detail":"Credential is already revoked"}`
	bodyUnavail    = `{"detail":"Credential service unavailable"}`
	bodyNotMember  = `{"detail":"Organization access denied"}`
	bodyImpersonal = `{"detail":"Not allowed in an impersonation session"}`
)

func TestTheActorIsTheLiveMembershipRowNeverTheRoleClaim(t *testing.T) {
	cases := []struct {
		name, claimRole, rowRole, wantActor string
	}{
		{"member", "member", "member", acrrevoke.ActorUser},
		{"admin row", "member", "admin", acrrevoke.ActorAdmin},
		{"owner row", "member", "owner", acrrevoke.ActorAdmin},
		{"admin claim, member row", "admin", "member", acrrevoke.ActorUser},
		{"owner claim, viewer row", "owner", "viewer", acrrevoke.ActorUser},
		{"admin claim, empty row role", "admin", "", acrrevoke.ActorUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			user, org := uuid.New(), uuid.New()
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: tc.rowRole, found: true}
			got := f.post(f.sign(user, org.String(), jwt.MapClaims{"role": tc.claimRole}), credOne)
			if got.Code != http.StatusNoContent || got.Body.Len() != 0 || got.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("answer %d %q %v", got.Code, got.Body.String(), got.Header())
			}
			want := revokeCall{org: org.String(), credential: credOne, actorType: tc.wantActor, actorID: user.String()}
			if len(f.revoker.calls) != 1 || f.revoker.calls[0] != want {
				t.Fatalf("revoke calls = %+v, want %+v", f.revoker.calls, want)
			}
		})
	}
}

func TestTheOrganizationAndActorAreCanonical(t *testing.T) {
	f := newFixture(t, true)
	user, org := uuid.New(), uuid.New()
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "member", found: true}
	got := f.post(f.sign(user, strings.ToUpper(org.String()), jwt.MapClaims{"sub": strings.ToUpper(user.String())}), credOne)
	if got.Code != http.StatusNoContent {
		t.Fatalf("answer %d %s", got.Code, got.Body.String())
	}
	if call := f.revoker.calls[0]; call.org != org.String() || call.actorID != user.String() {
		t.Fatalf("call = %+v", call)
	}
}

func TestRefusalsSendNoRevoke(t *testing.T) {
	user, org := uuid.New(), uuid.New()
	cases := []struct {
		name   string
		plant  func(*fixture) (token, credential string)
		status int
		body   string
		reads  int
	}{
		{"no token", func(*fixture) (string, string) { return "", credOne }, http.StatusUnauthorized, `{"detail":{"message":"Not authenticated"}}`, 0},
		{"no org claim", func(f *fixture) (string, string) { return f.sign(user, "", nil), credOne }, http.StatusForbidden, `{"detail":"Organization context required"}`, 0},
		{"org claim not a uuid", func(f *fixture) (string, string) { return f.sign(user, "not-a-uuid", nil), credOne }, http.StatusForbidden, bodyNotMember, 0},
		{"no membership row", func(f *fixture) (string, string) {
			return f.sign(user, org.String(), jwt.MapClaims{"role": "admin"}), credOne
		}, http.StatusForbidden, bodyNotMember, 1},
		{"membership read fails", func(f *fixture) (string, string) {
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{err: errors.New("db down")}
			return f.sign(user, org.String(), nil), credOne
		}, http.StatusServiceUnavailable, bodyUnavail, 1},
		{"malformed credential id", func(f *fixture) (string, string) {
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			return f.sign(user, org.String(), nil), "a.b"
		}, http.StatusNotFound, bodyNotFound, 0},
		{"overlong credential id", func(f *fixture) (string, string) {
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			return f.sign(user, org.String(), nil), strings.Repeat("a", 129)
		}, http.StatusNotFound, bodyNotFound, 0},
		{"impersonating superuser", func(f *fixture) (string, string) {
			f.store.superusers[user] = true
			f.store.sessions[user] = &policy.Impersonation{ID: uuid.New(), AdminUserID: user, TargetUserID: uuid.New(), TargetOrgID: org, TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			return f.sign(user, org.String(), jwt.MapClaims{"is_superuser": true}), credOne
		}, http.StatusForbidden, bodyImpersonal, 0},
		{"delegated bearer", func(f *fixture) (string, string) {
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			return f.sign(user, org.String(), jwt.MapClaims{"impersonating_user_id": uuid.NewString()}), credOne
		}, http.StatusForbidden, bodyImpersonal, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			token, credential := tc.plant(f)
			expect(t, f.post(token, credential), tc.status, tc.body)
			if len(f.revoker.calls) != 0 {
				t.Fatalf("revoke sent: %+v", f.revoker.calls)
			}
			if f.members.calls != tc.reads {
				t.Fatalf("%d membership reads, want %d", f.members.calls, tc.reads)
			}
		})
	}
}

// The guard refuses a delegated bearer while no row admits it; the handler refuses it too, so a row that admits
// delegated bearers never turns this route into one an impersonator can use.
func TestADelegatedBearerIsRefusedEvenWhenItsRowAdmitsIt(t *testing.T) {
	f := newFixture(t, true)
	f.auth.DelegatedRows(func(string) bool { return true })
	user, org := uuid.New(), uuid.New()
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
	expect(t, f.post(f.sign(user, org.String(), jwt.MapClaims{"impersonating_user_id": uuid.NewString()}), credOne), http.StatusForbidden, bodyImpersonal)
	if len(f.revoker.calls) != 0 || f.members.calls != 0 {
		t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
	}
}

// A request the api listener's Impersonation layer never decided is not served: nil impersonation there means
// "nobody looked", not "not impersonating".
func TestARequestNoScopeDecidedIsUnavailable(t *testing.T) {
	f := newFixture(t, false)
	user, org := uuid.New(), uuid.New()
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
	expect(t, f.post(f.sign(user, org.String(), nil), credOne), http.StatusServiceUnavailable, bodyUnavail)
	if len(f.revoker.calls) != 0 || f.members.calls != 0 {
		t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
	}
}

func TestEachRevokeAnswerMapsToOneStatus(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		body   string
	}{
		{"refused", acrrevoke.ErrNotFound, http.StatusNotFound, bodyNotFound},
		{"wrapped refusal", errors.Join(errors.New("x"), acrrevoke.ErrNotFound), http.StatusNotFound, bodyNotFound},
		{"already revoked", acrrevoke.ErrConflict, http.StatusConflict, bodyConflict},
		{"acr unavailable", &acrrevoke.Error{Kind: acrlisting.KindUnavailable, Status: 503}, http.StatusServiceUnavailable, bodyUnavail},
		{"binding refused", &acrrevoke.Error{Kind: acrlisting.KindForbidden, Status: 403}, http.StatusServiceUnavailable, bodyUnavail},
		{"invalid request", acrrevoke.ErrInvalidRequest, http.StatusServiceUnavailable, bodyUnavail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			f.revoker.err = tc.err
			user, org := uuid.New(), uuid.New()
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "member", found: true}
			got := f.post(f.sign(user, org.String(), nil), credOne)
			expect(t, got, tc.status, tc.body)
			if got.Header().Get("Cache-Control") != "no-store" || got.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("headers %v", got.Header())
			}
			if len(f.revoker.calls) != 1 {
				t.Fatalf("revoke calls %+v", f.revoker.calls)
			}
		})
	}
}

func TestMissingDependenciesAreUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		members  bool
		revoking bool
	}{{"no members", false, true}, {"no revoker", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			deps := Deps{Guard: policy.NewGuard(f.auth, quiet()), Logger: quiet()}
			if tc.members {
				deps.Members = f.members
			}
			if tc.revoking {
				deps.Revoker = f.revoker
			}
			f.build(deps)
			user, org := uuid.New(), uuid.New()
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			expect(t, f.post(f.sign(user, org.String(), nil), credOne), http.StatusServiceUnavailable, bodyUnavail)
			if len(f.revoker.calls) != 0 || f.members.calls != 0 {
				t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
			}
		})
	}
}

func TestTheRouteTakesNoOtherMethod(t *testing.T) {
	f := newFixture(t, true)
	user, org := uuid.New(), uuid.New()
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
	for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodPut} {
		request := httptest.NewRequest(method, "/api/v1/acr/credentials/"+credOne+"/revoke", nil)
		request.Header.Set("Authorization", "Bearer "+f.sign(user, org.String(), nil))
		recorder := httptest.NewRecorder()
		f.handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s answered %d", method, recorder.Code)
		}
	}
	if len(f.revoker.calls) != 0 {
		t.Fatalf("revoke %+v", f.revoker.calls)
	}
}

// A platform superuser is no organization admin here: with no membership row it is refused, with a member row it acts
// as a user, and only a live impersonation session (not the flag) is the impersonation refusal.
func TestAPlatformSuperuserGetsNoBypass(t *testing.T) {
	user, org := uuid.New(), uuid.New()
	f := newFixture(t, true)
	f.store.superusers[user] = true
	token := f.sign(user, org.String(), jwt.MapClaims{"is_superuser": true, "role": "admin"})
	expect(t, f.post(token, credOne), http.StatusForbidden, bodyNotMember)
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "member", found: true}
	if got := f.post(token, credOne); got.Code != http.StatusNoContent {
		t.Fatalf("answer %d %s", got.Code, got.Body.String())
	}
	if len(f.revoker.calls) != 1 || f.revoker.calls[0].actorType != acrrevoke.ActorUser {
		t.Fatalf("revoke calls %+v", f.revoker.calls)
	}
}
