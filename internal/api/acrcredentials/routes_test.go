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

	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

var tokens = edtokens.Fresh()

// users is the policy store: every user exists and is active; superusers and live impersonation sessions are
// planted per test, and role is the membership the access check reads for the principal ("member" when unset).
// The route's own membership read goes to members, never to this store.
type users struct {
	mu         sync.Mutex
	role       string
	superusers map[uuid.UUID]bool
	platforms  map[uuid.UUID]*rule.Platform
	sessions   map[uuid.UUID]*policy.Impersonation
}

func (u *users) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return policy.UserState{IsActive: true, IsSuperuser: u.superusers[id], Platform: u.platforms[id]}, true, nil
}
func (u *users) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.role == "" {
		return "member", true, nil
	}
	return u.role, true, nil
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
	f := &fixture{t: t, store: &users{superusers: map[uuid.UUID]bool{}, platforms: map[uuid.UUID]*rule.Platform{},
		sessions: map[uuid.UUID]*policy.Impersonation{}},
		members: &members{rows: map[[2]uuid.UUID]memberRow{}}, revoker: &revoker{}, scopeRun: scopeRun}
	f.auth = tokens.AuthenticatorOver(t, f.store, quiet())
	f.build(Deps{Guard: policy.NewGuard(f.auth, quiet()), Members: f.members, Revoker: f.revoker, Logger: quiet()})
	return f
}

// build mounts Routes on a mux behind the api listener's request-wide Scope layers (Principal, OrgScope,
// Impersonation) and, outermost, the route-row match, as apiservice composes them.
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
		handler = policy.RouteProfiles(func(r *http.Request) (string, bool) {
			_, pattern := mux.Handler(r)
			return pattern, pattern != ""
		})(handler)
	}
	f.handler = handler
}

// sign mints a routed access token for user in org; a non-nil actor makes it a delegated bearer.
func (f *fixture) sign(user uuid.UUID, org string, actor *uuid.UUID) string {
	f.t.Helper()
	return tokens.MustMint(f.t, edtokens.Claims{UserID: user, OrgID: org, Actor: actor})
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
	bodyRefused    = `{"detail":{"message":"Invalid or expired token"}}`
)

// The principal's role (the access check's membership read) never picks the actor: the route's own live
// membership row does.
func TestTheActorIsTheLiveMembershipRowNeverThePrincipalRole(t *testing.T) {
	cases := []struct {
		name, principalRole, rowRole, wantActor string
	}{
		{"member", "member", "member", acrrevoke.ActorUser},
		{"admin row", "member", "admin", acrrevoke.ActorAdmin},
		{"owner row", "member", "owner", acrrevoke.ActorAdmin},
		{"admin principal, member row", "admin", "member", acrrevoke.ActorUser},
		{"owner principal, viewer row", "owner", "viewer", acrrevoke.ActorUser},
		{"admin principal, empty row role", "admin", "", acrrevoke.ActorUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			user, org := uuid.New(), uuid.New()
			f.store.role = tc.principalRole
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: tc.rowRole, found: true}
			got := f.post(f.sign(user, org.String(), nil), credOne)
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

// A non-canonical organization or subject is refused at the access-token contract (401): the route is not
// reached, so no membership is read and no revoke is sent. (The HS256 path folded both to canonical form.)
func TestANonCanonicalOrganizationOrSubjectIsRefusedAtTheContract(t *testing.T) {
	user, org := uuid.New(), uuid.New()
	for name, claims := range map[string]edtokens.Claims{
		"upper-case org_id": {UserID: user, OrgID: strings.ToUpper(org.String())},
		"upper-case sub":    {UserID: user, OrgID: org.String(), Subject: strings.ToUpper(edtokens.PrincipalID(user))},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, true)
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "member", found: true}
			expect(t, f.post(tokens.MustMint(t, claims), credOne), http.StatusUnauthorized, bodyRefused)
			if len(f.revoker.calls) != 0 || f.members.calls != 0 {
				t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
			}
		})
	}
}

func TestRefusalsSendNoRevoke(t *testing.T) {
	user, org, actor := uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name   string
		plant  func(*fixture) (token, credential string)
		status int
		body   string
		reads  int
	}{
		{"no token", func(*fixture) (string, string) { return "", credOne }, http.StatusUnauthorized, `{"detail":{"message":"Not authenticated"}}`, 0},
		{"no org claim", func(f *fixture) (string, string) { return f.sign(user, "", nil), credOne }, http.StatusForbidden, `{"detail":"Organization context required"}`, 0},
		{"org claim not a uuid", func(f *fixture) (string, string) { return f.sign(user, "not-a-uuid", nil), credOne }, http.StatusUnauthorized, bodyRefused, 0},
		{"no membership row", func(f *fixture) (string, string) {
			f.store.role = "admin"
			return f.sign(user, org.String(), nil), credOne
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
			return f.sign(user, org.String(), nil), credOne
		}, http.StatusForbidden, bodyImpersonal, 0},
		{"delegated bearer", func(f *fixture) (string, string) {
			f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
			return f.sign(user, org.String(), &actor), credOne
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
	user, org, actor := uuid.New(), uuid.New(), uuid.New()
	f.members.rows[[2]uuid.UUID{user, org}] = memberRow{role: "admin", found: true}
	expect(t, f.post(f.sign(user, org.String(), &actor), credOne), http.StatusForbidden, bodyImpersonal)
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

// Product rule 1 (a platform admin is never forced to hold an organization membership row): a holder of admin.orgs
// revokes as the admin of the token's organization, with no membership read, whatever row it holds there, and the acr
// call names the platform admin as the actor.
func TestAPlatformAdminRevokesAsTheOrganizationAdmin(t *testing.T) {
	cases := []struct {
		name   string
		row    *memberRow
		err    error
		status int
		body   string
	}{
		{"no row", nil, nil, http.StatusNoContent, ""},
		{"no row, acr refuses", nil, acrrevoke.ErrNotFound, http.StatusNotFound, bodyNotFound},
		{"no row, already revoked", nil, acrrevoke.ErrConflict, http.StatusConflict, bodyConflict},
		{"no row, acr unavailable", nil, &acrrevoke.Error{Kind: acrlisting.KindUnavailable, Status: 503}, http.StatusServiceUnavailable, bodyUnavail},
		{"member row", &memberRow{role: "member", found: true}, nil, http.StatusNoContent, ""},
		{"viewer row", &memberRow{role: "viewer", found: true}, nil, http.StatusNoContent, ""},
		{"a failing row read is never made", &memberRow{err: errors.New("db down")}, nil, http.StatusNoContent, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			user, org := uuid.New(), uuid.New()
			f.store.superusers[user] = true
			f.revoker.err = tc.err
			if tc.row != nil {
				f.members.rows[[2]uuid.UUID{user, org}] = *tc.row
			}
			expect(t, f.post(f.sign(user, org.String(), nil), credOne), tc.status, tc.body)
			want := revokeCall{org: org.String(), credential: credOne, actorType: acrrevoke.ActorAdmin, actorID: user.String()}
			if len(f.revoker.calls) != 1 || f.revoker.calls[0] != want {
				t.Fatalf("revoke calls = %+v, want %+v", f.revoker.calls, want)
			}
			if f.members.calls != 0 {
				t.Fatalf("%d membership reads, want 0", f.members.calls)
			}
		})
	}
}

// The platform action decides, never the superuser flag: a role holding admin.orgs alone revokes as the admin; one
// holding every other platform action but not admin.orgs takes the membership path and, with no row, is refused.
func TestOnlyAdminOrgsIsThePlatformAdminPath(t *testing.T) {
	var others []string
	for _, action := range rule.PlatformActions {
		if action != rule.ActionAdminOrgs {
			others = append(others, action)
		}
	}
	orgsOnly, allButOrgs := rule.PlatformOf(rule.ActionAdminOrgs), rule.PlatformOf(others...)
	for _, tc := range []struct {
		name     string
		platform *rule.Platform
		status   int
		body     string
		calls    int
		reads    int
	}{
		{"admin.orgs alone", &orgsOnly, http.StatusNoContent, "", 1, 0},
		{"every action but admin.orgs", &allButOrgs, http.StatusForbidden, bodyNotMember, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			user, org := uuid.New(), uuid.New()
			f.store.platforms[user] = tc.platform
			expect(t, f.post(f.sign(user, org.String(), nil), credOne), tc.status, tc.body)
			if len(f.revoker.calls) != tc.calls || f.members.calls != tc.reads {
				t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
			}
			if tc.calls == 1 && f.revoker.calls[0].actorType != acrrevoke.ActorAdmin {
				t.Fatalf("revoke %+v, want the admin actor", f.revoker.calls)
			}
		})
	}
}

// Each refusal ahead of the platform question still holds for a platform admin: a delegated bearer, a live
// impersonation session and a token with no organization send no revoke and read no membership.
func TestAPlatformAdminKeepsTheRefusals(t *testing.T) {
	user, org, actor := uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name  string
		plant func(*fixture) string
		body  string
	}{
		{"delegated bearer", func(f *fixture) string { return f.sign(user, org.String(), &actor) }, bodyImpersonal},
		{"live impersonation session", func(f *fixture) string {
			f.store.sessions[user] = &policy.Impersonation{ID: uuid.New(), AdminUserID: user, TargetUserID: uuid.New(), TargetOrgID: org,
				TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
			return f.sign(user, org.String(), nil)
		}, bodyImpersonal},
		{"no org claim", func(f *fixture) string { return f.sign(user, "", nil) }, bodyNotMember},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			f.store.superusers[user] = true
			expect(t, f.post(tc.plant(f), credOne), http.StatusForbidden, tc.body)
			if len(f.revoker.calls) != 0 || f.members.calls != 0 {
				t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
			}
		})
	}
}

// With the admin cell on, the platform question is the decision point's: its allow makes a caller without the flag
// the admin actor, and no verdict is the decision point's 503, before any membership read or revoke.
func TestThePlatformQuestionIsTheDecisionPointsWithTheAdminCellOn(t *testing.T) {
	const bearer = "pdp-bearer"
	allow := pepfake.Rule{PrincipalReference: bearer, Action: rule.ActionAdminOrgs, ResourceType: "platform", ResourceKey: "platform"}
	bad := allow
	bad.MinimumAssurance = "bogus"
	for _, tc := range []struct {
		name   string
		rule   pepfake.Rule
		status int
		calls  int
	}{
		{"allow", allow, http.StatusNoContent, 1},
		{"no verdict", bad, http.StatusServiceUnavailable, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			user, org := uuid.New(), uuid.New()
			inner := f.handler
			f.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				inner.ServeHTTP(w, r.WithContext(policy.WithPlatformDecider(r.Context(), pepfake.New(tc.rule), bearer)))
			})
			got := f.post(f.sign(user, org.String(), nil), credOne)
			if got.Code != tc.status {
				t.Fatalf("answer %d %s, want %d", got.Code, got.Body.String(), tc.status)
			}
			if len(f.revoker.calls) != tc.calls || f.members.calls != 0 {
				t.Fatalf("revoke %+v, %d reads", f.revoker.calls, f.members.calls)
			}
			if tc.calls == 1 && f.revoker.calls[0].actorType != acrrevoke.ActorAdmin {
				t.Fatalf("revoke %+v, want the admin actor", f.revoker.calls)
			}
		})
	}
}
