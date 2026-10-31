package acr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const accessToken = "svc_acr_principal-access-token-0123456789"

var (
	accessOrg     = uuid.MustParse("6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11")
	accessUser    = uuid.MustParse("0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d")
	otherUser     = uuid.MustParse("1c2d3e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f")
	accessSession = uuid.MustParse("2d3e4f5a-6b7c-4d8e-9f0a-1b2c3d4e5f6a")
)

// accessOrgID is accessOrg as an addressable value (a session's organization).
var accessOrgID = accessOrg

func accessPath(org, user, query string) string {
	path := "/api/v1/internal/acr/orgs/" + org + "/principals/" + user + "/access"
	if query != "" {
		path += "?" + query
	}
	return path
}

func accessCreds() *fakeCredentials {
	f := credsFor()
	f.creds[accessToken] = Credential{ID: "cred-access", ServiceName: "acr", Scopes: []string{EntitlementScope, PrincipalAccessScope}}
	return f
}

// accessServer mounts Routes, which serves the principal access read from
// store, with limiter as its own attempt bucket.
func accessServer(t *testing.T, store PrincipalAccessStore, creds CredentialStore, deps Deps, limiter ...*httpapi.KeyedLimiter) http.Handler {
	t.Helper()
	deps.Auth = creds
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if deps.Store == nil {
		deps.Store = &fakeStore{}
	}
	deps.Access = store
	if len(limiter) > 0 {
		deps.AccessLimiter = limiter[0]
	}
	mux := http.NewServeMux()
	for _, route := range Routes(deps) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return mux
}

// Routes mounts the principal access read exactly once, as GET, and serves it
// from deps.Access: a Routes without the read, or one that reads another
// store, fails here.
func TestRoutesMountThePrincipalAccessRead(t *testing.T) {
	store := &fakeAccessStore{access: PrincipalAccess{Member: true}}
	var mounted []httpapi.Route
	for _, route := range Routes(Deps{Auth: accessCreds(), Access: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}) {
		if route.Pattern == PrincipalAccessPattern {
			mounted = append(mounted, route)
		}
	}
	if len(mounted) != 1 || mounted[0].Method != http.MethodGet {
		t.Fatalf("Routes mounts the principal access read %d time(s) (%v), want once as GET", len(mounted), mounted)
	}
	mux := http.NewServeMux()
	mux.Handle(mounted[0].Method+" "+mounted[0].Pattern, mounted[0].Handler)
	rec := do(mux, accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
	if rec.Code != http.StatusOK || len(store.queries) != 1 {
		t.Fatalf("mounted read = %d after %d store call(s), want 200 after 1: %s", rec.Code, len(store.queries), rec.Body.String())
	}
}

// fakeAccessStore answers one canned PrincipalAccess and records each query.
type fakeAccessStore struct {
	mu      sync.Mutex
	access  PrincipalAccess
	err     error
	queries []AccessQuery
}

func (f *fakeAccessStore) Access(_ context.Context, q AccessQuery) (PrincipalAccess, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q)
	return f.access, f.err
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return body
}

func TestPrincipalAccessBearerAndScope(t *testing.T) {
	cases := []struct {
		name   string
		header string
		status int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"unknown token", "Bearer svc_acr_nobody-0123456789", http.StatusUnauthorized},
		{"revoked", "Bearer svc_acr_revoked-token-0123456789", http.StatusUnauthorized},
		{"entitlements scope only", "Bearer " + entToken, http.StatusForbidden},
		{"llm config scope only", "Bearer svc_acr_llm-only-token-0123456789", http.StatusForbidden},
		{"principal access scope", "Bearer " + accessToken, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeAccessStore{access: PrincipalAccess{Member: true}}
			rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), tc.header)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if wantRead := tc.status == http.StatusOK; (len(store.queries) == 1) != wantRead {
				t.Fatalf("store read %d times; a refused request must not read", len(store.queries))
			}
		})
	}
}

// The entitlement route's credential never reaches the access read, and the
// read is not audited (one row per acr call would put a write on the read path).
func TestPrincipalAccessIsNotAudited(t *testing.T) {
	creds := accessCreds()
	rec := do(accessServer(t, &fakeAccessStore{}, creds, Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d %s", rec.Code, rec.Body.String())
	}
	if len(creds.audits) != 0 || len(creds.used) != 0 {
		t.Fatalf("audits %v used %v; the access read writes neither", creds.audits, creds.used)
	}
}

// The access read counts against its own bucket: an exhausted entitlement and
// health bucket does not refuse it.
func TestPrincipalAccessHasItsOwnAttemptBucket(t *testing.T) {
	counters := httpapi.NewMemoryCounters(time.Now)
	shared := httpapi.NewKeyedLimiter(counters, httpapi.Limit{ID: "internal_acr_auth_attempt", Count: 1, Window: time.Minute})
	own := httpapi.NewKeyedLimiter(counters, httpapi.Limit{ID: AccessAttemptLimit.ID, Count: 2, Window: time.Minute})
	h := accessServer(t, &fakeAccessStore{}, accessCreds(), Deps{Limiter: shared}, own)
	if rec := do(h, healthPath, "Bearer "+entToken); rec.Code != http.StatusOK {
		t.Fatalf("health = %d", rec.Code)
	}
	if rec := do(h, healthPath, "Bearer "+entToken); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second health = %d, want the shared bucket exhausted", rec.Code)
	}
	path := accessPath(accessOrg.String(), accessUser.String(), "")
	for i := 0; i < 2; i++ {
		if rec := do(h, path, "Bearer "+accessToken); rec.Code != http.StatusOK {
			t.Fatalf("access %d = %d; the shared bucket must not refuse it", i, rec.Code)
		}
	}
	if rec := do(h, path, "Bearer "+accessToken); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third access = %d, want its own bucket exhausted", rec.Code)
	}
}

func TestPrincipalAccessRefusesMalformedInput(t *testing.T) {
	wire := sessionid.Wire(accessSession)
	cases := []struct {
		name, org, user, query string
		status                 int
		code                   string
	}{
		{"malformed org", "not-a-uuid", accessUser.String(), "", http.StatusNotFound, ""},
		{"malformed user", accessOrg.String(), "not-a-uuid", "", http.StatusNotFound, ""},
		{"unknown parameter", accessOrg.String(), accessUser.String(), "org=x", http.StatusBadRequest, InvalidQueryCode},
		{"sid and another parameter", accessOrg.String(), accessUser.String(), "sid=" + wire + "&x=1", http.StatusBadRequest, InvalidQueryCode},
		{"two sids", accessOrg.String(), accessUser.String(), "sid=" + wire + "&sid=" + wire, http.StatusBadRequest, InvalidQueryCode},
		{"empty sid", accessOrg.String(), accessUser.String(), "sid=", http.StatusBadRequest, InvalidSessionCode},
		{"bare uuid sid", accessOrg.String(), accessUser.String(), "sid=" + accessSession.String(), http.StatusBadRequest, InvalidSessionCode},
		{"short sid", accessOrg.String(), accessUser.String(), "sid=ses_AAAA", http.StatusBadRequest, InvalidSessionCode},
		// url.Query would drop these pairs and read the request as having no sid.
		{"undecodable sid", accessOrg.String(), accessUser.String(), "sid=%zz", http.StatusBadRequest, InvalidQueryCode},
		{"undecodable key", accessOrg.String(), accessUser.String(), "%zz=" + wire, http.StatusBadRequest, InvalidQueryCode},
		{"semicolon separator", accessOrg.String(), accessUser.String(), "sid=" + wire + ";x=1", http.StatusBadRequest, InvalidQueryCode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeAccessStore{}
			rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(tc.org, tc.user, tc.query), "Bearer "+accessToken)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			body := decodeBody(t, rec)
			if code, _ := body["code"].(string); code != tc.code {
				t.Fatalf("code = %q, want %q (%v)", code, tc.code, body)
			}
			if len(store.queries) != 0 {
				t.Fatal("a refused request read the store")
			}
		})
	}
}

func TestPrincipalAccessPassesTheSessionToTheStore(t *testing.T) {
	store := &fakeAccessStore{}
	h := accessServer(t, store, accessCreds(), Deps{})
	do(h, accessPath(accessOrg.String(), accessUser.String(), "sid="+sessionid.Wire(accessSession)), "Bearer "+accessToken)
	do(h, accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
	if len(store.queries) != 2 {
		t.Fatalf("queries = %v", store.queries)
	}
	if got := store.queries[0]; got.OrgID != accessOrg || got.UserID != accessUser || got.SessionID == nil || *got.SessionID != accessSession {
		t.Fatalf("with sid: %+v", got)
	}
	if got := store.queries[1]; got.SessionID != nil {
		t.Fatalf("without sid: %+v", got)
	}
}

func TestPrincipalAccessStoreErrors(t *testing.T) {
	cases := []struct {
		name   string
		store  PrincipalAccessStore
		status int
		code   string
	}{
		{"no store configured", nil, http.StatusServiceUnavailable, ""},
		{"store unavailable", &fakeAccessStore{err: ErrUnavailable}, http.StatusServiceUnavailable, ""},
		{"unclassified error", &fakeAccessStore{err: errBoom}, http.StatusServiceUnavailable, ""},
		{"organization unknown", &fakeAccessStore{err: ErrOrgUnknown}, http.StatusNotFound, UnknownOrganizationCode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(accessServer(t, tc.store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if code, _ := decodeBody(t, rec)["code"].(string); code != tc.code {
				t.Fatalf("code = %q, want %q", code, tc.code)
			}
		})
	}
}

func TestPrincipalAccessBody(t *testing.T) {
	cases := []struct {
		name   string
		access PrincipalAccess
		want   string
	}{
		{"row-only, empty inventory", PrincipalAccess{Member: true, AskDev: true, AgentContextRuntime: false},
			`{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":true,"impersonating":false,` +
				`"entitlements":{"ask_dev":true,"agent_context_runtime":false},"repository_scope":{"kind":"none"}}`},
		{"row-only, impersonating", PrincipalAccess{Member: true, Impersonating: true},
			`{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":true,"impersonating":true,` +
				`"entitlements":{"ask_dev":false,"agent_context_runtime":false},"repository_scope":{"kind":"none"}}`},
		{"session, repos", PrincipalAccess{Member: false, Impersonating: true, Session: &SessionAccess{Live: true, Impersonating: true},
			AgentContextRuntime: true, Repositories: []string{"acme/a", "acme/b"}},
			`{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":false,"impersonating":true,` +
				`"session":{"live":true,"impersonating":true},"entitlements":{"ask_dev":false,"agent_context_runtime":true},` +
				`"repository_scope":{"kind":"repos","repos":["acme/a","acme/b"]}}`},
		{"empty non-nil inventory is none", PrincipalAccess{Repositories: []string{}},
			`{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":false,"impersonating":false,` +
				`"entitlements":{"ask_dev":false,"agent_context_runtime":false},"repository_scope":{"kind":"none"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(accessServer(t, &fakeAccessStore{access: tc.access}, accessCreds(), Deps{}),
				accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
			want := strings.NewReplacer("ORG", accessOrg.String(), "USER", accessUser.String()).Replace(tc.want)
			if rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Fatalf("status %d body\n%s\nwant\n%s", rec.Code, rec.Body.String(), want)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type %q", got)
			}
		})
	}
}

// ---- AccessStore ----

type fakeReads struct {
	mu            sync.Mutex
	users         map[uuid.UUID]policy.UserState
	members       map[[2]uuid.UUID]bool
	sessions      map[uuid.UUID]sessionissue.State
	impersonating map[uuid.UUID]bool
	err           map[string]error
	calls         map[string]int
}

func newFakeReads() *fakeReads {
	pin := int64(3)
	return &fakeReads{
		users:         map[uuid.UUID]policy.UserState{accessUser: {IsActive: true, TokenVersion: 3}, otherUser: {IsActive: true}},
		members:       map[[2]uuid.UUID]bool{{accessUser, accessOrg}: true},
		sessions:      map[uuid.UUID]sessionissue.State{accessSession: {PrincipalID: accessUser, OrganizationID: &accessOrgID, LegacyTokenVersion: &pin}},
		impersonating: map[uuid.UUID]bool{},
		err:           map[string]error{},
		calls:         map[string]int{},
	}
}

func (f *fakeReads) note(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[name]++
	return f.err[name]
}

func (f *fakeReads) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	if err := f.note("users"); err != nil {
		return policy.UserState{}, false, err
	}
	state, ok := f.users[id]
	return state, ok, nil
}

func (f *fakeReads) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	if err := f.note("memberships"); err != nil {
		return "", false, err
	}
	return "member", f.members[[2]uuid.UUID{user, org}], nil
}

func (f *fakeReads) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if err := f.note("impersonation"); err != nil {
		return nil, err
	}
	if f.impersonating[admin] {
		return &policy.Impersonation{AdminUserID: admin}, nil
	}
	return nil, nil
}

// State is auth-service's session state route (SessionStates).
func (f *fakeReads) State(_ context.Context, id uuid.UUID) (sessionissue.State, error) {
	if err := f.note("sessions"); err != nil {
		return sessionissue.State{}, err
	}
	state, ok := f.sessions[id]
	if !ok {
		return sessionissue.State{}, sessionissue.ErrSessionNotFound
	}
	return state, nil
}

type fakeFeatures struct {
	allowed map[string]bool
	err     map[string]error
	calls   int
}

func (f *fakeFeatures) Decide(_ context.Context, orgID, key string) (licensing.Decision, error) {
	f.calls++
	if orgID != accessOrg.String() {
		return licensing.Decision{}, errors.New("decision for another org")
	}
	if err := f.err[key]; err != nil {
		return licensing.Decision{}, err
	}
	return licensing.Decision{FeatureKey: key, Allowed: f.allowed[key]}, nil
}

type fakeOrgs struct {
	exists bool
	err    error
}

func (f fakeOrgs) OrganizationExists(context.Context, uuid.UUID) (bool, error) {
	return f.exists, f.err
}

type fakeInventory struct {
	repos []string
	err   error
	calls int
	mu    sync.Mutex
}

func (f *fakeInventory) Repositories(_ context.Context, org uuid.UUID) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if org != accessOrg {
		return nil, errors.New("inventory of another org")
	}
	return f.repos, f.err
}

type accessFixture struct {
	reads     *fakeReads
	features  *fakeFeatures
	orgs      fakeOrgs
	inventory *fakeInventory
}

func newAccessFixture() *accessFixture {
	return &accessFixture{
		reads:     newFakeReads(),
		features:  &fakeFeatures{allowed: map[string]bool{"ask_dev": true, "agent_context_runtime": true}, err: map[string]error{}},
		orgs:      fakeOrgs{exists: true},
		inventory: &fakeInventory{repos: []string{"acme/api", "acme/web"}},
	}
}

func (f *accessFixture) store() AccessStore {
	return AccessStore{Organizations: f.orgs, Reads: f.reads, Features: f.features, Inventory: f.inventory, Sessions: f.reads}
}

func (f *accessFixture) read(t *testing.T, user uuid.UUID, session *uuid.UUID) (PrincipalAccess, error) {
	t.Helper()
	return f.store().Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: user, SessionID: session})
}

func sessionRef(id uuid.UUID) *uuid.UUID { return &id }

func TestAccessStoreServesEveryFieldLive(t *testing.T) {
	f := newAccessFixture()
	got, err := f.read(t, accessUser, sessionRef(accessSession))
	if err != nil {
		t.Fatal(err)
	}
	want := PrincipalAccess{Member: true, Session: &SessionAccess{Live: true, Reason: sessionReasonLive},
		AskDev: true, AgentContextRuntime: true, Repositories: []string{"acme/api", "acme/web"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("access = %+v (session %+v), want %+v", got, got.Session, want)
	}
}

func TestAccessStoreMembership(t *testing.T) {
	cases := []struct {
		name   string
		plant  func(*accessFixture)
		user   uuid.UUID
		member bool
	}{
		{"active member", func(*accessFixture) {}, accessUser, true},
		{"no membership row", func(*accessFixture) {}, otherUser, false},
		{"unknown user", func(f *accessFixture) { delete(f.reads.users, accessUser) }, accessUser, false},
		{"deactivated user", func(f *accessFixture) {
			f.reads.users[accessUser] = policy.UserState{IsActive: false, TokenVersion: 3}
		}, accessUser, false},
		{"superuser without membership", func(f *accessFixture) {
			f.reads.users[otherUser] = policy.UserState{IsActive: true, IsSuperuser: true}
		}, otherUser, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFixture()
			tc.plant(f)
			got, err := f.read(t, tc.user, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Member != tc.member {
				t.Fatalf("member = %v, want %v", got.Member, tc.member)
			}
		})
	}
}

func TestAccessStoreSessionLiveness(t *testing.T) {
	stale, missing := int64(2), (*int64)(nil)
	otherSession := uuid.MustParse("3e4f5a6b-7c8d-4e9f-8a1b-2c3d4e5f6a7b")
	cases := []struct {
		name    string
		plant   func(*accessFixture)
		user    uuid.UUID
		session uuid.UUID
		live    bool
		reason  string
	}{
		{"live", func(*accessFixture) {}, accessUser, accessSession, true, sessionReasonLive},
		{"unknown session", func(*accessFixture) {}, accessUser, otherSession, false, sessionReasonNotFound},
		{"another user's sid", func(*accessFixture) {}, otherUser, accessSession, false, sessionReasonOtherUser},
		{"revoked", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			row.Revoked = true
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonRevoked},
		{"no token_version pin", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			row.LegacyTokenVersion = missing
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonPinMissing},
		{"stale token_version", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			row.LegacyTokenVersion = &stale
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonVersionStale},
		{"password change bumped token_version", func(f *accessFixture) {
			f.reads.users[accessUser] = policy.UserState{IsActive: true, TokenVersion: 4}
		}, accessUser, accessSession, false, sessionReasonVersionStale},
		{"users row gone", func(f *accessFixture) { delete(f.reads.users, accessUser) }, accessUser, accessSession, false, sessionReasonUserUnknown},
		{"bound to another organization", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			another := uuid.MustParse("4f5a6b7c-8d9e-4f0a-9b1c-2d3e4f5a6b7c")
			row.OrganizationID = &another
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonOrgMismatch},
		{"bound to no organization", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			row.OrganizationID = nil
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonOrgMismatch},
		{"expired", func(f *accessFixture) {
			row := f.reads.sessions[accessSession]
			row.Expired = true
			f.reads.sessions[accessSession] = row
		}, accessUser, accessSession, false, sessionReasonExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFixture()
			tc.plant(f)
			got, err := f.read(t, tc.user, sessionRef(tc.session))
			if err != nil {
				t.Fatal(err)
			}
			if got.Session == nil || got.Session.Live != tc.live || got.Session.Reason != tc.reason {
				t.Fatalf("session = %+v, want live=%v reason=%s", got.Session, tc.live, tc.reason)
			}
		})
	}
}

// Impersonation is keyed by the user, not by session, so it is read and
// served on every call, with or without sid: acr denies every user-class call
// while it is on (design §3.1 step 3).
func TestAccessStoreImpersonation(t *testing.T) {
	for _, on := range []bool{false, true} {
		f := newAccessFixture()
		f.reads.impersonating[accessUser] = on
		got, err := f.read(t, accessUser, sessionRef(accessSession))
		if err != nil {
			t.Fatal(err)
		}
		if got.Impersonating != on || got.Session == nil || got.Session.Impersonating != on {
			t.Fatalf("impersonating = %v, session %+v, want %v", got.Impersonating, got.Session, on)
		}
		// Impersonation does not change liveness: acr denies on either.
		if !got.Session.Live {
			t.Fatalf("session = %+v", got.Session)
		}
		rowOnly, err := f.read(t, accessUser, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rowOnly.Impersonating != on {
			t.Fatalf("row-only impersonating = %v, want %v", rowOnly.Impersonating, on)
		}
	}
}

func TestAccessStoreWithoutSidReadsNoSession(t *testing.T) {
	f := newAccessFixture()
	f.reads.impersonating[accessUser] = true
	got, err := f.read(t, accessUser, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Session != nil || f.reads.calls["sessions"] != 0 {
		t.Fatalf("session %+v, calls %v: a row-only read must omit the session", got.Session, f.reads.calls)
	}
	if !got.Impersonating || f.reads.calls["impersonation"] != 1 {
		t.Fatalf("impersonating %v, calls %v: a row-only read still reads impersonation", got.Impersonating, f.reads.calls)
	}
}

func TestAccessStoreEntitlements(t *testing.T) {
	for _, key := range []string{"ask_dev", "agent_context_runtime"} {
		t.Run(key+" off", func(t *testing.T) {
			f := newAccessFixture()
			f.features.allowed[key] = false
			got, err := f.read(t, accessUser, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.AskDev == (key == "ask_dev") || got.AgentContextRuntime == (key == "agent_context_runtime") {
				t.Fatalf("ask_dev=%v agent_context_runtime=%v with %s off", got.AskDev, got.AgentContextRuntime, key)
			}
		})
	}
}

func TestAccessStoreEmptyInventoryIsNone(t *testing.T) {
	for _, repos := range [][]string{nil, {}} {
		f := newAccessFixture()
		f.inventory.repos = repos
		got, err := f.read(t, accessUser, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Repositories) != 0 {
			t.Fatalf("repositories = %v", got.Repositories)
		}
		scope, _ := principalAccessBody("o", "u", got).Get("repository_scope")
		raw, _ := json.Marshal(scope)
		if string(raw) != `{"kind":"none"}` {
			t.Fatalf("repository_scope = %s, want kind none with no repos", raw)
		}
	}
}

// Every read failure fails the whole answer: no field is served from a
// partial read, and nothing reads as closed or empty instead of unavailable.
func TestAccessStoreFailsClosedOnEveryReadFailure(t *testing.T) {
	cases := []struct {
		name  string
		plant func(*accessFixture)
	}{
		{"organizations", func(f *accessFixture) { f.orgs.err = errBoom }},
		{"users", func(f *accessFixture) { f.reads.err["users"] = errBoom }},
		{"memberships", func(f *accessFixture) { f.reads.err["memberships"] = errBoom }},
		{"sessions", func(f *accessFixture) {
			f.reads.err["sessions"] = &policy.GrantError{Grant: "SELECT on sessions", Err: errBoom}
		}},
		{"impersonation", func(f *accessFixture) { f.reads.err["impersonation"] = errBoom }},
		{"ask_dev", func(f *accessFixture) { f.features.err["ask_dev"] = errBoom }},
		{"agent_context_runtime", func(f *accessFixture) { f.features.err["agent_context_runtime"] = errBoom }},
		{"inventory", func(f *accessFixture) { f.inventory.err = errBoom }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFixture()
			tc.plant(f)
			got, err := f.read(t, accessUser, sessionRef(accessSession))
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v (access %+v), want ErrUnavailable", err, got)
			}
			if !reflect.DeepEqual(got, PrincipalAccess{}) {
				t.Fatalf("a failed read served %+v", got)
			}
		})
	}
}

func TestAccessStoreUnknownOrganization(t *testing.T) {
	f := newAccessFixture()
	f.orgs.exists = false
	if _, err := f.read(t, accessUser, nil); !errors.Is(err, ErrOrgUnknown) {
		t.Fatalf("err = %v, want ErrOrgUnknown", err)
	}
	if f.inventory.calls != 0 || f.features.calls != 0 {
		t.Fatal("an unknown organization must read nothing else")
	}
}

func TestAccessStoreNotConfiguredIsUnavailable(t *testing.T) {
	f := newAccessFixture()
	store := f.store()
	store.Inventory = nil
	if _, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable without an inventory reader", err)
	}
}

// No cache (check 18): every call reads every source again, and a change
// between two calls is the next call's answer.
func TestAccessStoreReadsLiveOnEveryCall(t *testing.T) {
	f := newAccessFixture()
	first, err := f.read(t, accessUser, sessionRef(accessSession))
	if err != nil || !first.Session.Live || !first.AskDev {
		t.Fatalf("first = %+v, %v", first, err)
	}
	row := f.reads.sessions[accessSession]
	row.Revoked = true
	f.reads.sessions[accessSession] = row
	f.features.allowed["ask_dev"] = false
	f.inventory.repos = nil
	second, err := f.read(t, accessUser, sessionRef(accessSession))
	if err != nil {
		t.Fatal(err)
	}
	if second.Session.Live || second.AskDev || len(second.Repositories) != 0 {
		t.Fatalf("second = %+v (session %+v): a change between calls must be read", second, second.Session)
	}
	if f.reads.calls["sessions"] != 2 || f.inventory.calls != 2 || f.features.calls != 4 {
		t.Fatalf("calls %v inventory %d features %d", f.reads.calls, f.inventory.calls, f.features.calls)
	}
}

// Without auth-service's session state reader a read that names a sid is
// unavailable (never live, never not live), and a read without one does not
// need it (D6559: no start-up failure, a 503 per sid read).
func TestAccessStoreWithoutASessionStateReader(t *testing.T) {
	f := newAccessFixture()
	store := f.store()
	store.Sessions = nil
	if _, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser, SessionID: sessionRef(accessSession)}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a sid read without a state reader: err = %v, want ErrUnavailable", err)
	}
	got, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser})
	if err != nil || got.Session != nil || !got.Member {
		t.Fatalf("a row-only read without a state reader: %+v %v", got, err)
	}
}

// A fault of the state route (unreachable, any refusal, an auth-service with no
// state route: sessionissue answers ErrUnavailable for all of them) fails the
// whole read; it never reads as a session that is not live.
func TestAccessStoreFailsClosedOnASessionStateFault(t *testing.T) {
	f := newAccessFixture()
	f.reads.err["sessions"] = fmt.Errorf("%w: state route status 404", sessionissue.ErrUnavailable)
	if _, err := f.read(t, accessUser, sessionRef(accessSession)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}
