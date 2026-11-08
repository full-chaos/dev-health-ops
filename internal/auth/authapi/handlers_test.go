package authapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	testSecret   = "authapi-test-key-0123456789abcdef-0123"
	testIssuer   = "dev-health-ops"
	testAudience = "dev-health-api"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// fakeSessions is the session domain with programmable answers.
type fakeSessions struct {
	mu         sync.Mutex
	result     session.Result
	err        error
	records    map[uuid.UUID]session.Record
	classes    map[uuid.UUID]string
	currentErr error
	revoked    []uuid.UUID
	revokedAll []uuid.UUID
	actors     []session.Actor
	revokeErr  error
	// RevokeByCredential and CreateAudited.
	byCredential    []string
	byCredentialErr error
	created         []createdSession
	signIns         int
	createErr       error
	// RevokeLegacy.
	revokedLegacy   []session.LegacyRefresh
	revokeLegacyErr error
	// minted counts accepted exchanges: a successor a client never receives.
	minted int
	// exchanges counts every Exchange, accepted or refused: each one writes.
	exchanges int
	// StartDelegation and EndDelegation.
	delegated session.Record
	startErr  error
	started   []session.StartInput
	ended     []uuid.UUID
	endCalls  int
	endErr    error
}

func (f *fakeSessions) StartDelegation(_ context.Context, in session.StartInput) (session.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, in)
	if f.startErr != nil {
		return session.Record{}, f.startErr
	}
	return f.delegated, nil
}

func (f *fakeSessions) EndDelegation(_ context.Context, id uuid.UUID, _ session.Actor) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.endCalls++
	if f.endErr != nil {
		return false, f.endErr
	}
	record, ok := f.records[id]
	switch {
	case !ok:
		return false, session.ErrSessionNotFound
	case record.Delegation == nil:
		return false, session.ErrNotDelegated
	}
	f.ended = append(f.ended, id)
	return true, nil
}

// createdSession is one CreateAudited or CreateSupersedingAudited call;
// predecessor is set only for the second.
type createdSession struct {
	principal   uuid.UUID
	org         *uuid.UUID
	amr         []string
	event       audit.AuditEvent
	issued      session.Issued
	predecessor *uuid.UUID
}

func (f *fakeSessions) RevokeByCredential(_ context.Context, credential contracts.SecretString, actor session.Actor) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byCredential, f.actors = append(f.byCredential, credential.Reveal()), append(f.actors, actor)
	return f.byCredentialErr == nil, f.byCredentialErr
}

func (f *fakeSessions) CreateAudited(ctx context.Context, principal uuid.UUID, org *uuid.UUID, amr []string, event audit.AuditEvent) (session.Issued, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if session.IsSignIn(ctx) {
		f.signIns++
	}
	if f.createErr != nil {
		return session.Issued{}, f.createErr
	}
	now := time.Now().UTC()
	issued := session.Issued{
		Session: session.Record{ID: uuid.New(), PrincipalID: principal, OrganizationID: org, Revision: 1, AuthTime: now,
			AMR: amr, CreatedAt: now, ExpiresAt: now.Add(7 * 24 * time.Hour)},
		Credential: contracts.NewSecretString("dhr1_" + strings.Repeat("c", 43)), ExpiresAt: now.Add(7 * 24 * time.Hour),
	}
	f.created = append(f.created, createdSession{principal: principal, org: org, amr: amr, event: event, issued: issued})
	return issued, nil
}

func (f *fakeSessions) CreateSupersedingAudited(ctx context.Context, principal uuid.UUID, org *uuid.UUID, amr []string,
	predecessor uuid.UUID, event audit.AuditEvent) (session.Issued, error) {
	issued, err := f.CreateAudited(ctx, principal, org, amr, event)
	if err != nil {
		return issued, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created[len(f.created)-1].predecessor = &predecessor
	return issued, nil
}

func (f *fakeSessions) RevokeLegacy(_ context.Context, legacy session.LegacyRefresh, actor session.Actor) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokedLegacy, f.actors = append(f.revokedLegacy, legacy), append(f.actors, actor)
	return f.revokeLegacyErr == nil, f.revokeLegacyErr
}

func (f *fakeSessions) Exchange(context.Context, contracts.SecretString) (session.Result, error) {
	f.exchanges++
	if f.err == nil && f.result.Reason.Success() {
		f.minted++
	}
	return f.result, f.err
}

func (f *fakeSessions) ExchangeGuarded(ctx context.Context, credential contracts.SecretString, guard session.Guard) (session.Result, error) {
	if f.err == nil && f.result.Reason == session.ReasonAccepted {
		if err := guard(ctx, nil, f.result.Session); err != nil {
			return session.Result{Issued: session.Issued{Session: f.result.Session}}, err
		}
	}
	return f.Exchange(ctx, credential)
}

func (f *fakeSessions) CurrentWithClientClass(ctx context.Context, id uuid.UUID) (session.Record, string, error) {
	record, err := f.Current(ctx, id)
	if err != nil {
		return session.Record{}, "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return record, f.classes[id], nil
}

func (f *fakeSessions) Current(_ context.Context, id uuid.UUID) (session.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.currentErr != nil {
		return session.Record{}, f.currentErr
	}
	record, ok := f.records[id]
	if !ok {
		return session.Record{}, session.ErrSessionNotFound
	}
	return record, nil
}

func (f *fakeSessions) Revoke(_ context.Context, id uuid.UUID, actor session.Actor) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked, f.actors = append(f.revoked, id), append(f.actors, actor)
	return f.revokeErr == nil, f.revokeErr
}

func (f *fakeSessions) RevokeAll(_ context.Context, principal uuid.UUID, actor session.Actor) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokedAll, f.actors = append(f.revokedAll, principal), append(f.actors, actor)
	return 1, f.revokeErr
}

type fakeUsers struct {
	users       map[uuid.UUID]UserState
	roles       map[[2]uuid.UUID]*string
	memberships map[uuid.UUID][]Membership
	err         error
	roleErr     error
}

func (f fakeUsers) Memberships(_ context.Context, id uuid.UUID) ([]Membership, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.memberships[id], nil
}

func (f fakeUsers) State(_ context.Context, id uuid.UUID) (UserState, bool, error) {
	if f.err != nil {
		return UserState{}, false, f.err
	}
	user, ok := f.users[id]
	return user, ok, nil
}

func (f fakeUsers) Role(_ context.Context, user, org uuid.UUID) (*string, bool, error) {
	if f.roleErr != nil {
		return nil, false, f.roleErr
	}
	role, ok := f.roles[[2]uuid.UUID{user, org}]
	return role, ok, nil
}

type fixture struct {
	t         *testing.T
	sessions  *fakeSessions
	users     fakeUsers
	issuer    *Issuer
	server    http.Handler
	principal uuid.UUID
	org       uuid.UUID
	record    session.Record
	logs      *bytes.Buffer
}

// newFixture serves the session routes with the EdDSA issuer and its access
// check: without the check the bearer routes are not mounted.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, AccessTokenAudience)
	check, err := NewAccessCheck(AccessCheckConfig{IssuerURL: goldenIssuer, Keys: signer})
	if err != nil {
		t.Fatal(err)
	}
	return newFixtureWith(t, issuer.WithAccessCheck(check))
}

// withSessionBearer gives issuer the access check over a key set of its own
// (healthy whatever issuer signs with) and returns the EdDSA issuer of that key
// set, which mints the session-route bearer the check accepts.
func withSessionBearer(t *testing.T, issuer *Issuer) (*Issuer, *Issuer) {
	t.Helper()
	signer, _, _ := fileSigner(t)
	check, err := NewAccessCheck(AccessCheckConfig{IssuerURL: goldenIssuer, Keys: signer})
	if err != nil {
		t.Fatal(err)
	}
	return issuer.WithAccessCheck(check), newEdDSAIssuer(t, signer, AccessTokenAudience)
}

// newUncheckedFixture serves the session routes with the EdDSA issuer and no access
// check (login, refresh, logout mint; no bearer route mounts).
func newUncheckedFixture(t *testing.T) *fixture {
	t.Helper()
	issuer := newTestIssuer(t)
	return newFixtureWith(t, issuer)
}

func newFixtureWith(t *testing.T, issuer *Issuer) *fixture {
	t.Helper()
	principal, org := uuid.New(), uuid.New()
	now := time.Now().UTC()
	pin := int64(4)
	record := session.Record{
		ID: uuid.New(), PrincipalID: principal, OrganizationID: &org, Revision: 1,
		AuthTime: now.Add(-time.Hour), AMR: []string{"pwd"}, CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(7 * 24 * time.Hour),
		LegacyTokenVersion: &pin,
	}
	admin := "admin"
	username := "ada"
	f := &fixture{
		t:        t,
		sessions: &fakeSessions{records: map[uuid.UUID]session.Record{record.ID: record}},
		users: fakeUsers{
			users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", Username: &username, IsActive: true, IsSuperuser: true, TokenVersion: 4}},
			roles: map[[2]uuid.UUID]*string{{principal, org}: &admin},
		},
		issuer: issuer, principal: principal, org: org, record: record, logs: &bytes.Buffer{},
	}
	f.build()
	return f
}

func (f *fixture) build() {
	routes, err := Routes(Deps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Logger: slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		f.t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
}

func (f *fixture) do(method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	f.server.ServeHTTP(response, request)
	return response
}

func (f *fixture) bearer(grant Grant) map[string]string {
	return map[string]string{"Authorization": "Bearer " + f.accessFor(grant)}
}

// accessFor is the bearer of grant: the EdDSA session token when the issuer checks EdDSA tokens (WithAccessCheck),
// else go-api's HS256 token, the one the session routes verify without an access check.
func (f *fixture) accessFor(grant Grant) string {
	if f.issuer.access == nil {
		return legacyAccess(f.t, grant)
	}
	token, err := f.issuer.Access(grant)
	if err != nil {
		f.t.Fatal(err)
	}
	return token
}

func (f *fixture) grant() Grant {
	return Grant{PrincipalID: f.principal, OrganizationID: &f.org, Role: "admin", SessionID: f.record.ID, User: f.users.users[f.principal]}
}

func credential() string { return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)) }

func refreshBody(c string) string {
	return `{"schema_version":"refresh-request.v1","refresh_credential":"` + c + `"}`
}

// expectError asserts an error.v1 answer: the status, the reason, a
// server-minted request id and no credential in the body.
func expectError(t *testing.T, response *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d; body %s", response.Code, status, response.Body)
	}
	envelope, err := contracts.ParseErrorEnvelope(repoRoot(t), response.Body.Bytes(), status, time.Time{})
	if err != nil {
		t.Fatalf("not an error.v1 document: %v; body %s", err, response.Body)
	}
	if envelope.ReasonCode != reason {
		t.Fatalf("reason_code = %q, want %q", envelope.ReasonCode, reason)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("an auth answer is cacheable")
	}
}

func TestEveryExchangeRefusalMapsToItsStatusAndReason(t *testing.T) {
	cases := map[session.Reason]struct {
		status int
		reason string
	}{
		session.ReasonMalformed:            {401, "malformed"},
		session.ReasonRefreshUnknown:       {401, "refresh_unknown"},
		session.ReasonRefreshExpired:       {401, "refresh_expired"},
		session.ReasonRefreshReused:        {401, "refresh_reused"},
		session.ReasonSessionRevoked:       {401, "session_revoked"},
		session.ReasonSessionExpired:       {401, "session_expired"},
		session.ReasonPrincipalDeactivated: {401, "principal_deactivated"},
		session.ReasonStoreUnavailable:     {503, "store_unavailable"},
		session.ReasonDependency:           {503, "dependency_unavailable"},
	}
	vocabulary := map[string]bool{}
	for _, code := range contracts.AuthOutcomeReasonCodes {
		vocabulary[code] = true
	}
	for reason, want := range cases {
		f := newFixture(t)
		f.sessions.result = session.Result{Reason: reason}
		if reason == session.ReasonStoreUnavailable || reason == session.ReasonDependency {
			f.sessions.err = errors.New("store down")
		}
		response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
		expectError(t, response, want.status, want.reason)
		if !vocabulary[want.reason] {
			t.Fatalf("%s is not an auth-outcome.v1 reason code", want.reason)
		}
		if want.status == 503 && response.Header().Get("Retry-After") == "" {
			t.Fatalf("%s: a 503 without Retry-After", reason)
		}
	}
}

// The store refuses a session whose public user is inactive or gone under its
// lock (D6651) and names the locked session: the route's own read of the user
// decides the reason and revokes the session; a user the route reads active
// (a concurrent change) gets no token, revision_stale, and the session revoked.
func TestRefreshOfAStoreRefusedUserRevokesTheSession(t *testing.T) {
	for name, c := range map[string]struct {
		mutate  func(*fixture)
		status  int
		reason  string
		revokes bool
	}{
		"deactivated": {func(f *fixture) {
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
		}, 401, "principal_deactivated", true},
		"gone":             {func(f *fixture) { delete(f.users.users, f.principal) }, 401, "principal_not_found", true},
		"active on reread": {func(*fixture) {}, 401, "revision_stale", true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			c.mutate(f)
			f.sessions.result = session.Result{Reason: session.ReasonPrincipalDeactivated, Issued: session.Issued{Session: f.record}}
			response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
			expectError(t, response, c.status, c.reason)
			if revoked := len(f.sessions.revoked) == 1 && f.sessions.revoked[0] == f.record.ID; revoked != c.revokes || (!c.revokes && len(f.sessions.revoked) != 0) {
				t.Fatalf("revoked %v, want the session revoked = %v", f.sessions.revoked, c.revokes)
			}
		})
	}
}

func TestRefreshOfASessionWithoutAMembershipRefusesAndRevokes(t *testing.T) {
	f := newFixture(t)
	state := f.users.users[f.principal]
	state.IsSuperuser = false
	f.users.users[f.principal] = state
	f.users.roles = map[[2]uuid.UUID]*string{}
	f.build()
	f.sessions.result = session.Result{Reason: session.ReasonGraceReplay, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("answer %d %.200s, want 401", response.Code, response.Body)
	}
	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if body["reason_code"] != "not_entitled" {
		t.Fatalf("reason_code %v, want not_entitled", body["reason_code"])
	}
	if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
		t.Fatalf("revoked %v, want the session", f.sessions.revoked)
	}
}

// The membership check runs before the credential rotates: a removed member's
// live credential mints no successor.
func TestRefreshWithoutAMembershipRotatesNothing(t *testing.T) {
	for name, superuser := range map[string]bool{"member removed": false, "superuser without a membership": true} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			state := f.users.users[f.principal]
			state.IsSuperuser = superuser
			f.users.users[f.principal] = state
			f.users.roles = map[[2]uuid.UUID]*string{}
			f.build()
			f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
				Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
			response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
			if superuser {
				if response.Code != http.StatusOK || len(f.sessions.revoked) != 0 || f.sessions.minted != 1 {
					t.Fatalf("superuser: answer %d %.200s, revoked %v, minted %d; want 200, nothing revoked, 1 minted",
						response.Code, response.Body, f.sessions.revoked, f.sessions.minted)
				}
				return
			}
			var body map[string]any
			_ = json.Unmarshal(response.Body.Bytes(), &body)
			if response.Code != http.StatusUnauthorized || body["reason_code"] != "not_entitled" {
				t.Fatalf("answer %d %.200s, want 401 not_entitled", response.Code, response.Body)
			}
			if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
				t.Fatalf("revoked %v, want the session", f.sessions.revoked)
			}
			if f.sessions.minted != 0 {
				t.Fatalf("%d successors minted before the refusal, want 0", f.sessions.minted)
			}
		})
	}
}

// guardRefusingSessions refuses every exchange in its guard step, as the guard
// does when the session changed between the guard's read and the handler's.
type guardRefusingSessions struct{ *fakeSessions }

func (s guardRefusingSessions) ExchangeGuarded(context.Context, contracts.SecretString, session.Guard) (session.Result, error) {
	return session.Result{Issued: session.Issued{Session: s.result.Session}}, session.ErrGuardRefused
}

// A guard refusal that the handler's own reads no longer confirm still
// refuses: nothing was rotated, so no access token is answered.
func TestRefreshRefusedByTheGuardAnswersNoToken(t *testing.T) {
	f := newFixture(t)
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{Session: f.record}}
	routes, err := Routes(Deps{Sessions: guardRefusingSessions{f.sessions}, Users: f.users, Issuer: f.issuer,
		Logger: slog.New(slog.NewJSONHandler(f.logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if response.Code != http.StatusUnauthorized || body["reason_code"] != "revision_stale" || body["access_token"] != nil {
		t.Fatalf("answer %d %.200s, want 401 revision_stale and no token", response.Code, response.Body)
	}
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v, want nothing: the handler's reads allow the session", f.sessions.revoked)
	}
}

func ptr(s string) *string { return &s }

func TestRefreshOfAGoneOrDeactivatedUserRevokesTheSessionAndMintsNothing(t *testing.T) {
	for name, mutate := range map[string]func(*fixture){
		"deactivated": func(f *fixture) {
			u := f.users.users[f.principal]
			u.IsActive = false
			f.users.users[f.principal] = u
		},
		"gone": func(f *fixture) { delete(f.users.users, f.principal) },
	} {
		f := newFixture(t)
		mutate(f)
		f.build()
		f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
			Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
		response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
		want := "principal_deactivated"
		if name == "gone" {
			want = "principal_not_found"
		}
		expectError(t, response, 401, want)
		if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
			t.Fatalf("%s: revoked %v, want the session", name, f.sessions.revoked)
		}
		if f.sessions.actors[0].RequestID == nil {
			t.Fatalf("%s: the revocation carries no audit request id", name)
		}
	}
}

func TestRefreshRequestIsReadStrictly(t *testing.T) {
	good := credential()
	bodies := map[string]string{
		"empty":                "",
		"not an object":        `["x"]`,
		"wrong version":        `{"schema_version":"refresh-request.v2","refresh_credential":"` + good + `"}`,
		"missing version":      `{"refresh_credential":"` + good + `"}`,
		"missing credential":   `{"schema_version":"refresh-request.v1"}`,
		"unknown member":       `{"schema_version":"refresh-request.v1","refresh_credential":"` + good + `","x":1}`,
		"duplicate credential": `{"schema_version":"refresh-request.v1","refresh_credential":"` + good + `","refresh_credential":"` + good + `"}`,
		"refresh JWT":          refreshBody("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJlc2lnbmF0dXJl"),
		"too short":            refreshBody(good[:42]),
		"number credential":    `{"schema_version":"refresh-request.v1","refresh_credential":7}`,
		"trailing data":        refreshBody(good) + `{}`,
	}
	for name, body := range bodies {
		f := newFixture(t)
		f.sessions.err = errors.New("Exchange must not be reached")
		response := f.do(http.MethodPost, PatternRefresh, body, nil)
		expectError(t, response, 401, "malformed")
		if strings.Contains(response.Body.String(), good) || strings.Contains(f.logs.String(), good) {
			t.Fatalf("%s: the credential was echoed", name)
		}
	}
}

func TestSessionRoutesRefuseEveryBearerTheyCannotTrust(t *testing.T) {
	f := newFixture(t)
	foreignGrant := f.grant()
	foreignGrant.SessionID = uuid.New()
	otherGrant := f.grant()
	otherGrant.PrincipalID = uuid.New()
	// The retired HS256 class is the plane's one generic-401 cell
	// (TestEveryHS256TokenIsRefusedOnTheSessionRows); the EdDSA rows keep their
	// own reasons.
	cases := map[string]struct {
		header map[string]string
		reason string
	}{
		"no header":                    {nil, "malformed"},
		"basic scheme":                 {map[string]string{"Authorization": "Basic eDp5"}, "malformed"},
		"alg none":                     {map[string]string{"Authorization": "Bearer eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ4In0."}, "malformed"},
		"EdDSA unknown session":        {f.bearer(foreignGrant), "session_revoked"},
		"EdDSA another user's session": {f.bearer(otherGrant), "malformed"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			for _, route := range []struct{ method, path string }{
				{http.MethodGet, PatternCurrent}, {http.MethodPost, PatternRevoke}, {http.MethodPost, PatternRevokeAll},
			} {
				expectError(t, f.do(route.method, route.path, "", c.header), 401, c.reason)
			}
		})
	}
	if len(f.sessions.revoked)+len(f.sessions.revokedAll) != 0 {
		t.Fatal("a refused bearer revoked something")
	}
}

func TestSessionRoutesFollowTheLiveUserAndSessionState(t *testing.T) {
	cases := map[string]struct {
		mutate func(*fixture)
		status int
		reason string
	}{
		"token_version moved": {func(f *fixture) {
			u := f.users.users[f.principal]
			u.TokenVersion++
			f.users.users[f.principal] = u
		}, 401, "revision_stale"},
		"user deactivated": {func(f *fixture) {
			u := f.users.users[f.principal]
			u.IsActive = false
			f.users.users[f.principal] = u
		}, 401, "principal_deactivated"},
		"user gone": {func(f *fixture) { delete(f.users.users, f.principal) }, 401, "principal_not_found"},
		"session revoked": {func(f *fixture) {
			r := f.sessions.records[f.record.ID]
			at := time.Now()
			r.RevokedAt = &at
			f.sessions.records[f.record.ID] = r
		}, 401, "session_revoked"},
		"session expired": {func(f *fixture) {
			r := f.sessions.records[f.record.ID]
			r.ExpiresAt = time.Now().Add(-time.Second)
			f.sessions.records[f.record.ID] = r
		}, 401, "session_expired"},
		"store down":       {func(f *fixture) { f.sessions.currentErr = errors.New("down") }, 503, "store_unavailable"},
		"users store down": {func(f *fixture) { f.users.err = errors.New("down") }, 503, "store_unavailable"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			header := f.bearer(f.grant())
			c.mutate(f)
			f.build()
			expectError(t, f.do(http.MethodGet, PatternCurrent, "", header), c.status, c.reason)
		})
	}
}

func TestCurrentRevokeAndRevokeAllActOnTheBearersOwnSession(t *testing.T) {
	f := newFixture(t)
	header := f.bearer(f.grant())

	response := f.do(http.MethodGet, PatternCurrent, "", header)
	if response.Code != 200 {
		t.Fatalf("current: %d %s", response.Code, response.Body)
	}
	parsed, err := contracts.ParseSession(repoRoot(t), response.Body.Bytes())
	if err != nil {
		t.Fatalf("not session.v1: %v", err)
	}
	if parsed.SessionID != session.WireSessionID(f.record.ID) || parsed.PrincipalID != session.WirePrincipalID(f.principal) {
		t.Fatalf("session %+v", parsed)
	}

	if response := f.do(http.MethodPost, PatternRevoke, "", header); response.Code != http.StatusNoContent {
		t.Fatalf("revoke: %d %s", response.Code, response.Body)
	}
	if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
		t.Fatalf("revoked %v", f.sessions.revoked)
	}
	if response := f.do(http.MethodPost, PatternRevokeAll, "", header); response.Code != http.StatusNoContent {
		t.Fatalf("revoke-all: %d %s", response.Code, response.Body)
	}
	if len(f.sessions.revokedAll) != 1 || f.sessions.revokedAll[0] != f.principal {
		t.Fatalf("revoked all %v", f.sessions.revokedAll)
	}
	for _, actor := range f.sessions.actors {
		if actor.PrincipalID == nil || *actor.PrincipalID != f.principal || actor.RequestID == nil {
			t.Fatalf("actor %+v: want the bearer and an audit request id", actor)
		}
		if _, err := uuid.Parse(*actor.RequestID); err != nil {
			t.Fatalf("audit request id %q is not a UUID", *actor.RequestID)
		}
	}
}

func TestRefreshExpiresInStaysInsideTheSchema(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		at   time.Time
		want int
	}{
		{now.Add(-time.Hour), 1}, {now, 1}, {now.Add(1500 * time.Millisecond), 2},
		{now.Add(7 * 24 * time.Hour), 604800}, {now.Add(30 * 24 * time.Hour), 604800},
	} {
		if got := refreshExpiresIn(c.at, now); got != c.want {
			t.Errorf("refreshExpiresIn(%v) = %d, want %d", c.at.Sub(now), got, c.want)
		}
	}
}

func TestRoutesRequireTheirCollaborators(t *testing.T) {
	if _, err := Routes(Deps{}); err == nil {
		t.Fatal("Routes built with no collaborators")
	}
}

// A store failure is logged by class and SQLSTATE, never by its text: a pg
// error's Detail can hold a key value such as an email.
func TestStoreFailuresAreLoggedWithoutTheirText(t *testing.T) {
	f := newFixture(t)
	header := f.bearer(f.grant())
	detailValue := "key-value@example.com"
	f.users.err = fmt.Errorf("%w: %w", errUsersStore, &pgconn.PgError{Code: "23505", Detail: "Key (email)=(" + detailValue + ") already exists."})
	f.build()
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", header), 503, "store_unavailable")
	logs := f.logs.String()
	if strings.Contains(logs, detailValue) || strings.Contains(logs, "already exists") {
		t.Fatalf("the log carries the error text: %s", logs)
	}
	if !strings.Contains(logs, `"failure":"users_store"`) || !strings.Contains(logs, `"code":"23505"`) {
		t.Fatalf("the log lacks the error class and SQLSTATE: %s", logs)
	}
}
