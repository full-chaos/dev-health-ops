package authapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	apisession "github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// switchRows is the endpoint-profile document with one auth-service row for
// the switch route accepting classes; the real row is PR-2's (D6617).
func switchRows(classes ...string) []byte {
	quoted, _ := json.Marshal(classes)
	return []byte(`{"rows":[{"service":"` + AccessService + `","method":"POST","route":"` + PatternCompatSwitchOrg +
		`","accepted_credential_classes":` + string(quoted) + `}]}`)
}

// fakeSwitchReads are the in-transaction reads, over maps. calls names each
// read in order.
type fakeSwitchReads struct {
	mu                        sync.Mutex
	users                     map[uuid.UUID]UserState
	orgs                      map[uuid.UUID]bool
	roles                     map[[2]uuid.UUID]*string
	stateErr, orgErr, roleErr error
	calls                     []string
}

func (f *fakeSwitchReads) StateOn(_ context.Context, _ session.Queryer, id uuid.UUID) (UserState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "state")
	if f.stateErr != nil {
		return UserState{}, false, f.stateErr
	}
	user, ok := f.users[id]
	return user, ok, nil
}

func (f *fakeSwitchReads) OrganizationActiveOn(_ context.Context, _ session.Queryer, org uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "organization")
	if f.orgErr != nil {
		return false, f.orgErr
	}
	return f.orgs[org], nil
}

func (f *fakeSwitchReads) MemberRoleLockedOn(_ context.Context, _ session.Queryer, user, org uuid.UUID) (*string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "membership")
	if f.roleErr != nil {
		return nil, false, f.roleErr
	}
	role, ok := f.roles[[2]uuid.UUID{user, org}]
	return role, ok, nil
}

// switchCall is one CreateSupersedingGuarded call.
type switchCall struct {
	principal   uuid.UUID
	org         *uuid.UUID
	amr         []string
	predecessor uuid.UUID
	event       audit.AuditEvent
}

// fakeSwitchSessions runs the guard as the store does: on the predecessor's
// record, after its checks and before anything is written. before is
// returned before the guard (a predecessor refusal), after once it passed
// (a failed insert). minted counts the successors created.
type fakeSwitchSessions struct {
	mu            sync.Mutex
	records       *fakeSessions
	before, after error
	calls         []switchCall
	minted        []session.Issued
}

func (f *fakeSwitchSessions) CreateSupersedingGuarded(ctx context.Context, principal uuid.UUID, org *uuid.UUID, amr []string,
	predecessor uuid.UUID, event audit.AuditEvent, guard session.Guard) (session.Issued, error) {
	f.mu.Lock()
	f.calls = append(f.calls, switchCall{principal: principal, org: org, amr: amr, predecessor: predecessor, event: event})
	f.mu.Unlock()
	if f.before != nil {
		return session.Issued{}, f.before
	}
	record, err := f.records.Current(ctx, predecessor)
	if err != nil {
		return session.Issued{}, session.ErrPredecessorNotLive
	}
	if err := guard(ctx, nil, record); errors.Is(err, session.ErrGuardRefused) {
		return session.Issued{}, session.ErrGuardRefused
	} else if err != nil {
		return session.Issued{}, fmt.Errorf("%w: superseding a session: %w", session.ErrStore, err)
	}
	if f.after != nil {
		return session.Issued{}, f.after
	}
	now := time.Now().UTC()
	issued := session.Issued{
		Session: session.Record{ID: uuid.New(), PrincipalID: principal, OrganizationID: org, Revision: 1, AuthTime: now,
			AMR: amr, CreatedAt: now, ExpiresAt: now.Add(7 * 24 * time.Hour)},
		Credential: contracts.NewSecretString("dhr1_" + strings.Repeat("s", 43)), ExpiresAt: now.Add(7 * 24 * time.Hour),
	}
	f.mu.Lock()
	f.minted = append(f.minted, issued)
	f.mu.Unlock()
	return issued, nil
}

// switchFixture serves the switch route over fakes: a user, member of A
// (admin) and B (member), whose live session (the predecessor) is in A.
type switchFixture struct {
	t                     *testing.T
	principal, orgA, orgB uuid.UUID
	record                session.Record
	sessions              *fakeSessions
	users                 fakeUsers
	reads                 *fakeSwitchReads
	store                 *fakeSwitchSessions
	projector             *fakeProjector
	issuer                *Issuer
	logs                  *bytes.Buffer
	counters              httpapi.CounterStore
	document              []byte
	server                http.Handler
}

func newSwitchFixture(t *testing.T) *switchFixture {
	t.Helper()
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	return newSwitchFixtureWith(t, issuer)
}

func newSwitchFixtureWith(t *testing.T, issuer *Issuer) *switchFixture {
	t.Helper()
	principal, orgA, orgB := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	pin := int64(4)
	record := session.Record{ID: uuid.New(), PrincipalID: principal, OrganizationID: &orgA, Revision: 1,
		AuthTime: now.Add(-time.Hour), AMR: []string{"pwd", "mfa"}, CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(7 * 24 * time.Hour),
		LegacyTokenVersion: &pin}
	admin, member, username := "admin", "member", "ada"
	user := UserState{Email: "ada@example.com", Username: &username, IsActive: true, TokenVersion: 4}
	roles := map[[2]uuid.UUID]*string{{principal, orgA}: &admin, {principal, orgB}: &member}
	sessions := &fakeSessions{records: map[uuid.UUID]session.Record{record.ID: record}}
	f := &switchFixture{
		t: t, principal: principal, orgA: orgA, orgB: orgB, record: record, sessions: sessions,
		users:     fakeUsers{users: map[uuid.UUID]UserState{principal: user}, roles: map[[2]uuid.UUID]*string{{principal, orgA}: &admin, {principal, orgB}: &member}},
		reads:     &fakeSwitchReads{users: map[uuid.UUID]UserState{principal: user}, orgs: map[uuid.UUID]bool{orgA: true, orgB: true}, roles: roles},
		store:     &fakeSwitchSessions{records: sessions},
		projector: &fakeProjector{orgs: map[uuid.UUID]bool{orgA: true, orgB: true}, gone: map[uuid.UUID]bool{}},
		issuer:    issuer, logs: &bytes.Buffer{}, counters: httpapi.NewMemoryCounters(nil),
		document: switchRows("ops_access_token_hs256"),
	}
	f.build()
	return f
}

func (f *switchFixture) deps() CompatDeps {
	return CompatDeps{
		Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Projector: f.projector,
		Logger:      slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		AppendAudit: func(context.Context, audit.AuditEvent) error { return nil },
		Limits:      mustLimits(f.t, f.counters),
		SwitchOrg:   &SwitchOrgConfig{Sessions: f.store, Reads: f.reads},
	}
}

func (f *switchFixture) build() {
	f.t.Helper()
	routes, err := compatRoutes(f.deps(), f.document)
	if err != nil {
		f.t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		route.ResponseModel = true
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	f.server = mux
}

func (f *switchFixture) post(body string, header map[string]string) *httptest.ResponseRecorder {
	return f.postTo(PatternCompatSwitchOrg, body, header)
}

func (f *switchFixture) postTo(target, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	request.Body = http.MaxBytesReader(httptest.NewRecorder(), request.Body, 1<<20)
	response := httptest.NewRecorder()
	f.server.ServeHTTP(response, request)
	return response
}

func (f *switchFixture) grant() Grant {
	return Grant{PrincipalID: f.principal, OrganizationID: &f.orgA, Role: "admin", SessionID: f.record.ID, User: f.users.users[f.principal]}
}

func (f *switchFixture) bearerOf(grant Grant) map[string]string {
	f.t.Helper()
	compact, err := f.issuer.Access(grant)
	if err != nil {
		f.t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + compact}
}

func (f *switchFixture) bearer() map[string]string { return f.bearerOf(f.grant()) }

func orgBody(org uuid.UUID) string { return `{"org_id":"` + org.String() + `"}` }

// nothingMinted fails when the switch reached the store's write or minted a
// successor.
func (f *switchFixture) nothingMinted(name string) {
	f.t.Helper()
	if len(f.store.minted) != 0 {
		f.t.Errorf("%s: %d successor sessions minted", name, len(f.store.minted))
	}
}

const (
	switchNotMember  = `{"detail":{"message":"User is not a member of the selected organization"}}`
	switchNotAllowed = `{"detail":{"message":"Organization switch is not allowed for this session"}}`
	switchInvalidOrg = `{"detail":{"message":"Invalid organization ID"}}`
	switchBadToken   = `{"detail":{"message":"Invalid or expired token"}}`
	switchNoAuth     = `{"detail":{"message":"Not authenticated"}}`
	switchBadHeader  = `{"detail":{"message":"Invalid authorization header"}}`
	switchDown       = `{"detail":{"message":"Authentication service unavailable"}}`
)

// switchClaims decodes a compact JWS's payload (no verification: the issuer
// signed it in this process).
func switchClaims(t *testing.T, compact string) map[string]any {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("not a compact JWS: %d parts", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

// memberOrder is the member names of a JSON object in document order.
func memberOrder(t *testing.T, raw []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %s", raw)
	}
	var names []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, tok.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return names
}

// A member of B, signed in to A, switches to B: the bearer's own session is
// superseded by a session in B, minted for B with the role read in the
// switch, and the answer is go-api's LoginResponse for B.
func TestSwitchOrgHappyPath(t *testing.T) {
	f := newSwitchFixture(t)
	r := f.post(orgBody(f.orgB), f.bearer())
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	if len(f.store.calls) != 1 || len(f.store.minted) != 1 {
		t.Fatalf("store calls %d, minted %d, want 1 and 1", len(f.store.calls), len(f.store.minted))
	}
	call, issued := f.store.calls[0], f.store.minted[0]
	if call.principal != f.principal || call.org == nil || *call.org != f.orgB || call.predecessor != f.record.ID {
		t.Fatalf("store call %+v, want the bearer's principal and session and org B", call)
	}
	if strings.Join(call.amr, ",") != "pwd,mfa" {
		t.Fatalf("successor amr %v, want the predecessor's", call.amr)
	}
	if call.event.EventType != EventSessionIssued || call.event.OrganizationID == nil || *call.event.OrganizationID != f.orgB.String() {
		t.Fatalf("issue event %+v", call.event)
	}
	var answer struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		User         struct {
			OrgID string `json:"org_id"`
			Role  string `json:"role"`
		} `json:"user"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if answer.RefreshToken != issued.Credential.Reveal() || answer.User.OrgID != f.orgB.String() || answer.User.Role != "member" {
		t.Fatalf("answer %s", r.Body)
	}
	claims := switchClaims(t, answer.AccessToken)
	if claims["org_id"] != f.orgB.String() || claims["role"] != "member" || claims["sid"] != session.WireSessionID(issued.Session.ID) {
		t.Fatalf("access claims org=%v role=%v sid=%v, want B, member and the successor", claims["org_id"], claims["role"], claims["sid"])
	}
	if got := strings.Join(f.reads.calls, ","); got != "state,organization,membership" {
		t.Fatalf("in-transaction reads %q, want state,organization,membership", got)
	}
	if r.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control %q, want no-store on a token answer", r.Header().Get("Cache-Control"))
	}
}

// O-A1: the body is go-api's SwitchOrgRequest, strict: a body that is not an
// object or lacks a string org_id is go-api's 422; an extra member is
// pydantic's extra_forbidden 422; a member named twice, a value that is not a
// canonical lowercase hyphenated UUID, is go-api's 400. Nothing is minted.
func TestSwitchOrgBodyStrict(t *testing.T) {
	f := newSwitchFixture(t)
	b := f.orgB.String()
	cases := []struct {
		name, body string
		status     int
		want       string
	}{
		{"not an object", `["` + b + `"]`, http.StatusUnprocessableEntity, `{"detail":[{"type":"model_attributes_type"`},
		{"missing", `{}`, http.StatusUnprocessableEntity, `{"detail":[{"type":"missing","loc":["body","org_id"]`},
		{"not a string", `{"org_id":5}`, http.StatusUnprocessableEntity, `{"detail":[{"type":"string_type","loc":["body","org_id"]`},
		{"extra member", `{"org_id":"` + b + `","role":"owner"}`, http.StatusUnprocessableEntity,
			`{"detail":[{"type":"extra_forbidden","loc":["body","role"],"msg":"Extra inputs are not permitted"`},
		{"predecessor named", `{"org_id":"` + b + `","predecessor_session_id":"` + uuid.NewString() + `"}`, http.StatusUnprocessableEntity,
			`{"detail":[{"type":"extra_forbidden","loc":["body","predecessor_session_id"]`},
		{"duplicate member", `{"org_id":"` + f.orgA.String() + `","org_id":"` + b + `"}`, http.StatusBadRequest, switchInvalidOrg},
		{"duplicate by escape", `{"org_id":"` + f.orgA.String() + `","org_id":"` + b + `"}`, http.StatusBadRequest, switchInvalidOrg},
		{"not a UUID", `{"org_id":"acme"}`, http.StatusBadRequest, switchInvalidOrg},
		{"empty", `{"org_id":""}`, http.StatusBadRequest, switchInvalidOrg},
		{"upper case", `{"org_id":"` + strings.ToUpper(b) + `"}`, http.StatusBadRequest, switchInvalidOrg},
		{"no hyphens", `{"org_id":"` + strings.ReplaceAll(b, "-", "") + `"}`, http.StatusBadRequest, switchInvalidOrg},
		{"urn form", `{"org_id":"urn:uuid:` + b + `"}`, http.StatusBadRequest, switchInvalidOrg},
		{"braces", `{"org_id":"{` + b + `}"}`, http.StatusBadRequest, switchInvalidOrg},
	}
	for _, c := range cases {
		r := f.post(c.body, f.bearer())
		if r.Code != c.status || !strings.HasPrefix(r.Body.String(), c.want) {
			t.Errorf("%s: %d %s, want %d %s", c.name, r.Code, r.Body, c.status, c.want)
		}
	}
	if len(f.store.calls) != 0 {
		t.Fatalf("%d store calls for refused bodies", len(f.store.calls))
	}
	f.nothingMinted("refused bodies")
	if r := f.post(`{"org_id":"`+b+`"}`, f.bearer()); r.Code != http.StatusOK {
		t.Fatalf("positive control: %d %s", r.Code, r.Body)
	}
}

// O-A2: the session superseded is the bearer's own (its sid), never one the
// request names: a body that names one is refused, and a header or query that
// names one is not read.
func TestSwitchOrgPredecessorIsTheBearersSession(t *testing.T) {
	f := newSwitchFixture(t)
	other := uuid.New()
	now := time.Now().UTC()
	pin := int64(4)
	f.sessions.records[other] = session.Record{ID: other, PrincipalID: f.principal, OrganizationID: &f.orgA, Revision: 1,
		AuthTime: now, AMR: []string{"pwd"}, CreatedAt: now, ExpiresAt: now.Add(time.Hour), LegacyTokenVersion: &pin}
	for _, body := range []string{
		`{"org_id":"` + f.orgB.String() + `","session_id":"` + other.String() + `"}`,
		`{"org_id":"` + f.orgB.String() + `","predecessor_session_id":"` + other.String() + `"}`,
	} {
		if r := f.post(body, f.bearer()); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("a body naming a session: %d %s", r.Code, r.Body)
		}
	}
	header := f.bearer()
	header["X-Session-Id"] = other.String()
	r := f.postTo(PatternCompatSwitchOrg+"?session_id="+other.String()+"&predecessor_session_id="+other.String(), orgBody(f.orgB), header)
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	if len(f.store.calls) != 1 || f.store.calls[0].predecessor != f.record.ID {
		t.Fatalf("store calls %+v, want one with the bearer's sid %s", f.store.calls, f.record.ID)
	}
}

// O-A3: the organization is only the body's org_id: a header, a query or the
// bearer's own org claim never chooses it.
func TestSwitchOrgNeverReadsOrgFromAnyOtherInput(t *testing.T) {
	f := newSwitchFixture(t)
	header := f.bearer()
	header["X-Org-Id"] = f.orgA.String()
	r := f.postTo(PatternCompatSwitchOrg+"?org_id="+f.orgA.String(), orgBody(f.orgB), header)
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	if call := f.store.calls[0]; call.org == nil || *call.org != f.orgB {
		t.Fatalf("switched to %v, want the body's org B", call.org)
	}
	stranger := uuid.New()
	f.projector.orgs[stranger] = true
	f.reads.orgs[stranger] = true
	header = f.bearerOf(Grant{PrincipalID: f.principal, OrganizationID: &stranger, Role: "owner", SessionID: f.record.ID, User: f.users.users[f.principal]})
	if r := f.post(orgBody(stranger), header); r.Code != http.StatusForbidden || r.Body.String() != switchNotMember {
		t.Fatalf("an org claim in the bearer chose the org: %d %s", r.Code, r.Body)
	}
	if len(f.store.minted) != 1 {
		t.Fatalf("%d successors, want only the first switch's", len(f.store.minted))
	}
}

// O-C1..O-C6: every refusal answers go-api's status and text with a closed
// reason, mints nothing and creates no session.
func TestSwitchOrgRefusalTable(t *testing.T) {
	type arrange func(f *switchFixture) map[string]string
	cases := []struct {
		name    string
		arrange arrange
		status  int
		body    string
		reason  string
	}{
		{"removed member (pool read)", func(f *switchFixture) map[string]string {
			delete(f.users.roles, [2]uuid.UUID{f.principal, f.orgB})
			delete(f.reads.roles, [2]uuid.UUID{f.principal, f.orgB})
			return f.bearer()
		}, http.StatusForbidden, switchNotMember, reasonNotEntitled},
		{"removed member (in the transaction)", func(f *switchFixture) map[string]string {
			delete(f.reads.roles, [2]uuid.UUID{f.principal, f.orgB})
			return f.bearer()
		}, http.StatusForbidden, switchNotMember, reasonNotEntitled},
		{"inactive organization", func(f *switchFixture) map[string]string {
			f.reads.orgs[f.orgB] = false
			return f.bearer()
		}, http.StatusForbidden, switchNotMember, reasonNotEntitled},
		{"organization not projected", func(f *switchFixture) map[string]string {
			delete(f.projector.orgs, f.orgB)
			return f.bearer()
		}, http.StatusForbidden, switchNotMember, reasonNotEntitled},
		{"deactivated user (bearer check)", func(f *switchFixture) map[string]string {
			header := f.bearer()
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
			return header
		}, http.StatusUnauthorized, switchBadToken, reasonDeactivated},
		{"deactivated user (in the transaction)", func(f *switchFixture) map[string]string {
			user := f.reads.users[f.principal]
			user.IsActive = false
			f.reads.users[f.principal] = user
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonDeactivated},
		{"user not found (in the transaction)", func(f *switchFixture) map[string]string {
			delete(f.reads.users, f.principal)
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonPrincipalNotFound},
		{"user not projected", func(f *switchFixture) map[string]string {
			f.projector.gone[f.principal] = true
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonPrincipalNotFound},
		{"revoked session", func(f *switchFixture) map[string]string {
			record := f.sessions.records[f.record.ID]
			at := time.Now()
			record.RevokedAt = &at
			f.sessions.records[f.record.ID] = record
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonSessionRevoked},
		{"expired session", func(f *switchFixture) map[string]string {
			record := f.sessions.records[f.record.ID]
			record.ExpiresAt = time.Now().Add(-time.Minute)
			f.sessions.records[f.record.ID] = record
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonSessionExpired},
		{"unknown session", func(f *switchFixture) map[string]string {
			header := f.bearer()
			delete(f.sessions.records, f.record.ID)
			return header
		}, http.StatusUnauthorized, switchBadToken, reasonSessionRevoked},
		{"predecessor ended in the race", func(f *switchFixture) map[string]string {
			f.store.before = session.ErrPredecessorNotLive
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonSessionRevoked},
		{"stale token version (bearer)", func(f *switchFixture) map[string]string {
			grant := f.grant()
			grant.User.TokenVersion = 3
			return f.bearerOf(grant)
		}, http.StatusUnauthorized, switchBadToken, reasonRevisionStale},
		{"stale pin (in the transaction)", func(f *switchFixture) map[string]string {
			user := f.reads.users[f.principal]
			user.TokenVersion = 5
			f.reads.users[f.principal] = user
			return f.bearer()
		}, http.StatusUnauthorized, switchBadToken, reasonRevisionStale},
		{"delegated session", func(f *switchFixture) map[string]string {
			record := f.sessions.records[f.record.ID]
			record.Delegation = &session.Delegation{Actor: uuid.New(), Reason: "support"}
			f.sessions.records[f.record.ID] = record
			return f.bearer()
		}, http.StatusForbidden, switchNotAllowed, reasonNotEntitled},
		{"impersonation bearer", func(f *switchFixture) map[string]string {
			grant := f.grant()
			actor := uuid.New()
			grant.Actor = &actor
			return f.bearerOf(grant)
		}, http.StatusForbidden, switchNotAllowed, reasonNotEntitled},
		{"service account user", func(f *switchFixture) map[string]string {
			user := f.reads.users[f.principal]
			provider := "service"
			user.AuthProvider = &provider
			f.reads.users[f.principal] = user
			return f.bearer()
		}, http.StatusForbidden, switchNotAllowed, reasonNotEntitled},
		{"superuser without the membership", func(f *switchFixture) map[string]string {
			for _, users := range []map[uuid.UUID]UserState{f.users.users, f.reads.users} {
				user := users[f.principal]
				user.IsSuperuser = true
				users[f.principal] = user
			}
			delete(f.users.roles, [2]uuid.UUID{f.principal, f.orgB})
			delete(f.reads.roles, [2]uuid.UUID{f.principal, f.orgB})
			return f.bearer()
		}, http.StatusForbidden, switchNotMember, reasonNotEntitled},
		{"no bearer", func(f *switchFixture) map[string]string { return nil }, http.StatusUnauthorized, switchNoAuth, reasonMalformed},
		{"not a bearer", func(f *switchFixture) map[string]string {
			return map[string]string{"Authorization": "Basic YWRhOnB3"}
		}, http.StatusUnauthorized, switchBadHeader, reasonMalformed},
		{"forged bearer", func(f *switchFixture) map[string]string {
			return map[string]string{"Authorization": "Bearer " + strings.Repeat("x", 20) + "." + strings.Repeat("y", 20) + "." + strings.Repeat("z", 20)}
		}, http.StatusUnauthorized, switchBadToken, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newSwitchFixture(t)
			header := c.arrange(f)
			f.build()
			f.logs.Reset()
			r := f.post(orgBody(f.orgB), header)
			if r.Code != c.status || r.Body.String() != c.body {
				t.Fatalf("%d %s, want %d %s", r.Code, r.Body, c.status, c.body)
			}
			if c.status == http.StatusUnauthorized && r.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("401 without WWW-Authenticate: Bearer")
			}
			if c.reason != "" && !strings.Contains(f.logs.String(), `"operation":"switch_org","result":"denied","reason_code":"`+c.reason+`"`) {
				t.Fatalf("no denied %s line: %s", c.reason, f.logs)
			}
			f.nothingMinted(c.name)
		})
	}
}

// O-B1, O-B2: the decision is the in-transaction reads', not the bearer's
// claims or the first read on the pool: a membership, an organization or a
// user that changed between them refuses the switch.
func TestSwitchOrgDecidesOnTheInTransactionReads(t *testing.T) {
	f := newSwitchFixture(t)
	delete(f.reads.roles, [2]uuid.UUID{f.principal, f.orgB})
	if r := f.post(orgBody(f.orgB), f.bearer()); r.Code != http.StatusForbidden {
		t.Fatalf("membership gone after the first read: %d %s", r.Code, r.Body)
	}
	f = newSwitchFixture(t)
	admin := "owner"
	f.reads.roles[[2]uuid.UUID{f.principal, f.orgB}] = &admin
	r := f.post(orgBody(f.orgB), f.bearer())
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	var answer struct {
		AccessToken string `json:"access_token"`
		User        struct {
			Role  string `json:"role"`
			Email string `json:"email"`
		} `json:"user"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if answer.User.Role != "owner" || switchClaims(t, answer.AccessToken)["role"] != "owner" {
		t.Fatalf("role %q / claim %v, want the in-transaction read's owner", answer.User.Role, switchClaims(t, answer.AccessToken)["role"])
	}
	f = newSwitchFixture(t)
	user := f.reads.users[f.principal]
	user.Email = "renamed@example.com"
	f.reads.users[f.principal] = user
	r = f.post(orgBody(f.orgB), f.bearer())
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil || answer.User.Email != "renamed@example.com" {
		t.Fatalf("user %q, want the in-transaction read's (%v): %s", answer.User.Email, err, r.Body)
	}
}

// O-C7: a read that fails is go-api's 503 (a dependency) and nothing is
// minted; never a pass.
func TestSwitchOrgStoreFaultIs503(t *testing.T) {
	down := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	cases := []struct {
		name    string
		arrange func(f *switchFixture)
		body    string
	}{
		{"membership pool read", func(f *switchFixture) { f.users.roleErr = down }, switchDown},
		{"user state in the transaction", func(f *switchFixture) { f.reads.stateErr = down }, switchDown},
		{"organization in the transaction", func(f *switchFixture) { f.reads.orgErr = down }, switchDown},
		{"membership in the transaction", func(f *switchFixture) { f.reads.roleErr = down }, switchDown},
		{"projection", func(f *switchFixture) { f.projector.orgErr = down }, switchDown},
		{"session write", func(f *switchFixture) { f.store.after = fmt.Errorf("%w: inserting: %w", session.ErrStore, down) }, switchDown},
		// The bearer's session read is go-api's guard: its own 503 text.
		{"session read", func(f *switchFixture) { f.sessions.currentErr = down }, `{"detail":{"message":"Database temporarily unavailable"}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newSwitchFixture(t)
			header := f.bearer()
			c.arrange(f)
			f.build()
			r := f.post(orgBody(f.orgB), header)
			if r.Code != http.StatusServiceUnavailable || r.Body.String() != c.body {
				t.Fatalf("%d %s, want 503 %s", r.Code, r.Body, c.body)
			}
			f.nothingMinted(c.name)
		})
	}
}

// The answer is go-api's LoginResponse: its members in its order, with
// session_access_token last under the eddsa class only.
func TestSwitchOrgAnswerShape(t *testing.T) {
	want := "access_token,refresh_token,token_type,expires_in,needs_onboarding,user"
	wantUser := "id,email,username,full_name,org_id,role,is_superuser"
	f := newSwitchFixture(t)
	r := f.post(orgBody(f.orgB), f.bearer())
	if got := strings.Join(memberOrder(t, r.Body.Bytes()), ","); r.Code != http.StatusOK || got != want {
		t.Fatalf("hs256_compat members %q (%d), want %q", got, r.Code, want)
	}
	var answer map[string]json.RawMessage
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(memberOrder(t, answer["user"]), ","); got != wantUser {
		t.Fatalf("user members %q, want %q", got, wantUser)
	}
	if string(answer["token_type"]) != `"bearer"` || string(answer["needs_onboarding"]) != "false" ||
		string(answer["expires_in"]) != fmt.Sprint(f.issuer.ExpiresIn()) {
		t.Fatalf("answer %s", r.Body)
	}
	if !strings.Contains(string(answer["user"]), `"username":"ada","full_name":null,"org_id":"`+f.orgB.String()+`","role":"member","is_superuser":false`) {
		t.Fatalf("user %s", answer["user"])
	}

	signer, _, _ := fileSigner(t)
	e := newSwitchFixtureWith(t, newEdDSAIssuer(t, signer, testAudience))
	legacy, err := edgetoken.NewSigner(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	sid := session.WireSessionID(e.record.ID)
	compact, err := legacy.Access(edgetoken.AccessClaims{UserID: e.principal.String(), Email: "ada@example.com", OrgID: e.orgA.String(),
		Role: "admin", TokenVersion: 4, SessionID: &sid}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	r = e.post(orgBody(e.orgB), map[string]string{"Authorization": "Bearer " + compact})
	if got := strings.Join(memberOrder(t, r.Body.Bytes()), ","); r.Code != http.StatusOK || got != want+",session_access_token" {
		t.Fatalf("eddsa members %q (%d %s), want %q", got, r.Code, r.Body, want+",session_access_token")
	}
}

// O-E2: the budget is per signed-in principal, by digest: one principal's
// burst does not spend another's, and the key holds no principal id.
func TestSwitchOrgLimiterKeyIsPrincipalDigest(t *testing.T) {
	f := newSwitchFixture(t)
	for i := 0; i < 3; i++ {
		if r := f.post(`{"org_id":"nope"}`, f.bearer()); r.Code != http.StatusBadRequest {
			t.Fatalf("a refused body: %d %s", r.Code, r.Body)
		}
	}
	// A refused body is answered before the budget (validate, then limit, as
	// change-password); a switch the reads refuse is counted.
	for i := 0; i < switchOrgLimit.Count; i++ {
		if r := f.post(orgBody(uuid.New()), f.bearer()); r.Code != http.StatusForbidden {
			t.Fatalf("switch %d: %d %s", i+1, r.Code, r.Body)
		}
	}
	expectAnswer(t, "one over the count", f.post(orgBody(f.orgB), f.bearer()), http.StatusTooManyRequests,
		`{"detail":{"message":"Rate limit exceeded. Please try again later."}}`)
	f.nothingMinted("over the count")

	other := uuid.New()
	now := time.Now().UTC()
	pin := int64(4)
	otherSession := session.Record{ID: uuid.New(), PrincipalID: other, OrganizationID: &f.orgA, Revision: 1, AuthTime: now,
		AMR: []string{"pwd"}, CreatedAt: now, ExpiresAt: now.Add(time.Hour), LegacyTokenVersion: &pin}
	f.sessions.records[otherSession.ID] = otherSession
	user := f.users.users[f.principal]
	f.users.users[other], f.reads.users[other] = user, user
	member := "member"
	f.users.roles[[2]uuid.UUID{other, f.orgB}], f.reads.roles[[2]uuid.UUID{other, f.orgB}] = &member, &member
	header := f.bearerOf(Grant{PrincipalID: other, OrganizationID: &f.orgA, Role: "member", SessionID: otherSession.ID, User: user})
	if r := f.post(orgBody(f.orgB), header); r.Code != http.StatusOK {
		t.Fatalf("another principal has its own budget: %d %s", r.Code, r.Body)
	}
	digest := mustLimits(t, f.counters).digest("principal", f.principal.String())
	g := newSwitchFixture(t)
	spy := &keySpy{CounterStore: g.counters}
	g.counters = spy
	g.build()
	if r := g.post(orgBody(g.orgB), g.bearer()); r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	digest = mustLimits(t, spy).digest("principal", g.principal.String())
	if len(spy.keys) != 1 || spy.keys[0] != "switch-org:"+digest || strings.Contains(spy.keys[0], g.principal.String()) {
		t.Fatalf("limit keys %q, want one switch-org:<principal digest> with no principal id", spy.keys)
	}
}

// keySpy records the key of every counted hit.
type keySpy struct {
	httpapi.CounterStore
	mu   sync.Mutex
	keys []string
}

func (s *keySpy) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	s.mu.Lock()
	s.keys = append(s.keys, hit.Key)
	s.mu.Unlock()
	return s.CounterStore.Increment(ctx, hit)
}

// The budget against how a switch is used (D6418: a ratio, no clock): a
// switch is user-paced, so the window's count allows two switches a minute
// (A to B and back), and stays well under a scripted burst.
func TestSwitchOrgBudgetRatio(t *testing.T) {
	perMinute := float64(switchOrgLimit.Count) / switchOrgLimit.Window.Minutes()
	if perMinute < 2 || perMinute > 5 {
		t.Fatalf("budget %d per %s is %.2f a minute, want 2 to 5", switchOrgLimit.Count, switchOrgLimit.Window, perMinute)
	}
	if switchOrgLimit.Count != 30 || switchOrgLimit.Window != 15*time.Minute || switchOrgLimit.ID != "auth_svc_switch_org" {
		t.Fatalf("limit %+v, want pm's 30 per 15 min (Q4)", switchOrgLimit)
	}
}

// O-E1: no bearer, refresh credential or body org reaches a log line or a
// metric label.
func TestSwitchOrgNoSecretInLogs(t *testing.T) {
	f := newSwitchFixture(t)
	header := f.bearer()
	token := strings.TrimPrefix(header["Authorization"], "Bearer ")
	r := f.post(orgBody(f.orgB), header)
	if r.Code != http.StatusOK {
		t.Fatalf("switch: %d %s", r.Code, r.Body)
	}
	refresh := f.store.minted[0].Credential.Reveal()
	delete(f.reads.roles, [2]uuid.UUID{f.principal, f.orgB})
	stranger := uuid.New()
	f.post(orgBody(f.orgB), header)
	f.post(orgBody(stranger), header)
	f.post(`{"org_id":"`+strings.ToUpper(stranger.String())+`"}`, header)
	f.post(`{"org_id":"x","org_id":"`+stranger.String()+`"}`, header)
	logs := f.logs.String()
	for name, secret := range map[string]string{"bearer": token, "refresh credential": refresh, "org B": f.orgB.String(),
		"stranger org": stranger.String(), "stranger org upper": strings.ToUpper(stranger.String())} {
		if strings.Contains(logs, secret) {
			t.Errorf("the %s is in a log line", name)
		}
	}
	if strings.Contains(logs, token[len(token)-12:]) {
		t.Errorf("a bearer fragment is in a log line")
	}
}

// O-F1: the route mounts only when the endpoint-profile document holds its
// row with the classes the issuer verifies. The embedded document holds it
// (PR-2), so CompatRoutes mounts the route once, with no not-mounted line.
func TestSwitchOrgNotMountedWithoutRow(t *testing.T) {
	f := newSwitchFixture(t)
	if r := f.post(orgBody(f.orgB), f.bearer()); r.Code != http.StatusOK {
		t.Fatalf("positive control: mounted on its row: %d %s", r.Code, r.Body)
	}
	f.logs.Reset()
	routes, err := CompatRoutes(f.deps())
	if err != nil {
		t.Fatal(err)
	}
	mounted := 0
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == PatternCompatSwitchOrg {
			mounted++
		}
	}
	if mounted != 1 {
		t.Fatalf("mounted %d times on the embedded document, want 1", mounted)
	}
	if strings.Contains(f.logs.String(), "auth switch-org route not mounted") {
		t.Fatalf("not-mounted line with the embedded row present: %s", f.logs)
	}
	for name, document := range map[string][]byte{
		"empty":                   []byte(`{"rows":[]}`),
		"another route's row":     validateRowsHS256,
		"a row of no HS256 class": switchRows("user_access_token"),
	} {
		f.document = document
		f.build()
		if r := f.post(orgBody(f.orgB), f.bearer()); r.Code != http.StatusNotFound {
			t.Fatalf("%s document: %d %s", name, r.Code, r.Body)
		}
	}
	deps := f.deps()
	deps.SwitchOrg = &SwitchOrgConfig{Sessions: f.store}
	if _, err := compatRoutes(deps, switchRows("ops_access_token_hs256")); err == nil {
		t.Fatal("a switch-org config without its reads built")
	}
}

// go-api's own switch-org handler and this route answer the refusals they
// share with the same status, content type and bytes: the body refusals go-api
// makes before its transaction and its guard's bearer refusals.
func TestSwitchOrgSharedRefusalsAreGoAPIsBytes(t *testing.T) {
	v := newValidateFixture(t, false)
	f := newSwitchFixture(t)
	f.principal, f.record = v.principal, v.record
	f.sessions.records = map[uuid.UUID]session.Record{v.record.ID: v.record}
	f.users = v.users
	f.build()
	goAPI := goAPISwitchHandler(t, v)
	token := v.hs256Token(nil)
	bearer := map[string]string{"Authorization": "Bearer " + token}
	for _, c := range []struct {
		name, body string
		header     map[string]string
	}{
		{"missing org_id", `{}`, bearer},
		{"org_id not a string", `{"org_id":5}`, bearer},
		{"not an object", `[1]`, bearer},
		{"not a UUID", `{"org_id":"acme"}`, bearer},
		{"empty org_id", `{"org_id":""}`, bearer},
		{"undecodable JSON", `{"org_id":`, bearer},
		{"no Authorization", orgBody(v.org), nil},
		{"not a bearer", orgBody(v.org), map[string]string{"Authorization": "Basic YWRhOnB3"}},
		{"forged bearer", orgBody(v.org), map[string]string{"Authorization": "Bearer " + token + "x"}},
	} {
		compat := f.post(c.body, c.header)
		request := httptest.NewRequest(http.MethodPost, PatternCompatSwitchOrg, strings.NewReader(c.body))
		for k, value := range c.header {
			request.Header.Set(k, value)
		}
		theirs := httptest.NewRecorder()
		goAPI.ServeHTTP(theirs, request)
		expectSamePlanes(t, c.name, compat, theirs)
	}
}

// goAPISwitchHandler is go-api's own POST /api/v1/auth/switch-org over the
// validate fixture's store, with a pool that reaches no database: the shared
// refusals are answered before go-api opens its transaction.
func goAPISwitchHandler(t *testing.T, v *validateFixture) http.Handler {
	t.Helper()
	verifier, err := edgetoken.New(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	auth, err := policy.NewAuthenticator(verifier, goAPIValidateStore{v}, logger)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://switch@127.0.0.1:1/none")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, route := range apisession.Routes(apisession.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Auth: auth,
		Verifier: verifier, Signer: v.hs256, Logger: logger, Limits: httpapi.NewMemoryCounters(nil)}) {
		if route.Method == http.MethodPost && route.Pattern == PatternCompatSwitchOrg {
			mux := http.NewServeMux()
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
			return mux
		}
	}
	t.Fatal("go-api mounts no switch-org route")
	return nil
}

// The route over the production flag table (MarkResponseModels on the
// embedded document's route set) writes its success answer with no writer
// violation, in the hs256_compat class.
func TestSwitchOrgProductionResponseModelRaisesNoWriterViolation(t *testing.T) {
	f := newSwitchFixture(t)
	routes, err := CompatRoutes(f.deps())
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range MarkResponseModels(routes) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	expectNoViolation(t, "switch-org", http.StatusOK, func() int {
		request := httptest.NewRequest(http.MethodPost, PatternCompatSwitchOrg, strings.NewReader(orgBody(f.orgB)))
		for k, v := range f.bearer() {
			request.Header.Set(k, v)
		}
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		return response.Code
	})
}

func TestSwitchOrgResponseModelIsTheSameAsGoAPIs(t *testing.T) {
	if !ResponseModelRoutes()["POST "+PatternCompatSwitchOrg] {
		t.Fatal("POST " + PatternCompatSwitchOrg + " is not a response-model route; go-api's twin is")
	}
}
