package authapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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
	currentErr error
	revoked    []uuid.UUID
	revokedAll []uuid.UUID
	actors     []session.Actor
	revokeErr  error
}

func (f *fakeSessions) Exchange(context.Context, contracts.SecretString) (session.Result, error) {
	return f.result, f.err
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
	users   map[uuid.UUID]UserState
	roles   map[[2]uuid.UUID]*string
	err     error
	roleErr error
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

func newFixture(t *testing.T) *fixture {
	t.Helper()
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	principal, org := uuid.New(), uuid.New()
	now := time.Now().UTC()
	record := session.Record{
		ID: uuid.New(), PrincipalID: principal, OrganizationID: &org, Revision: 1,
		AuthTime: now.Add(-time.Hour), AMR: []string{"pwd"}, CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(7 * 24 * time.Hour),
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
	token, err := f.issuer.Access(grant)
	if err != nil {
		f.t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + token}
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

func TestRefreshMintsAGoAPIAccessTokenWithSidAndAnswersRefreshResponseV1(t *testing.T) {
	f := newFixture(t)
	rotated := credential()
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(rotated), ExpiresAt: time.Now().Add(7*24*time.Hour - time.Second),
	}}
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	parsed, err := contracts.ParseRefreshResponse(repoRoot(t), response.Body.Bytes())
	if err != nil {
		t.Fatalf("not refresh-response.v1: %v", err)
	}
	if parsed.RefreshCredential.Reveal() != rotated || parsed.SessionID != session.WireSessionID(f.record.ID) ||
		parsed.ExpiresIn != 3600 || parsed.RefreshExpiresIn < 1 || parsed.RefreshExpiresIn > 604800 {
		t.Fatalf("response %+v", parsed)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("a token response is cacheable")
	}

	// go-api's verifier (the same edgetoken decision query-api and go-api use)
	// accepts it, and the claims are go-api's from the live user and role.
	verifier, err := edgetoken.New(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(parsed.AccessToken.Reveal())
	if err != nil {
		t.Fatalf("go-api's verifier refused the token: %v", err)
	}
	want := map[string]any{
		"sub": f.principal.String(), "email": "ada@example.com", "org_id": f.org.String(), "role": "admin",
		"is_superuser": true, "type": "access", "iss": testIssuer, "aud": testAudience, "tv": float64(4),
		"username": "ada", "sid": session.WireSessionID(f.record.ID),
	}
	for key, value := range want {
		if claims[key] != value {
			t.Errorf("claim %s = %v, want %v", key, claims[key], value)
		}
	}
	if _, has := claims["full_name"]; has {
		t.Error("an absent full_name was written")
	}
	if exp, iat := claims["exp"].(float64), claims["iat"].(float64); exp-iat != 3600 {
		t.Errorf("lifetime = %v s, want 3600", exp-iat)
	}
	if strings.Contains(f.logs.String(), rotated) || strings.Contains(f.logs.String(), parsed.AccessToken.Reveal()) {
		t.Fatal("a credential reached the log")
	}
}

func TestRefreshTakesGoAPIsRoleRules(t *testing.T) {
	cases := map[string]struct {
		org  bool
		role *string
		has  bool
		want string
	}{
		"no organization": {org: false, want: "member"},
		"no membership":   {org: true, has: false, want: "member"},
		"NULL role":       {org: true, has: true, role: nil, want: "None"},
		"membership role": {org: true, has: true, role: ptr("owner"), want: "owner"},
	}
	for name, c := range cases {
		f := newFixture(t)
		record := f.record
		if !c.org {
			record.OrganizationID = nil
		}
		f.users.roles = map[[2]uuid.UUID]*string{}
		if c.has {
			f.users.roles[[2]uuid.UUID{f.principal, f.org}] = c.role
		}
		f.build()
		f.sessions.result = session.Result{Reason: session.ReasonGraceReplay, Issued: session.Issued{
			Session: record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
		response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
		if response.Code != 200 {
			t.Fatalf("%s: %d %s", name, response.Code, response.Body)
		}
		var body map[string]any
		_ = json.Unmarshal(response.Body.Bytes(), &body)
		claims, _ := f.issuer.verifier.Verify(body["access_token"].(string))
		if claims["role"] != c.want {
			t.Errorf("%s: role = %v, want %q", name, claims["role"], c.want)
		}
		wantOrg := ""
		if c.org {
			wantOrg = f.org.String()
		}
		if claims["org_id"] != wantOrg {
			t.Errorf("%s: org_id = %v, want %q", name, claims["org_id"], wantOrg)
		}
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
	other, err := edgetoken.NewSigner("a-different-signing-key-0123456789abcdef", testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	goAPI, _ := edgetoken.NewSigner(testSecret, testIssuer, testAudience)
	now := time.Now()
	sign := func(s *edgetoken.Signer, claims edgetoken.AccessClaims) string {
		token, err := s.Access(claims, now, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	sid := session.WireSessionID(f.record.ID)
	base := edgetoken.AccessClaims{UserID: f.principal.String(), Email: "ada@example.com", Role: "admin", TokenVersion: 4, SessionID: &sid}
	refresh, _ := goAPI.Refresh(edgetoken.RefreshClaims{UserID: f.principal.String()}, now, uuid.NewString())
	foreignSession := session.WireSessionID(uuid.New())
	withForeign := base
	withForeign.SessionID = &foreignSession
	noSID := base
	noSID.SessionID = nil
	otherUser := base
	otherUser.UserID = uuid.NewString()

	cases := map[string]struct {
		header map[string]string
		reason string
	}{
		"no header":              {nil, "malformed"},
		"basic scheme":           {map[string]string{"Authorization": "Basic eDp5"}, "malformed"},
		"alg none":               {map[string]string{"Authorization": "Bearer eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ4In0."}, "bad_signature"},
		"another key":            {map[string]string{"Authorization": "Bearer " + sign(other, base)}, "bad_signature"},
		"refresh JWT":            {map[string]string{"Authorization": "Bearer " + refresh}, "type_mismatch"},
		"go-api token, no sid":   {map[string]string{"Authorization": "Bearer " + sign(goAPI, noSID)}, "malformed"},
		"unknown session":        {map[string]string{"Authorization": "Bearer " + sign(goAPI, withForeign)}, "session_revoked"},
		"another user's session": {map[string]string{"Authorization": "Bearer " + sign(goAPI, otherUser)}, "malformed"},
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
