package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// goldenAnswer is one answer of the recorded Python corpus
// (internal/testsupport/sessionscenario/testdata/python_golden.json), the
// wire go-api reproduces. FastAPI's 422 bodies echo "input" and "ctx"; go-api
// never writes them (the validation-echo quirk is Unsafe), so they are
// dropped here.
func goldenAnswer(t *testing.T, name string) (int, any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal", "testsupport", "sessionscenario", "testdata", "python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Answers []struct {
			Name   string `json:"name"`
			Status int    `json:"status"`
			Body   string `json:"body"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, answer := range corpus.Answers {
		if answer.Name == name {
			var body any
			if err := json.Unmarshal([]byte(answer.Body), &body); err != nil {
				t.Fatalf("golden %q: %v", name, err)
			}
			return answer.Status, withoutEcho(body)
		}
	}
	t.Fatalf("no golden answer %q", name)
	return 0, nil
}

func withoutEcho(body any) any {
	object, ok := body.(map[string]any)
	if !ok {
		return body
	}
	if list, ok := object["detail"].([]any); ok {
		for _, item := range list {
			if entry, ok := item.(map[string]any); ok {
				delete(entry, "input")
				delete(entry, "ctx")
			}
		}
	}
	return object
}

// expectGolden asserts the answer is the golden one: status and body.
func expectGolden(t *testing.T, response interface {
	Result() *http.Response
}, body []byte, name string) {
	t.Helper()
	status, want := goldenAnswer(t, name)
	if got := response.Result().StatusCode; got != status {
		t.Fatalf("%s: status %d, golden %d; body %s", name, got, status, body)
	}
	var got any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("%s: body is not JSON: %s", name, body)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: body %s, golden %v", name, body, want)
	}
}

// keyOrder is the member order of a JSON object, recursively for the "user"
// member: go-api writes a response_model's fields in declaration order.
func keyOrder(t *testing.T, body string) []string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(body))
	var keys []string
	depth := 0
	expectKey := false
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch v := tok.(type) {
		case json.Delim:
			switch v {
			case '{':
				depth++
				expectKey = true
			case '}':
				depth--
				expectKey = depth > 0
			}
		case string:
			if expectKey {
				keys = append(keys, strings.Repeat(".", depth-1)+v)
				expectKey = false
				continue
			}
			expectKey = true
		default:
			expectKey = true
		}
	}
	return keys
}

func goldenKeyOrder(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal", "testsupport", "sessionscenario", "testdata", "python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Answers []struct {
			Name string `json:"name"`
			Body string `json:"body"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, answer := range corpus.Answers {
		if answer.Name == name {
			// The corpus records a JWT as "<jwt {header} {claims}>" with its
			// quotes unescaped; it is one string value of the body.
			return keyOrder(t, recordedJWT.ReplaceAllString(answer.Body, `"jwt"`))
		}
	}
	t.Fatalf("no golden answer %q", name)
	return nil
}

var recordedJWT = regexp.MustCompile(`"<jwt .*?\}>"`)

// fakeLoginUsers is local.Users.
type fakeLoginUsers struct {
	byEmail   map[string]*local.User
	projected []uuid.UUID
}

func (f *fakeLoginUsers) ByEmail(_ context.Context, email string) (*local.User, error) {
	return f.byEmail[email], nil
}

func (f *fakeLoginUsers) Project(_ context.Context, id uuid.UUID) error {
	f.projected = append(f.projected, id)
	return nil
}

// fakeAttempts is local.Attempts with a programmable lock.
type fakeAttempts struct {
	locked    bool
	remaining time.Duration
	failures  int
}

func (f *fakeAttempts) Check(context.Context, []byte) (bool, time.Duration, error) {
	return f.locked, f.remaining, nil
}

func (f *fakeAttempts) RecordFailure(context.Context, []byte) error {
	f.failures++
	return nil
}

func (f *fakeAttempts) Clear(context.Context, []byte) error { return nil }

// fakeProjector is the identity store's projections: an organization is
// found when it is in orgs, a principal when it is not in gone.
type fakeProjector struct {
	mu        sync.Mutex
	orgs      map[uuid.UUID]bool
	gone      map[uuid.UUID]bool
	orgCalls  []uuid.UUID
	userCalls []uuid.UUID
}

func (f *fakeProjector) Project(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.userCalls = append(f.userCalls, id)
	if f.gone[id] {
		return identity.ErrUserNotFound
	}
	return nil
}

func (f *fakeProjector) ProjectOrganization(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orgCalls = append(f.orgCalls, id)
	if !f.orgs[id] {
		return identity.ErrOrganizationNotFound
	}
	return nil
}

var testLockoutKey = base64.RawURLEncoding.EncodeToString([]byte("authapi-compat-lockout-key-00032"))

// compatFixture is the fixture with login mounted.
type compatFixture struct {
	*fixture
	loginUsers *fakeLoginUsers
	attempts   *fakeAttempts
	projector  *fakeProjector
	appended   []audit.AuditEvent
	appendErr  error
	social     SocialExchange
	orgB       uuid.UUID
	orgD       uuid.UUID
}

const (
	compatEmail    = "alice@example.com"
	compatPassword = "correct horse"
)

func newCompatFixture(t *testing.T) *compatFixture {
	t.Helper()
	f := &compatFixture{fixture: newFixture(t), attempts: &fakeAttempts{}, orgB: uuid.New(), orgD: uuid.New()}
	hash := "hash:" + compatPassword
	f.loginUsers = &fakeLoginUsers{byEmail: map[string]*local.User{
		compatEmail: {ID: f.principal, IsActive: true, IsVerified: true, PasswordHash: &hash},
	}}
	f.projector = &fakeProjector{orgs: map[uuid.UUID]bool{f.org: true, f.orgB: true, f.orgD: true}, gone: map[uuid.UUID]bool{}}
	owner, member := "owner", "member"
	early, late := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Ordered as go-api reads them (joined_at, created_at, NULLs last): orgB
	// has no joined_at, so it sorts last but joined earliest by created_at.
	f.users.memberships = map[uuid.UUID][]Membership{f.principal: {
		{OrgID: f.org, Role: &owner, JoinedAt: &late, CreatedAt: late},
		{OrgID: f.orgB, Role: &member, CreatedAt: early},
	}}
	f.buildCompat()
	return f
}

func (f *compatFixture) buildCompat() {
	routes, err := CompatRoutes(CompatDeps{
		Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Logger:    slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Projector: f.projector,
		// The real writer (audit.Append) refuses an attribute outside the
		// allowlist; so does this one.
		AppendAudit: func(_ context.Context, event audit.AuditEvent) error {
			if err := audit.ValidateAttributes(event.Attributes); err != nil {
				return err
			}
			f.appended = append(f.appended, event)
			return f.appendErr
		},
		Login: &LoginConfig{
			Users: f.loginUsers, Attempts: f.attempts, LockoutKey: contracts.NewSecretString(testLockoutKey),
			Compare: func(hash, password string) bool { return hash == "hash:"+password },
		},
		Social: f.social,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
}

func loginBody(email, password, org string) string {
	body := map[string]any{"email": email, "password": password}
	if org != "" {
		body["org_id"] = org
	}
	raw, _ := json.Marshal(body)
	return string(raw)
}

func decodeObject(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("not a JSON object: %s", body)
	}
	return out
}

func TestCompatRefreshAnswersGoAPIsRefusals(t *testing.T) {
	cases := map[session.Reason]string{
		session.ReasonMalformed:            "refresh: garbage",
		session.ReasonRefreshUnknown:       "refresh: garbage",
		session.ReasonRefreshExpired:       "refresh: garbage",
		session.ReasonSessionRevoked:       "refresh: garbage",
		session.ReasonSessionExpired:       "refresh: garbage",
		session.ReasonRefreshReused:        "refresh: original outside the grace window (reuse)",
		session.ReasonPrincipalDeactivated: "refresh: judy deactivated",
	}
	for reason, golden := range cases {
		f := newCompatFixture(t)
		f.sessions.result = session.Result{Reason: reason}
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), golden)
	}
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatRefresh, `{}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "refresh: missing")
}

func TestCompatRefreshOfAGoneOrDisabledUserRevokesTheSession(t *testing.T) {
	for golden, mutate := range map[string]func(*fixture){
		"refresh: live jti, subject no user": func(f *fixture) { delete(f.users.users, f.principal) },
		"refresh: judy deactivated": func(f *fixture) {
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
		},
	} {
		f := newCompatFixture(t)
		mutate(f.fixture)
		f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
			Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour),
		}}
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), golden)
		if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
			t.Fatalf("%s: revoked %v, want the session", golden, f.sessions.revoked)
		}
	}
}

func TestCompatRefreshAnswersGoAPIsBodyWithTheRotatedCredential(t *testing.T) {
	f := newCompatFixture(t)
	rotated := "dhr1_" + strings.Repeat("r", 43)
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(rotated), ExpiresAt: time.Now().Add(time.Hour),
	}}
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if got, want := keyOrder(t, response.Body.String()), goldenKeyOrder(t, "refresh: alice"); !reflect.DeepEqual(got, want) {
		t.Fatalf("members %v, golden %v", got, want)
	}
	body := decodeObject(t, response.Body.Bytes())
	if body["refresh_token"] != rotated || body["token_type"] != "bearer" || body["expires_in"] != float64(3600) {
		t.Fatalf("body %s", response.Body)
	}
	user := body["user"].(map[string]any)
	want := map[string]any{"id": f.principal.String(), "email": "ada@example.com", "username": nil, "full_name": nil,
		"org_id": f.org.String(), "role": "admin", "is_superuser": true}
	if !reflect.DeepEqual(user, want) {
		t.Fatalf("user %v, want %v", user, want)
	}
	bearer, _, err := f.issuer.Verify(body["access_token"].(string))
	if err != nil || bearer.SessionID != f.record.ID || bearer.PrincipalID != f.principal {
		t.Fatalf("access token: %+v %v", bearer, err)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("a token answer is cacheable")
	}
}

func TestCompatRefreshWithoutAnOrganizationAnswersAnEmptyOrgID(t *testing.T) {
	f := newCompatFixture(t)
	record := f.record
	record.OrganizationID = nil
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour),
	}}
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	user := decodeObject(t, response.Body.Bytes())["user"].(map[string]any)
	if user["org_id"] != "" || user["role"] != "member" {
		t.Fatalf("user %v, want go-api's empty org and member role", user)
	}
}

func TestCompatFailuresAnswerGoAPIs500WithoutTheirText(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.result = session.Result{Reason: session.ReasonStoreUnavailable}
	f.sessions.err = errors.New("dial tcp 10.0.0.9:5432: secret-db-host refused")
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	if response.Code != http.StatusInternalServerError || strings.TrimSpace(response.Body.String()) != `{"detail":"Internal Server Error"}` {
		t.Fatalf("answer %d %s", response.Code, response.Body)
	}
	if response.Header().Get("X-Dho-Unhandled-Error") != "1" {
		t.Fatal("go-api's unhandled-error marker is missing")
	}
	if strings.Contains(f.logs.String(), "secret-db-host") {
		t.Fatal("the store error text reached the log")
	}
}

func TestCompatLogoutAlwaysAnswersGoAPIs200(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"garbage"}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "logout: anonymous garbage")
	if len(f.sessions.byCredential) != 1 || f.sessions.byCredential[0] != "garbage" {
		t.Fatalf("revoke by credential %v", f.sessions.byCredential)
	}
	if f.sessions.actors[0].RequestID == nil {
		t.Fatal("the revocation carries no request id")
	}
	response = f.do(http.MethodPost, PatternCompatLogout, `{}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "logout: missing body")
	f.sessions.byCredentialErr = errors.New("store down")
	response = f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"x"}`, nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("a store failure answered %d", response.Code)
	}
}

func TestCompatLoginRefusalsAnswerGoAPIsWireAndAreAudited(t *testing.T) {
	disabled, noPassword := uuid.New(), uuid.New()
	cases := []struct {
		golden   string
		email    string
		password string
		reason   string
		subject  *uuid.UUID
	}{
		{"login: alice wrong password", compatEmail, "wrong", "invalid_credentials", nil},
		{"login: unknown user", "nobody@example.com", "x", "invalid_credentials", nil},
		{"login: dave inactive", "dave@example.com", compatPassword, "principal_deactivated", &disabled},
		{"login: erin no password", "erin@example.com", compatPassword, "invalid_credentials", &noPassword},
	}
	for _, c := range cases {
		f := newCompatFixture(t)
		hash := "hash:" + compatPassword
		f.loginUsers.byEmail["dave@example.com"] = &local.User{ID: disabled, IsActive: false, IsVerified: true, PasswordHash: &hash}
		f.loginUsers.byEmail["erin@example.com"] = &local.User{ID: noPassword, IsActive: true, IsVerified: true}
		response := f.do(http.MethodPost, PatternCompatLogin, loginBody(c.email, c.password, ""), nil)
		expectGolden(t, response, response.Body.Bytes(), c.golden)
		if len(f.appended) != 1 {
			t.Fatalf("%s: %d audit rows, want 1", c.golden, len(f.appended))
		}
		event := f.appended[0]
		if event.EventType != audit.EventLoginFailed || event.Outcome != audit.OutcomeDenied ||
			event.Attributes["reason_code"] != c.reason || event.RequestID == nil {
			t.Fatalf("%s: audit row %+v", c.golden, event)
		}
		if len(f.sessions.created) != 0 {
			t.Fatalf("%s: a refused login created a session", c.golden)
		}
		if f.attempts.failures != 1 {
			t.Fatalf("%s: %d failures recorded, want 1", c.golden, f.attempts.failures)
		}
		if c.golden == "login: unknown user" && (event.SubjectPrincipalID != nil || event.OrganizationID != nil) {
			t.Fatalf("unknown user: audit row names a subject or org: %+v", event)
		}
	}
}

func TestCompatLoginAuditsAKnownUsersFailureAtTheirFirstMembership(t *testing.T) {
	f := newCompatFixture(t)
	f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", ""), nil)
	event := f.appended[0]
	if event.SubjectPrincipalID == nil || *event.SubjectPrincipalID != f.principal.String() {
		t.Fatalf("subject %v, want the user", event.SubjectPrincipalID)
	}
	if event.OrganizationID == nil || *event.OrganizationID != f.orgB.String() {
		t.Fatalf("organization %v, want the earliest-joined membership %s", event.OrganizationID, f.orgB)
	}
}

func TestCompatLoginAuditsTheRequestedOrganizationWhenItExists(t *testing.T) {
	f := newCompatFixture(t)
	f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", f.orgD.String()), nil)
	if org := f.appended[0].OrganizationID; org == nil || *org != f.orgD.String() {
		t.Fatalf("organization %v, want the requested %s", org, f.orgD)
	}
	missing := uuid.New()
	f = newCompatFixture(t)
	f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", missing.String()), nil)
	if org := f.appended[0].OrganizationID; org == nil || *org != f.orgB.String() {
		t.Fatalf("organization %v, want the first membership for a requested org that does not exist", org)
	}
}

func TestCompatLoginOfAUserGoAPINoLongerHoldsIsAuditedWithoutASubject(t *testing.T) {
	f := newCompatFixture(t)
	f.projector.gone[f.principal] = true
	f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", ""), nil)
	if f.appended[0].SubjectPrincipalID != nil {
		t.Fatal("a principal that could not be projected is named as the subject")
	}
}

func TestCompatLoginLockAnswers429WithRetryAfterSeconds(t *testing.T) {
	f := newCompatFixture(t)
	f.attempts.locked, f.attempts.remaining = true, 899*time.Second+time.Millisecond
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	want := map[string]any{"detail": map[string]any{"message": "Too many failed login attempts. Please try again later.", "retry_after_seconds": float64(900)}}
	if got := decodeObject(t, response.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("body %s", response.Body)
	}
	_, golden := goldenAnswer(t, "lockout: wrong password 6")
	if golden.(map[string]any)["detail"].(map[string]any)["message"] != want["detail"].(map[string]any)["message"] {
		t.Fatal("the lock text is not go-api's")
	}
	if f.appended[0].Attributes["reason_code"] != "account_locked" {
		t.Fatalf("audit %+v", f.appended[0])
	}
	f = newCompatFixture(t)
	f.attempts.locked, f.attempts.remaining = true, 0
	response = f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if decodeObject(t, response.Body.Bytes())["detail"].(map[string]any)["retry_after_seconds"] != float64(1) {
		t.Fatalf("an elapsed lock answered %s, want retry_after_seconds 1", response.Body)
	}
}

func TestCompatLoginOfAnUnverifiedLocalUserAsksForVerification(t *testing.T) {
	f := newCompatFixture(t)
	provider := "local"
	user := f.loginUsers.byEmail[compatEmail]
	user.IsVerified, user.AuthProvider = false, &provider
	f.users.users[f.principal] = UserState{Email: "carol@example.com", IsActive: true}
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	expectGolden(t, response, response.Body.Bytes(), "login: carol unverified")
	if len(f.sessions.created) != 0 || f.appended[0].Attributes["reason_code"] != "email_unverified" {
		t.Fatalf("sessions %d, audit %+v", len(f.sessions.created), f.appended)
	}
}

func TestCompatLoginToAnOrganizationTheUserIsNotIn(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.orgD.String()), nil)
	expectGolden(t, response, response.Body.Bytes(), "login: alice not a member of D")
	if len(f.sessions.created) != 0 {
		t.Fatal("a session was created for an organization the user is not in")
	}
	event := f.appended[0]
	if event.Attributes["reason_code"] != reasonNotMember || event.OrganizationID == nil || *event.OrganizationID != f.orgD.String() {
		t.Fatalf("audit %+v", event)
	}
}

func TestCompatLoginStartsAnAuditedSessionAndAnswersGoAPIsLoginResponse(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if got, want := keyOrder(t, response.Body.String()), goldenKeyOrder(t, "login: alice"); !reflect.DeepEqual(got, want) {
		t.Fatalf("members %v, golden %v", got, want)
	}
	if len(f.sessions.created) != 1 {
		t.Fatalf("%d sessions created", len(f.sessions.created))
	}
	created := f.sessions.created[0]
	if created.org == nil || *created.org != f.orgB || !reflect.DeepEqual(created.amr, []string{"pwd"}) {
		t.Fatalf("session org %v amr %v, want the earliest-joined membership and pwd", created.org, created.amr)
	}
	event := created.event
	if event.EventType != EventLoginSucceeded || event.Outcome != audit.OutcomeAllowed || event.OrganizationID == nil ||
		*event.OrganizationID != f.orgB.String() || event.RequestID == nil || *event.SubjectPrincipalID != f.principal.String() {
		t.Fatalf("audit %+v", event)
	}
	if len(f.appended) != 0 {
		t.Fatal("a successful login appended a failure row")
	}
	body := decodeObject(t, response.Body.Bytes())
	if body["refresh_token"] != created.issued.Credential.Reveal() || body["needs_onboarding"] != false {
		t.Fatalf("body %s", response.Body)
	}
	user := body["user"].(map[string]any)
	if user["org_id"] != f.orgB.String() || user["role"] != "member" || user["username"] != "ada" {
		t.Fatalf("user %v", user)
	}
	bearer, _, err := f.issuer.Verify(body["access_token"].(string))
	if err != nil || bearer.SessionID != created.issued.Session.ID {
		t.Fatalf("access token %+v %v", bearer, err)
	}
	if len(f.loginUsers.projected) != 1 {
		t.Fatal("the principal was not projected before the session")
	}
}

func TestCompatLoginToARequestedOrganization(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.org.String()), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if created := f.sessions.created[0]; *created.org != f.org || *created.event.OrganizationID != f.org.String() {
		t.Fatalf("session org %v", created.org)
	}
	if user := decodeObject(t, response.Body.Bytes())["user"].(map[string]any); user["role"] != "owner" {
		t.Fatalf("user %v", user)
	}
}

func TestCompatLoginWithoutAMembership(t *testing.T) {
	f := newCompatFixture(t)
	delete(f.users.memberships, f.principal)
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, uuid.NewString()), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	body := decodeObject(t, response.Body.Bytes())
	user := body["user"].(map[string]any)
	// The fixture's user is a superuser: no onboarding.
	if user["org_id"] != nil || user["role"] != "member" || body["needs_onboarding"] != false {
		t.Fatalf("body %s", response.Body)
	}
	created := f.sessions.created[0]
	if created.org != nil || created.event.OrganizationID != nil {
		t.Fatalf("session org %v, audit org %v: a requested org that does not exist names neither", created.org, created.event.OrganizationID)
	}
	state := f.users.users[f.principal]
	state.IsSuperuser = false
	f.users.users[f.principal] = state
	f.buildCompat()
	response = f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if decodeObject(t, response.Body.Bytes())["needs_onboarding"] != true {
		t.Fatalf("a member-less user does not need onboarding: %s", response.Body)
	}
}

func TestCompatLoginValidatesTheBodyAsGoAPIDoes(t *testing.T) {
	for golden, body := range map[string]string{
		"login: missing fields":     `{}`,
		"login: password 129 chars": loginBody(compatEmail, strings.Repeat("p", 129), ""),
		"login: invalid email":      loginBody("not-an-email", "x", ""),
		"login: org_id int":         `{"email":"alice@example.com","password":"x","org_id":5}`,
		"login: body is a list":     `[]`,
		"login: malformed json":     `{"email":}`,
		"login: no body":            ``,
	} {
		f := newCompatFixture(t)
		response := f.do(http.MethodPost, PatternCompatLogin, body, nil)
		expectGolden(t, response, response.Body.Bytes(), golden)
		if f.attempts.failures != 0 {
			t.Fatalf("%s: a refused body reached the credential check", golden)
		}
		if len(f.appended) != 1 || f.appended[0].Attributes["reason_code"] != "malformed" {
			t.Fatalf("%s: audit rows %+v, want one malformed refusal", golden, f.appended)
		}
	}
}

func TestCompatLoginFailsClosedWhenItsAuditRowCannotBeWritten(t *testing.T) {
	f := newCompatFixture(t)
	f.appendErr = errors.New("audit down")
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", ""), nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 when the refusal cannot be audited", response.Code)
	}
}

// fakeSocial is the social sign-in domain with a programmable answer.
type fakeSocial struct {
	result   social.ExchangeResult
	err      error
	calls    int
	provider social.Provider
	token    string
}

func (f *fakeSocial) Exchange(_ context.Context, provider social.Provider, token contracts.SecretString, _ *string) (social.ExchangeResult, error) {
	f.calls++
	f.provider, f.token = provider, token.Reveal()
	return f.result, f.err
}

func newSocialFixture(t *testing.T) (*compatFixture, *fakeSocial) {
	t.Helper()
	f := newCompatFixture(t)
	fake := &fakeSocial{}
	f.social = fake
	f.buildCompat()
	return f, fake
}

func TestCompatSocialLoginStartsAFederatedSessionAndAnswersGoAPIsSocialLoginResponse(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.result = social.ExchangeResult{PrincipalID: f.principal, Created: true}
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"gho_test"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if got, want := keyOrder(t, response.Body.String()), goldenKeyOrder(t, "social: github new user"); !reflect.DeepEqual(got, want) {
		t.Fatalf("members %v, golden %v", got, want)
	}
	if decodeObject(t, response.Body.Bytes())["is_new_user"] != true || fake.provider != social.GitHub || fake.token != "gho_test" {
		t.Fatalf("body %s, exchange %s/%s", response.Body, fake.provider, fake.token)
	}
	created := f.sessions.created[0]
	if !reflect.DeepEqual(created.amr, []string{"fed"}) || created.event.EventType != EventLoginSucceeded || *created.org != f.orgB ||
		created.event.Attributes["provider"] != "github" {
		t.Fatalf("session %+v", created)
	}
	if len(f.appended) != 0 {
		t.Fatal("a social sign-in appended a failure row")
	}
}

func TestCompatSocialLoginValidatesTheBodyAsGoAPIDoes(t *testing.T) {
	for golden, body := range map[string]string{
		"social: missing token":    `{"provider":"github"}`,
		"social: unknown provider": `{"provider":"bitbucket","provider_access_token":"x"}`,
	} {
		f, fake := newSocialFixture(t)
		response := f.do(http.MethodPost, PatternCompatSocialLogin, body, nil)
		expectGolden(t, response, response.Body.Bytes(), golden)
		if fake.calls != 0 || len(f.appended) != 1 || f.appended[0].Attributes["reason_code"] != "malformed" {
			t.Fatalf("%s: a refused body reached the exchange (%d) or wrote audit rows %+v, want one malformed refusal", golden, fake.calls, f.appended)
		}
	}
	f, _ := newSocialFixture(t)
	for _, body := range []string{`[]`, ``, `{"provider":"github"`} {
		response := f.do(http.MethodPost, PatternCompatSocialLogin, body, nil)
		if response.Code != http.StatusUnprocessableEntity {
			t.Fatalf("body %q answered %d, want go-api's 422", body, response.Code)
		}
		if _, isList := decodeObject(t, response.Body.Bytes())["detail"].([]any); !isList {
			t.Fatalf("body %q answered %s, want a FastAPI validation list", body, response.Body)
		}
	}
}

// Every refused social sign-in writes its auth.login.failed row through the
// same path as a refused password sign-in; an operational failure (5xx) is no
// sign-in decision and writes none.
func TestCompatSocialRefusalsAreAuditedLikeEveryOtherSignInRefusal(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		provider string
		status   int
		golden   string
		reason   string
	}{
		{"provider refused", fmt.Errorf("fetch: %w", social.ErrProviderRefused), "github", 401, "social: github refused token", "invalid_credentials"},
		{"email of another provider", &social.ConflictError{ExistingMethod: "github"}, "gitlab", 409, "social: gitlab email of a github user", "email_collision"},
		{"email unverified", social.ErrEmailUnverified, "github", 403, "", "email_unverified"},
		{"principal disabled", social.ErrPrincipalDisabled, "github", 403, "", "principal_deactivated"},
		{"sign-up not available", identity.ErrSignupNotAvailable, "google", 403, "", "signup_unavailable"},
		{"identity conflict", identity.ErrIdentityConflict, "github", 409, "", "identity_conflict"},
		{"provider not configured", social.ErrProviderNotConfigured, "google", 503, "social: google not configured", ""},
		{"provider unavailable", social.ErrProviderUnavailable, "github", 500, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, fake := newSocialFixture(t)
			fake.err = c.err
			response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"`+c.provider+`","provider_access_token":"tok"}`, nil)
			if c.golden != "" {
				expectGolden(t, response, response.Body.Bytes(), c.golden)
			} else if response.Code != c.status {
				t.Fatalf("status %d, want %d: %s", response.Code, c.status, response.Body)
			}
			if len(f.sessions.created) != 0 {
				t.Fatal("a refused social sign-in created a session")
			}
			if c.reason == "" {
				if len(f.appended) != 0 {
					t.Fatalf("an operational failure appended %+v", f.appended)
				}
				return
			}
			if len(f.appended) != 1 {
				t.Fatalf("%d audit rows, want 1", len(f.appended))
			}
			event := f.appended[0]
			if event.EventType != audit.EventLoginFailed || event.Outcome != audit.OutcomeDenied || event.RequestID == nil ||
				event.Attributes["reason_code"] != c.reason || event.Attributes["provider"] != c.provider {
				t.Fatalf("audit row %+v", event)
			}
			if err := audit.ValidateAttributes(event.Attributes); err != nil {
				t.Fatalf("the audit row's attributes are outside the allowlist: %v", err)
			}
		})
	}
}

func TestCompatSocialRefusalFailsClosedWhenItsAuditRowCannotBeWritten(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.err = social.ErrProviderRefused
	f.appendErr = errors.New("audit down")
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 when the refusal cannot be audited", response.Code)
	}
}

// Every refusal a password sign-in answers is audited too, and every audit
// row of a sign-in refusal carries attributes the audit allowlist admits.
func TestEveryLoginRefusalIsAuditedWithAllowlistedAttributes(t *testing.T) {
	f := newCompatFixture(t)
	for _, body := range []string{
		loginBody(compatEmail, "wrong", ""),
		loginBody("nobody@example.com", "x", ""),
		loginBody(compatEmail, compatPassword, f.orgD.String()),
	} {
		before := len(f.appended)
		response := f.do(http.MethodPost, PatternCompatLogin, body, nil)
		if response.Code < 400 || response.Code >= 500 || len(f.appended) != before+1 {
			t.Fatalf("answer %d with %d new audit rows, want a 4xx with one", response.Code, len(f.appended)-before)
		}
		if err := audit.ValidateAttributes(f.appended[len(f.appended)-1].Attributes); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChooseMembershipIsGoAPIsChoiceWithoutActivity(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	t1, t2 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	list := []Membership{{OrgID: a, JoinedAt: &t2, CreatedAt: t1}, {OrgID: b, CreatedAt: t1}, {OrgID: c, JoinedAt: &t1, CreatedAt: t2}}
	if got := chooseMembership(list, nil); got.OrgID != b {
		t.Fatalf("chose %v, want the first earliest join (b: created_at stands in for a NULL joined_at)", got.OrgID)
	}
	if got := chooseMembership(list, &c); got.OrgID != c {
		t.Fatalf("requested c, chose %v", got.OrgID)
	}
	other := uuid.New()
	if chooseMembership(list, &other) != nil || chooseMembership(nil, nil) != nil {
		t.Fatal("a requested org the user is not in, or no membership, chose one")
	}
}

func TestCompatRoutesNeedAProjectorAndAnAuditAppender(t *testing.T) {
	f := newFixture(t)
	if _, err := CompatRoutes(CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer}); err == nil {
		t.Fatal("compatibility routes mounted without a projector and an audit appender")
	}
	routes, err := CompatRoutes(CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Projector: &fakeProjector{}, AppendAudit: func(context.Context, audit.AuditEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if route.Pattern == PatternCompatLogin || route.Pattern == PatternCompatSocialLogin {
			t.Fatalf("%s mounted without its configuration", route.Pattern)
		}
	}
	base, err := Routes(Deps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range base {
		if strings.HasPrefix(route.Pattern, "/api/") {
			t.Fatalf("the session routes mount %s", route.Pattern)
		}
	}
}

// Every reason social.Classify names for a refusal (4xx) is an audit reason,
// so no social refusal turns into a 500 at its audit row.
func TestEverySocialRefusalReasonIsAnAuditReason(t *testing.T) {
	for _, err := range []error{
		&social.ConflictError{ExistingMethod: "github"}, social.ErrProviderUnavailable, social.ErrProviderNotConfigured,
		social.ErrProviderRefused, social.ErrEmailUnverified, social.ErrPrincipalDisabled, identity.ErrSignupNotAvailable,
		social.ErrReauthenticationRequired, identity.ErrIdentityConflict, social.ErrProviderLinked, identity.ErrNotFound,
		identity.ErrLastSignInMethod, errors.New("anything else"),
	} {
		status, reason := social.Classify(err)
		if status >= http.StatusInternalServerError {
			continue
		}
		if err := audit.ValidateAttributes(map[string]any{"reason_code": string(reason), "provider": "github"}); err != nil {
			t.Errorf("%v: reason %q is not an audit reason: %v", status, reason, err)
		}
	}
}

func TestAProviderAlreadyLinkedRefusalAnswers409AndIsAudited(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.err = social.ErrProviderLinked
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
	if response.Code != http.StatusConflict || len(f.appended) != 1 || f.appended[0].Attributes["reason_code"] != "provider_already_linked" {
		t.Fatalf("status %d, audit %+v", response.Code, f.appended)
	}
}

// A refusal of a principal the social domain knows names it, and its
// organization, as a refused password sign-in does.
func TestAKnownSocialPrincipalsRefusalNamesIt(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.result, fake.err = social.ExchangeResult{PrincipalID: f.principal}, social.ErrPrincipalDisabled
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
	if response.Code != http.StatusForbidden || len(f.appended) != 1 {
		t.Fatalf("status %d, %d audit rows", response.Code, len(f.appended))
	}
	event := f.appended[0]
	if event.SubjectPrincipalID == nil || *event.SubjectPrincipalID != f.principal.String() ||
		event.OrganizationID == nil || *event.OrganizationID != f.orgB.String() {
		t.Fatalf("audit row %+v, want the principal and its first membership", event)
	}
}

// A refusal that names an existing account (the email's holder, a linked
// provider's user) signs nobody in: no session, no cookie, no token.
func TestASocialRefusalNamingAnExistingAccountStartsNoSession(t *testing.T) {
	for name, refusal := range map[string]error{
		"email of another account": &social.ConflictError{ExistingMethod: "password"},
		"provider already linked":  social.ErrProviderLinked,
		"identity conflict":        identity.ErrIdentityConflict,
	} {
		t.Run(name, func(t *testing.T) {
			f, fake := newSocialFixture(t)
			fake.result, fake.err = social.ExchangeResult{PrincipalID: f.principal}, refusal
			response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
			if response.Code != http.StatusConflict {
				t.Fatalf("status %d, want 409: %s", response.Code, response.Body)
			}
			if len(f.sessions.created) != 0 {
				t.Fatalf("the refusal started a session for %s", f.sessions.created[0].principal)
			}
			if cookies := response.Header().Values("Set-Cookie"); len(cookies) != 0 {
				t.Fatalf("the refusal set cookies %q", cookies)
			}
			body := decodeObject(t, response.Body.Bytes())
			if _, issued := body["access_token"]; issued {
				t.Fatalf("the refusal answered a token: %s", response.Body)
			}
		})
	}
}

// Every 4xx answer of a sign-in route, a refused body included, writes its
// auth.login.failed row; the provider is named only when it parsed.
func TestARefusedSignInBodyIsAudited(t *testing.T) {
	cases := []struct {
		path, body, provider string
	}{
		{PatternCompatLogin, `{}`, "local"},
		{PatternCompatLogin, `[]`, "local"},
		{PatternCompatSocialLogin, `{"provider":"github"}`, "github"},
		{PatternCompatSocialLogin, `{"provider":"bitbucket","provider_access_token":"x"}`, ""},
		{PatternCompatSocialLogin, `{"provider":"github"`, ""},
	}
	for _, c := range cases {
		f, _ := newSocialFixture(t)
		response := f.do(http.MethodPost, c.path, c.body, nil)
		if response.Code != http.StatusUnprocessableEntity || len(f.appended) != 1 {
			t.Fatalf("%s %s: status %d, %d audit rows", c.path, c.body, response.Code, len(f.appended))
		}
		event := f.appended[0]
		provider, named := event.Attributes["provider"]
		if event.Attributes["reason_code"] != "malformed" || (c.provider == "" && named) || (c.provider != "" && provider != c.provider) {
			t.Fatalf("%s %s: audit row %+v", c.path, c.body, event)
		}
		if err := audit.ValidateAttributes(event.Attributes); err != nil {
			t.Fatal(err)
		}
	}
	f, _ := newSocialFixture(t)
	f.appendErr = errors.New("audit down")
	if response := f.do(http.MethodPost, PatternCompatLogin, `{}`, nil); response.Code != http.StatusInternalServerError {
		t.Fatalf("a refused body that cannot be audited answered %d, want 500", response.Code)
	}
}

// The sign-in routes carry a transport ErrorWriter: a body over the limit or
// one that could not be read is a refused sign-in and is audited; the rate
// limit's 429 is not (one audit insert per refused request would defeat the
// bucket that guards the store).
func TestTheSignInRoutesAuditTheirTransportRefusals(t *testing.T) {
	f, _ := newSocialFixture(t)
	routes, err := CompatRoutes(CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Projector: f.projector,
		AppendAudit: func(_ context.Context, event audit.AuditEvent) error {
			f.appended = append(f.appended, event)
			return nil
		},
		Login:  &LoginConfig{Users: f.loginUsers, Attempts: f.attempts, LockoutKey: contracts.NewSecretString(testLockoutKey)},
		Social: f.social})
	if err != nil {
		t.Fatal(err)
	}
	signIn := map[string]bool{PatternCompatLogin: true, PatternCompatSocialLogin: true}
	for _, route := range routes {
		if !signIn[route.Pattern] {
			if route.ErrorWriter != nil {
				t.Fatalf("%s carries a sign-in transport writer", route.Pattern)
			}
			continue
		}
		if route.ErrorWriter == nil {
			t.Fatalf("%s has no transport writer", route.Pattern)
		}
		for code, audited := range map[httpapi.Code]bool{
			httpapi.CodePayloadTooLarge: true, httpapi.CodeInvalidRequest: true, httpapi.CodeRateLimited: false, httpapi.CodeInternal: false,
		} {
			before := len(f.appended)
			response := httptest.NewRecorder()
			route.ErrorWriter(response, httptest.NewRequest(http.MethodPost, route.Pattern, nil), code)
			if got := len(f.appended) - before; (got == 1) != audited || got > 1 {
				t.Fatalf("%s %s: %d audit rows, audited=%v", route.Pattern, code, got, audited)
			}
			if !strings.Contains(response.Body.String(), string(code)) {
				t.Fatalf("%s %s: answer %s is not the transport envelope", route.Pattern, code, response.Body)
			}
		}
	}
}

// The two 429s of a sign-in: the route bucket's is a transport answer and
// writes no audit row; the account lockout's is a sign-in decision and
// writes one.
func TestTheBucket429WritesNoRowAndTheLockout429WritesOne(t *testing.T) {
	f := newCompatFixture(t)
	routes, err := CompatRoutes(CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Projector: f.projector,
		AppendAudit: func(_ context.Context, event audit.AuditEvent) error {
			f.appended = append(f.appended, event)
			return nil
		},
		Login: &LoginConfig{Users: f.loginUsers, Attempts: f.attempts, LockoutKey: contracts.NewSecretString(testLockoutKey)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if route.Pattern != PatternCompatLogin {
			continue
		}
		response := httptest.NewRecorder()
		route.ErrorWriter(response, httptest.NewRequest(http.MethodPost, PatternCompatLogin, nil), httpapi.CodeRateLimited)
		if response.Code != http.StatusTooManyRequests || len(f.appended) != 0 {
			t.Fatalf("bucket 429: status %d, %d audit rows, want 429 and none", response.Code, len(f.appended))
		}
	}
	f.attempts.locked, f.attempts.remaining = true, time.Minute
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if response.Code != http.StatusTooManyRequests || len(f.appended) != 1 || f.appended[0].Attributes["reason_code"] != "account_locked" {
		t.Fatalf("lockout 429: status %d, audit rows %+v, want 429 and one account_locked row", response.Code, f.appended)
	}
}

// An unverified local sign-in's refusal row is under the user's own first
// membership, as go-api's unverified branch writes it, whatever organization
// the request names.
func TestAnUnverifiedSignInIsAuditedUnderTheUsersOwnMembership(t *testing.T) {
	f := newCompatFixture(t)
	provider := "local"
	user := f.loginUsers.byEmail[compatEmail]
	user.IsVerified, user.AuthProvider = false, &provider
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.orgD.String()), nil)
	if response.Code != http.StatusOK || len(f.appended) != 1 {
		t.Fatalf("status %d, %d audit rows", response.Code, len(f.appended))
	}
	if org := f.appended[0].OrganizationID; org == nil || *org != f.orgB.String() {
		t.Fatalf("audit organization %v, want the user's first membership %s, not the requested %s", org, f.orgB, f.orgD)
	}
	f = newCompatFixture(t)
	user = f.loginUsers.byEmail[compatEmail]
	user.IsVerified, user.AuthProvider = false, &provider
	delete(f.users.memberships, f.principal)
	f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.orgD.String()), nil)
	if org := f.appended[0].OrganizationID; org == nil || *org != f.orgD.String() {
		t.Fatalf("without a membership the audit organization is %v, want the requested %s", org, f.orgD)
	}
}

// A collision's refusal row names the account that holds the email.
func TestAnEmailCollisionsRefusalRowNamesTheAccountThatHoldsTheEmail(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.result, fake.err = social.ExchangeResult{PrincipalID: f.principal}, &social.ConflictError{ExistingMethod: "local"}
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
	if response.Code != http.StatusConflict || len(f.appended) != 1 {
		t.Fatalf("status %d, %d audit rows", response.Code, len(f.appended))
	}
	event := f.appended[0]
	if event.Attributes["reason_code"] != "email_collision" || event.SubjectPrincipalID == nil || *event.SubjectPrincipalID != f.principal.String() ||
		event.OrganizationID == nil || *event.OrganizationID != f.orgB.String() {
		t.Fatalf("audit row %+v, want the account that holds the email and its organization", event)
	}
}

func TestCompatSocialLoginWithoutAMembershipReturnsNoUsableRefreshCredential(t *testing.T) {
	f, fake := newSocialFixture(t)
	delete(f.users.memberships, f.principal)
	state := f.users.users[f.principal]
	state.IsSuperuser = false
	f.users.users[f.principal] = state
	f.buildCompat()
	fake.result = social.ExchangeResult{PrincipalID: f.principal, Created: true}
	response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"gho_test"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	body := decodeObject(t, response.Body.Bytes())
	created := f.sessions.created[0]
	refresh, _ := body["refresh_token"].(string)
	if body["needs_onboarding"] != true || created.org != nil {
		t.Fatalf("needs_onboarding %v, session org %v", body["needs_onboarding"], created.org)
	}
	if refresh == created.issued.Credential.Reveal() || !refreshCredentialPattern.MatchString(refresh) {
		t.Fatalf("refresh_token %q: want an unstored credential-shaped value, not the session's", refresh)
	}
}

func TestCompatSocialLoginOfAMemberOrASuperuserKeepsTheSessionCredential(t *testing.T) {
	for name, superuser := range map[string]bool{"member": false, "org-less superuser": true} {
		t.Run(name, func(t *testing.T) {
			f, fake := newSocialFixture(t)
			if superuser {
				delete(f.users.memberships, f.principal)
			}
			state := f.users.users[f.principal]
			state.IsSuperuser = superuser
			f.users.users[f.principal] = state
			f.buildCompat()
			fake.result = social.ExchangeResult{PrincipalID: f.principal}
			response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"gho_test"}`, nil)
			body := decodeObject(t, response.Body.Bytes())
			if body["refresh_token"] != f.sessions.created[0].issued.Credential.Reveal() {
				t.Fatalf("refresh_token is not the session's credential: %s", response.Body)
			}
		})
	}
}
