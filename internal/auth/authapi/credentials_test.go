package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

type credLinkKey struct {
	purpose local.Purpose
	hash    [32]byte
}

// credFakes backs a real local.Flows: one account, a link store that is
// single-use and purpose-bound, a counting mailer.
type credFakes struct {
	mu        sync.Mutex
	user      *local.User
	email     string
	links     map[credLinkKey]uuid.UUID
	issued    []uuid.UUID
	mails     []local.Message
	password  string
	pwdSet    int
	revoked   int
	revokeErr error
	created   int
	issueErr  error
	principal uuid.UUID
	org       *uuid.UUID
}

func (c *credFakes) ByEmail(_ context.Context, email string) (*local.User, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if email == c.email {
		return c.user, nil
	}
	return nil, nil
}

func (c *credFakes) ByID(_ context.Context, id uuid.UUID) (*local.User, error) {
	if c.user != nil && id == c.user.ID {
		return c.user, nil
	}
	return nil, nil
}

func (c *credFakes) ProvisionLocal(context.Context, local.ProvisionLocalRequest) (uuid.UUID, error) {
	return uuid.Nil, errors.New("not used")
}
func (c *credFakes) SetEmailVerified(_ context.Context, id uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.user.IsVerified = true
	return nil
}
func (c *credFakes) SetPasswordHash(_ context.Context, _ uuid.UUID, hash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pwdSet++
	c.user.PasswordHash = &hash
	return nil
}
func (c *credFakes) Issue(_ context.Context, req local.IssueRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.issued = append(c.issued, req.PrincipalID)
	if req.PrincipalID != uuid.Nil {
		c.links[credLinkKey{req.Purpose, req.TokenHash}] = req.PrincipalID
	}
	return c.issueErr
}
func (c *credFakes) Redeem(_ context.Context, req local.RedeemRequest) (uuid.UUID, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := credLinkKey{req.Purpose, req.TokenHash}
	id, ok := c.links[key]
	delete(c.links, key)
	return id, ok, nil
}
func (c *credFakes) WithPrincipalLock(ctx context.Context, _ uuid.UUID, fn func(context.Context) error) error {
	return fn(ctx)
}
func (c *credFakes) Send(_ context.Context, msg local.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mails = append(c.mails, msg)
	return nil
}
func (c *credFakes) Create(_ context.Context, id uuid.UUID, org *uuid.UUID, _ []string) (session.Issued, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created++
	return session.Issued{Session: session.Record{ID: uuid.New(), PrincipalID: id, OrganizationID: org}}, nil
}
func (c *credFakes) Current(context.Context, uuid.UUID) (session.Record, error) {
	return session.Record{}, session.ErrSessionNotFound
}
func (c *credFakes) Revoke(context.Context, uuid.UUID, session.Actor) (bool, error) {
	return true, nil
}
func (c *credFakes) RevokeAll(context.Context, uuid.UUID, session.Actor) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revoked++
	return 1, c.revokeErr
}

const credKnownEmail = "member@example.com"

type credFixture struct {
	*fixture
	fakes  *credFakes
	routes http.Handler
	logs   *bytes.Buffer
}

func newCredFixture(t *testing.T) *credFixture {
	t.Helper()
	return newCredFixtureWith(t, httpapi.NewMemoryCounters(nil))
}

func newCredFixtureWith(t *testing.T, counters httpapi.CounterStore) *credFixture {
	t.Helper()
	f := newFixture(t)
	hash := "stored-hash"
	provider := "local"
	fakes := &credFakes{
		email: credKnownEmail, links: map[credLinkKey]uuid.UUID{}, principal: f.principal,
		user: &local.User{ID: f.principal, IsActive: true, IsVerified: false, PasswordHash: &hash, AuthProvider: &provider},
	}
	flows, err := local.NewFlows(local.FlowsConfig{
		Accounts: fakes, Links: fakes, Sessions: fakes, Mail: fakes, Locker: fakes, UsersOwnedByAuth: true,
		Compare: func(stored, given string) bool { return stored == "stored-hash" && given == "current-password-1" },
		Hash:    func(string) (string, error) { return "new-hash", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	routes, err := CredentialRoutes(CredentialDeps{
		Flows: flows, Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Limits: mustLimits(t, counters),
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return &credFixture{fixture: f, fakes: fakes, routes: mux, logs: logs}
}

func (c *credFixture) call(method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	c.routes.ServeHTTP(response, request)
	return response
}

func (c *credFixture) linkFor(purpose local.Purpose) (string, bool) {
	for _, msg := range c.fakes.mails {
		if msg.Purpose == purpose && msg.Deliver {
			return msg.Secret.Reveal(), true
		}
	}
	return "", false
}

func jsonBody(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	return out
}

func TestForgotAndResendAnswerTheSameForKnownAndUnknownAddresses(t *testing.T) {
	for _, route := range []string{PatternForgotPassword, PatternResendVerification} {
		known, unknown := newCredFixture(t), newCredFixture(t)
		a := known.call(http.MethodPost, route, `{"email":"`+credKnownEmail+`"}`, nil)
		b := unknown.call(http.MethodPost, route, `{"email":"nobody@example.com"}`, nil)
		if a.Code != http.StatusOK || a.Code != b.Code || a.Body.String() != b.Body.String() {
			t.Fatalf("%s: known %d %s, unknown %d %s", route, a.Code, a.Body, b.Code, b.Body)
		}
		if len(known.fakes.issued) != 1 || len(unknown.fakes.issued) != 1 {
			t.Fatalf("%s: issue calls known %d unknown %d, want 1 and 1", route, len(known.fakes.issued), len(unknown.fakes.issued))
		}
		if len(known.fakes.mails) != len(unknown.fakes.mails) {
			t.Fatalf("%s: send calls differ", route)
		}
		if unknown.fakes.issued[0] != uuid.Nil || unknown.fakes.mails[0].Deliver {
			t.Fatalf("%s: unknown address was not a decoy", route)
		}
		if !known.fakes.mails[0].Deliver {
			t.Fatalf("%s: known address got no link", route)
		}
	}
}

func TestVerifyLinkIsSingleUseAndBoundToItsPurpose(t *testing.T) {
	c := newCredFixture(t)
	c.call(http.MethodPost, PatternResendVerification, `{"email":"`+credKnownEmail+`"}`, nil)
	secret, ok := c.linkFor(local.PurposeEmailVerification)
	if !ok {
		t.Fatal("no verification link was issued")
	}
	reset := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	if reset.Code != http.StatusBadRequest || c.fakes.pwdSet != 0 {
		t.Fatalf("a verification link reset a password: %d", reset.Code)
	}
	first := c.call(http.MethodGet, PatternVerifyEmail+"?token="+secret, "", nil)
	if first.Code != http.StatusOK || jsonBody(t, first)["verified"] != true || !c.fakes.user.IsVerified {
		t.Fatalf("first use: %d %s", first.Code, first.Body)
	}
	again := c.call(http.MethodGet, PatternVerifyEmail+"?token="+secret, "", nil)
	if again.Code != http.StatusBadRequest {
		t.Fatalf("second use = %d, want 400", again.Code)
	}
}

func TestResetLinkIsSingleUseRevokesSessionsAndIsBoundToItsPurpose(t *testing.T) {
	c := newCredFixture(t)
	c.call(http.MethodPost, PatternForgotPassword, `{"email":"`+credKnownEmail+`"}`, nil)
	secret, ok := c.linkFor(local.PurposePasswordReset)
	if !ok {
		t.Fatal("no reset link was issued")
	}
	if r := c.call(http.MethodGet, PatternVerifyEmail+"?token="+secret, "", nil); r.Code != http.StatusBadRequest || c.fakes.user.IsVerified {
		t.Fatalf("a reset link verified an address: %d", r.Code)
	}
	short := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"short"}`, nil)
	if short.Code != http.StatusUnprocessableEntity {
		t.Fatalf("short password = %d, want 422", short.Code)
	}
	long := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"`+strings.Repeat("é", 40)+`"}`, nil)
	if long.Code != http.StatusUnprocessableEntity || c.fakes.pwdSet != 0 {
		t.Fatalf("over-long password = %d", long.Code)
	}
	ok1 := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	if ok1.Code != http.StatusOK || c.fakes.pwdSet != 1 || c.fakes.revoked != 1 {
		t.Fatalf("reset: %d, writes %d, revokes %d", ok1.Code, c.fakes.pwdSet, c.fakes.revoked)
	}
	again := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	if again.Code != http.StatusBadRequest || c.fakes.pwdSet != 1 {
		t.Fatalf("second use = %d", again.Code)
	}
}

func TestChangePasswordNeedsTheBearerAndTheCurrentPassword(t *testing.T) {
	c := newCredFixture(t)
	body := `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`
	if r := c.call(http.MethodPost, PatternChangePassword, body, nil); r.Code != http.StatusUnauthorized || c.fakes.pwdSet != 0 {
		t.Fatalf("no bearer = %d", r.Code)
	}
	header := c.bearer(c.grant())
	wrong := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"not-the-password","new_password":"Brand-new-pass-77!"}`, header)
	if wrong.Code != http.StatusForbidden || c.fakes.pwdSet != 0 || c.fakes.revoked != 0 {
		t.Fatalf("wrong current password = %d", wrong.Code)
	}
	invalid := c.call(http.MethodPost, PatternChangePassword, `{"new_password":"Brand-new-pass-77!"}`, header)
	if invalid.Code != http.StatusUnprocessableEntity || strings.Contains(invalid.Body.String(), "Brand-new") {
		t.Fatalf("missing current password = %d %s", invalid.Code, invalid.Body)
	}
	weak := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"aaaaaaaa"}`, header)
	if weak.Code != http.StatusUnprocessableEntity || c.fakes.pwdSet != 0 {
		t.Fatalf("weak password = %d", weak.Code)
	}
	done := c.call(http.MethodPost, PatternChangePassword, body, header)
	if done.Code != http.StatusOK || c.fakes.pwdSet != 1 || c.fakes.revoked != 1 || c.fakes.created != 1 {
		t.Fatalf("change: %d writes %d revokes %d sessions %d; %s", done.Code, c.fakes.pwdSet, c.fakes.revoked, c.fakes.created, done.Body)
	}
	answer := jsonBody(t, done)
	if token, _ := answer["access_token"].(string); token == "" || answer["refresh_token"] == nil || answer["token_type"] != "bearer" {
		t.Fatalf("change answer lacks the new session: %v", answer)
	}
	if done.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("password answers must not be cached")
	}
}

func TestChangePasswordOfAnExternallyManagedAccountIsRefused(t *testing.T) {
	c := newCredFixture(t)
	other := "github"
	c.fakes.user.AuthProvider = &other
	r := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, c.bearer(c.grant()))
	if r.Code != http.StatusForbidden || c.fakes.pwdSet != 0 {
		t.Fatalf("managed account = %d", r.Code)
	}
}

func TestCredentialRoutesLimitAsGoAPIDoes(t *testing.T) {
	cases := []struct {
		name, method, path, body string
		allowed                  int
		header                   map[string]string
	}{
		{"forgot", http.MethodPost, PatternForgotPassword, `{"email":"a@example.com"}`, 3, nil},
		{"resend", http.MethodPost, PatternResendVerification, `{"email":"a@example.com"}`, 3, nil},
		{"verify", http.MethodGet, PatternVerifyEmail + "?token=x", "", 10, nil},
	}
	for _, tc := range cases {
		c := newCredFixture(t)
		for i := 1; i <= tc.allowed+1; i++ {
			r := c.call(tc.method, tc.path, tc.body, tc.header)
			want := http.StatusOK
			if tc.name == "verify" {
				want = http.StatusBadRequest
			}
			if i > tc.allowed {
				want = http.StatusTooManyRequests
			}
			if r.Code != want {
				t.Fatalf("%s call %d = %d, want %d", tc.name, i, r.Code, want)
			}
		}
	}
	c := newCredFixture(t)
	header := c.bearer(c.grant())
	for i := 1; i <= 6; i++ {
		r := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"not-the-password","new_password":"Brand-new-pass-77!"}`, header)
		want := http.StatusForbidden
		if i > 5 {
			want = http.StatusTooManyRequests
		}
		if r.Code != want {
			t.Fatalf("change call %d = %d, want %d", i, r.Code, want)
		}
	}
}

func TestRefusedBodiesCostNoAllowance(t *testing.T) {
	c := newCredFixture(t)
	for i := 0; i < 8; i++ {
		if r := c.call(http.MethodPost, PatternForgotPassword, `{"email":"not an address"}`, nil); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("malformed body = %d", r.Code)
		}
	}
	if r := c.call(http.MethodPost, PatternForgotPassword, `{"email":"a@example.com"}`, nil); r.Code != http.StatusOK {
		t.Fatalf("valid body after refused ones = %d", r.Code)
	}
}

func TestCredentialRoutesLogNoSecretOrAddress(t *testing.T) {
	c := newCredFixture(t)
	c.fakes.issueErr = errors.New("db down for member@example.com")
	c.call(http.MethodPost, PatternForgotPassword, `{"email":"`+credKnownEmail+`"}`, nil)
	c.fakes.issueErr = nil
	c.call(http.MethodPost, PatternForgotPassword, `{"email":"`+credKnownEmail+`"}`, nil)
	secret, _ := c.linkFor(local.PurposePasswordReset)
	c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	out := c.logs.String() + c.fixture.logs.String()
	for _, banned := range []string{credKnownEmail, secret, "another-password-1", "db down"} {
		if banned != "" && strings.Contains(out, banned) {
			t.Fatal("a log line carries a secret or an address")
		}
	}
}

func TestStoreFailureAnswersGoAPIs500(t *testing.T) {
	c := newCredFixture(t)
	c.fakes.issueErr = errors.New("store down")
	r := c.call(http.MethodPost, PatternForgotPassword, `{"email":"`+credKnownEmail+`"}`, nil)
	if r.Code != http.StatusInternalServerError {
		t.Fatalf("store failure = %d", r.Code)
	}
}

func TestCredentialRoutesRequireTheirCollaborators(t *testing.T) {
	if _, err := CredentialRoutes(CredentialDeps{}); err == nil {
		t.Fatal("empty deps built routes")
	}
}

func TestCredentialRoutePatternsAreGoAPIs(t *testing.T) {
	c := newCredFixture(t)
	_ = c
	routes, err := CredentialRoutes(CredentialDeps{Flows: &local.Flows{}, Sessions: c.sessions, Users: c.users, Issuer: c.issuer, Limits: mustLimits(t, httpapi.NewMemoryCounters(nil))})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, route := range routes {
		got[route.Method+" "+route.Pattern] = true
	}
	for _, want := range []string{
		"GET /api/v1/auth/verify", "POST /api/v1/auth/resend-verification", "POST /api/v1/auth/forgot-password",
		"POST /api/v1/auth/reset-password", "POST /api/v1/auth/change-password",
	} {
		if !got[want] {
			t.Fatalf("route %q is not mounted", want)
		}
	}
}

func TestChangePasswordRefusesAnImpersonatedBearer(t *testing.T) {
	c := newCredFixture(t)
	grant := c.grant()
	other := uuid.NewString()
	token, err := c.issuer.signer.Access(edgetoken.AccessClaims{
		UserID: grant.PrincipalID.String(), Email: grant.User.Email, OrgID: grant.OrganizationID.String(), Role: grant.Role,
		TokenVersion: grant.User.TokenVersion, SessionID: ptr(session.WireSessionID(grant.SessionID)), ImpersonatingUserID: &other,
	}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	r := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`,
		map[string]string{"Authorization": "Bearer " + token})
	if r.Code != http.StatusForbidden || c.fakes.pwdSet != 0 || c.fakes.revoked != 0 {
		t.Fatalf("impersonated bearer = %d, writes %d", r.Code, c.fakes.pwdSet)
	}
}

func TestChangePasswordReadsTheBodyBeforeTheBearer(t *testing.T) {
	c := newCredFixture(t)
	r := c.call(http.MethodPost, PatternChangePassword, `{not json`, nil)
	if r.Code != http.StatusUnprocessableEntity {
		t.Fatalf("malformed body without a bearer = %d, want go-api's 422", r.Code)
	}
	if r := c.call(http.MethodPost, PatternChangePassword, `{}`, nil); r.Code != http.StatusUnauthorized {
		t.Fatalf("decodable body without a bearer = %d, want 401", r.Code)
	}
}

func TestFailedRevocationFailsClosedOnResetAndChange(t *testing.T) {
	c := newCredFixture(t)
	c.fakes.revokeErr = errors.New("sessions store down")
	c.call(http.MethodPost, PatternForgotPassword, `{"email":"`+credKnownEmail+`"}`, nil)
	secret, _ := c.linkFor(local.PurposePasswordReset)
	r := c.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	if r.Code != http.StatusInternalServerError || strings.Contains(r.Body.String(), "another-password") {
		t.Fatalf("reset with failed revocation = %d, want go-api's 500", r.Code)
	}
	c = newCredFixture(t)
	c.fakes.revokeErr = errors.New("sessions store down")
	header := c.bearer(c.grant())
	r = c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, header)
	if r.Code != http.StatusInternalServerError || strings.Contains(r.Body.String(), "access_token") {
		t.Fatalf("change with failed revocation = %d %s, want a 500 without a token", r.Code, r.Body)
	}
	if !strings.Contains(c.logs.String(), reasonSessionsNotRevoked) || strings.Contains(c.logs.String(), "sessions store down") {
		t.Fatal("the failed revocation must be logged by reason code only")
	}
}
