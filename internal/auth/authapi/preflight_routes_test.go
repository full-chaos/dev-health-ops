package authapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// expectGoAPIUnavailable asserts go-api's own 503 for a session it cannot
// issue: its detail bytes and no Retry-After.
func expectGoAPIUnavailable(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503: %s", response.Code, response.Body)
	}
	if got := response.Header().Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After %q on a go-api route", got)
	}
	want := map[string]any{"detail": map[string]any{"message": sessionissue.UnavailableText}}
	if got := decodeObject(t, response.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("body %s, want go-api's %v", response.Body, want)
	}
}

// onEdDSA rebuilds the compat routes over an EdDSA issuer on signer.
func onEdDSA(t *testing.T, f *compatFixture, signer AccessSigner) *compatFixture {
	t.Helper()
	f.issuer = newEdDSAIssuer(t, signer, testAudience)
	f.buildCompat()
	return f
}

// compatWrites counts every call the compat doubles take that writes: the
// attempt counter, projections, sessions, audit rows, exchanges and revokes.
func compatWrites(f *compatFixture) int {
	f.projector.mu.Lock()
	projected := len(f.projector.userCalls) + len(f.projector.orgCalls)
	f.projector.mu.Unlock()
	f.sessions.mu.Lock()
	sessions := len(f.sessions.created) + len(f.sessions.legacy) + len(f.sessions.revoked) + len(f.sessions.revokedLegacy) + f.sessions.exchanges
	f.sessions.mu.Unlock()
	return f.attempts.failures + len(f.loginUsers.projected) + len(f.appended) + projected + sessions
}

// outageAndControl runs one request twice: under an EdDSA signer whose key
// cannot be read (503, nothing written, nothing signed) and under a readable
// key (the healthy answer, with the writes the outage skipped).
func outageAndControl(t *testing.T, healthyStatus int, build func(*testing.T, AccessSigner) *compatFixture,
	send func(*compatFixture) *httptest.ResponseRecorder) {
	t.Helper()
	signer := unreadableKeySigner(t)
	f := build(t, signer)
	expectGoAPIUnavailable(t, send(f))
	if n := compatWrites(f); n != 0 {
		t.Fatalf("%d writes before the token could be signed", n)
	}
	if n := signer.signs.Load(); n != 0 {
		t.Fatalf("%d signs after the refusal", n)
	}
	healthy := build(t, readableKeySigner(t))
	if code := send(healthy).Code; code != healthyStatus {
		t.Fatalf("control with a healthy signer: %d, want %d", code, healthyStatus)
	}
	if compatWrites(healthy) == 0 {
		t.Fatal("control with a healthy signer wrote nothing: the doubles do not observe the writes")
	}
}

func compatOnEdDSA(mutate func(*compatFixture)) func(*testing.T, AccessSigner) *compatFixture {
	return func(t *testing.T, signer AccessSigner) *compatFixture {
		f := newCompatFixture(t)
		if mutate != nil {
			mutate(f)
		}
		return onEdDSA(t, f, signer)
	}
}

func TestCompatRefreshRefusesBeforeExchangeWhenSignerUnavailable(t *testing.T) {
	opaque := func(f *compatFixture) *httptest.ResponseRecorder {
		return f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	}
	legacy := func(f *compatFixture) *httptest.ResponseRecorder {
		return f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	}
	cases := []struct {
		name    string
		mutate  func(*compatFixture)
		send    func(*compatFixture) *httptest.ResponseRecorder
		healthy int
	}{
		{"opaque accepted", func(f *compatFixture) { f.sessions.result = accepted(f.record) }, opaque, http.StatusOK},
		{"opaque user gone", func(f *compatFixture) {
			f.sessions.result = accepted(f.record)
			delete(f.users.users, f.principal)
		}, opaque, http.StatusUnauthorized},
		{"opaque reused", func(f *compatFixture) {
			f.sessions.result = session.Result{Reason: session.ReasonRefreshReused}
		}, opaque, http.StatusUnauthorized},
		{"legacy accepted", func(f *compatFixture) { f.sessions.legacyResult = accepted(f.record) }, legacy, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			outageAndControl(t, c.healthy, compatOnEdDSA(c.mutate), c.send)
		})
	}
}

func TestCompatLoginRefusesBeforeTheCredentialCheckWhenSignerUnavailable(t *testing.T) {
	good := func(f *compatFixture) *httptest.ResponseRecorder {
		return f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	}
	cases := []struct {
		name    string
		mutate  func(*compatFixture)
		send    func(*compatFixture) *httptest.ResponseRecorder
		healthy int
	}{
		{"success", nil, good, http.StatusOK},
		{"wrong password", nil, func(f *compatFixture) *httptest.ResponseRecorder {
			return f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", ""), nil)
		}, http.StatusUnauthorized},
		{"locked", func(f *compatFixture) { f.attempts.locked, f.attempts.remaining = true, 900*time.Second }, good, http.StatusTooManyRequests},
		{"email unverified", func(f *compatFixture) {
			provider := "local"
			user := f.loginUsers.byEmail[compatEmail]
			user.IsVerified, user.AuthProvider = false, &provider
			f.users.users[f.principal] = UserState{Email: "carol@example.com", IsActive: true}
		}, good, http.StatusOK},
		{"not a member", nil, func(f *compatFixture) *httptest.ResponseRecorder {
			return f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.orgD.String()), nil)
		}, http.StatusUnauthorized},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			outageAndControl(t, c.healthy, compatOnEdDSA(c.mutate), c.send)
		})
	}
}

// The limiter is charged before the signer check, so a caller over its
// budget gets the 429 during an outage too.
func TestALimitedLoginAnswers429BeforeTheSignerIsChecked(t *testing.T) {
	signer := unreadableKeySigner(t)
	f := onEdDSA(t, newCompatFixture(t), signer)
	for i := 0; i < loginAccountLimit.Count; i++ {
		expectGoAPIUnavailable(t, f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil))
	}
	response := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d after the budget, want 429: %s", response.Code, response.Body)
	}
	if n := compatWrites(f); n != 0 || f.attempts.failures != 0 {
		t.Fatalf("%d writes during the outage", n)
	}
}

func TestSocialLoginRefusesBeforeTheExchangeWhenSignerUnavailable(t *testing.T) {
	for name, c := range map[string]struct {
		program func(*compatFixture, *fakeSocial)
		healthy int
	}{
		"accepted": {func(f *compatFixture, fake *fakeSocial) {
			fake.result = social.ExchangeResult{PrincipalID: f.principal}
		}, http.StatusOK},
		"refused": {func(_ *compatFixture, fake *fakeSocial) { fake.err = social.ErrProviderRefused }, http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			fakes := map[*compatFixture]*fakeSocial{}
			build := func(t *testing.T, signer AccessSigner) *compatFixture {
				f, fake := newSocialFixture(t)
				c.program(f, fake)
				fakes[f] = fake
				return onEdDSA(t, f, signer)
			}
			var exchanged []int
			send := func(f *compatFixture) *httptest.ResponseRecorder {
				response := f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil)
				exchanged = append(exchanged, fakes[f].calls)
				return response
			}
			outageAndControl(t, c.healthy, build, send)
			if !reflect.DeepEqual(exchanged, []int{0, 1}) {
				t.Fatalf("exchanges (outage, healthy) = %v, want [0 1]", exchanged)
			}
		})
	}
}

// credOnEdDSA serves the credential routes over an EdDSA issuer. The bearer
// is the HS256 access token the EdDSA issuer still verifies.
func credOnEdDSA(t *testing.T, signer AccessSigner) (*credFixture, map[string]string) {
	t.Helper()
	c := newCredFixtureOn(t, httpapi.NewMemoryCounters(nil), newFixtureWith(t, newEdDSAIssuer(t, signer, testAudience)))
	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := hs.Access(c.grant())
	if err != nil {
		t.Fatal(err)
	}
	return c, map[string]string{"Authorization": "Bearer " + token}
}

func credWrites(c *credFixture) int {
	c.fakes.mu.Lock()
	defer c.fakes.mu.Unlock()
	c.sessions.mu.Lock()
	defer c.sessions.mu.Unlock()
	return c.fakes.pwdSet + c.fakes.revoked + c.fakes.created + len(c.sessions.created) + len(c.sessions.revoked) + len(c.sessions.revokedAll)
}

func TestChangePasswordRefusesBeforeTheChangeWhenSignerUnavailable(t *testing.T) {
	const good = `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`
	cases := []struct {
		name    string
		body    string
		mutate  func(*credFixture)
		healthy int
	}{
		{"changed", good, nil, http.StatusOK},
		{"current rejected", `{"current_password":"not-the-password","new_password":"Brand-new-pass-77!"}`, nil, http.StatusForbidden},
		{"policy", `{"current_password":"current-password-1","new_password":"aaaaaaaa"}`, nil, http.StatusUnprocessableEntity},
		{"managed elsewhere", good, func(c *credFixture) {
			other := "github"
			c.fakes.user.AuthProvider = &other
		}, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer := unreadableKeySigner(t)
			c, bearer := credOnEdDSA(t, signer)
			if tc.mutate != nil {
				tc.mutate(c)
			}
			expectGoAPIUnavailable(t, c.call(http.MethodPost, PatternChangePassword, tc.body, bearer))
			if n := credWrites(c); n != 0 || c.fakes.user.PasswordHash == nil || *c.fakes.user.PasswordHash != "stored-hash" {
				t.Fatalf("%d writes, hash %v: the password changed before the token could be signed", n, c.fakes.user.PasswordHash)
			}
			if n := signer.signs.Load(); n != 0 {
				t.Fatalf("%d signs after the refusal", n)
			}
			healthy, hb := credOnEdDSA(t, readableKeySigner(t))
			if tc.mutate != nil {
				tc.mutate(healthy)
			}
			if code := healthy.call(http.MethodPost, PatternChangePassword, tc.body, hb).Code; code != tc.healthy {
				t.Fatalf("control with a healthy signer: %d, want %d", code, tc.healthy)
			}
			if tc.healthy == http.StatusOK && credWrites(healthy) == 0 {
				t.Fatal("control with a healthy signer wrote nothing: the doubles do not observe the writes")
			}
		})
	}
}

// The federated session path checks again before its session write: OIDC
// starts nothing, SAML answers go-api's 503.
func TestSSOSessionIssueRefusesBeforeTheSessionWriteWhenSignerUnavailable(t *testing.T) {
	signer := unreadableKeySigner(t)
	f := onEdDSA(t, newCompatFixture(t), signer)
	if _, err := f.ssoSessions(t).OIDC().Issue(context.Background(), oidc.SessionRequest{
		PrincipalID: f.principal, OrgID: &f.org, Role: "owner", AMR: []string{"fed"},
	}); err == nil {
		t.Fatal("an OIDC session was issued while the signer cannot sign")
	}
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &f.org, AMR: []string{"fed"}})
	expectGoAPIUnavailable(t, w)
	if n := compatWrites(f); n != 0 || signer.signs.Load() != 0 {
		t.Fatalf("%d writes, %d signs before the token could be signed", n, signer.signs.Load())
	}
}

// The SSO adapters' Preflight is the issuer's: nil under hs256, an
// unavailable mark under an EdDSA signer that cannot sign.
func TestSSOAdaptersPreflightIsTheIssuers(t *testing.T) {
	if textAuthUnavailable != sessionissue.UnavailableText {
		t.Fatalf("textAuthUnavailable %q is not go-api's %q", textAuthUnavailable, sessionissue.UnavailableText)
	}
	hs := newCompatFixture(t).ssoSessions(t)
	if err := hs.OIDC().Preflight(context.Background()); err != nil {
		t.Fatalf("hs256 OIDC preflight: %v", err)
	}
	if err := hs.SAML().Preflight(context.Background()); err != nil {
		t.Fatalf("hs256 SAML preflight: %v", err)
	}
	f := onEdDSA(t, newCompatFixture(t), unreadableKeySigner(t))
	sessions := f.ssoSessions(t)
	if err := sessions.OIDC().Preflight(context.Background()); !errors.Is(err, oidc.ErrSessionUnavailable) {
		t.Fatalf("OIDC preflight %v, want oidc.ErrSessionUnavailable", err)
	}
	if err := sessions.SAML().Preflight(context.Background()); err == nil {
		t.Fatal("SAML preflight passed while the signer cannot sign")
	}
	_, err := sessions.OIDC().Issue(context.Background(), oidc.SessionRequest{PrincipalID: f.principal, OrgID: &f.org, Role: "owner"})
	if !errors.Is(err, oidc.ErrSessionUnavailable) {
		t.Fatalf("OIDC issue %v, want oidc.ErrSessionUnavailable", err)
	}
}

// hs256_compat: every route answers as before, with its writes.
func TestPreflightRoutesUnderHS256AreUnchanged(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.result = accepted(f.record)
	if r := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil); r.Code != http.StatusOK || f.sessions.minted != 1 {
		t.Fatalf("compat refresh %d, minted %d: %s", r.Code, f.sessions.minted, r.Body)
	}
	if r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil); r.Code != http.StatusOK || len(f.sessions.created) != 1 {
		t.Fatalf("login %d, created %d: %s", r.Code, len(f.sessions.created), r.Body)
	}
	s, fake := newSocialFixture(t)
	fake.result = social.ExchangeResult{PrincipalID: s.principal}
	if r := s.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil); r.Code != http.StatusOK || fake.calls != 1 {
		t.Fatalf("social %d, exchanges %d: %s", r.Code, fake.calls, r.Body)
	}
	c := newCredFixture(t)
	r := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, c.bearer(c.grant()))
	if r.Code != http.StatusOK || c.fakes.pwdSet != 1 {
		t.Fatalf("change password %d, writes %d: %s", r.Code, c.fakes.pwdSet, r.Body)
	}
	if strings.Contains(f.logs.String()+c.logs.String(), reasonDependency) {
		t.Fatal("an hs256 route logged a dependency refusal")
	}
}

// One EdDSA issuer behind every route runs one check per window.
func TestOneCheckPerWindowAcrossTheRoutes(t *testing.T) {
	signer := readableKeySigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	withClock(t, issuer)
	f, fake := newSocialFixture(t)
	fake.result = social.ExchangeResult{PrincipalID: f.principal}
	f.sessions.result = accepted(f.record)
	f.issuer = issuer
	f.buildCompat()
	c := newCredFixtureOn(t, httpapi.NewMemoryCounters(nil), newFixtureWith(t, issuer))
	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := hs.Access(c.grant())
	if err != nil {
		t.Fatal(err)
	}
	answers := []int{
		f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil).Code,
		f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil).Code,
		f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil).Code,
		c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`,
			map[string]string{"Authorization": "Bearer " + bearer}).Code,
	}
	if !reflect.DeepEqual(answers, []int{200, 200, 200, 200}) {
		t.Fatalf("answers %v", answers)
	}
	if n := signer.checks.Load(); n != 1 {
		t.Fatalf("%d signer checks across the routes in one window, want 1", n)
	}
}

// Body, bearer and limit refusals answer before the check and never run it.
func TestRefusalsBeforeTheCheckNeverRunIt(t *testing.T) {
	signer := readableKeySigner(t)
	f, _ := newSocialFixture(t)
	onEdDSA(t, f, signer)
	c, bearer := credOnEdDSA(t, signer)
	answers := []int{
		f.do(http.MethodPost, PatternCompatRefresh, `{}`, nil).Code,
		f.do(http.MethodPost, PatternCompatLogin, `{"email":"x"}`, nil).Code,
		f.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github"}`, nil).Code,
		c.call(http.MethodPost, PatternChangePassword, `{"current_password":"a","new_password":"Brand-new-pass-77!"}`, nil).Code,
		c.call(http.MethodPost, PatternChangePassword, `{"new_password":"Brand-new-pass-77!"}`, bearer).Code,
	}
	if want := []int{422, 422, 422, 401, 422}; !reflect.DeepEqual(answers, want) {
		t.Fatalf("answers %v, want %v", answers, want)
	}
	if n := signer.checks.Load(); n != 0 {
		t.Fatalf("%d signer checks for requests refused before the check", n)
	}
	for i := 0; i < changePasswordLimit.Count; i++ {
		c.call(http.MethodPost, PatternChangePassword, `{"current_password":"not-the-password","new_password":"Brand-new-pass-77!"}`, bearer)
	}
	before := signer.checks.Load()
	limited := c.call(http.MethodPost, PatternChangePassword, `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, bearer)
	if limited.Code != http.StatusTooManyRequests || signer.checks.Load() != before {
		t.Fatalf("over the budget: %d, checks %d -> %d; want 429 with no new check", limited.Code, before, signer.checks.Load())
	}
	if c.fakes.pwdSet != 0 {
		t.Fatal("a refused change wrote the password")
	}
}
