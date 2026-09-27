//go:build integration

// Real-producer coverage for CHAOS-6986 (initiateOAuthAuth / oauthCallback
// / initiateOAuthByType).
//
// Same AGENTS.md oracle exception as CHAOS-6658/6659: process_oauth_*'s
// state is decorative (oauthstate.go's doc comment) but not structurally
// unreachable the way OIDC's callback was -- an attacker COULD reach
// oauth_callback in production Python, state check or no. There is still
// no live Python producer worth diffing the substantive exchange against,
// because the fix (a real AEAD state) has no Python analog to compare
// against; Python's own dead state is pinned separately by the shared
// ssovenue oracle (unchanged by this PR).
//
// This file drives the full round trip against a fake GitLab-shaped OAuth
// provider (GitLab is the one provider Python's own OAuthConfig lets a
// caller point at an arbitrary base_url -- github.com and
// accounts.google.com are hardcoded in both planes, so a fake server
// cannot stand in for them without inventing a production-code override
// neither plane has). The authorization-URL-building test below covers
// GitHub and Google's request shape without a network round trip;
// oauthprovider's own unit tests already cover FetchUserInfo's per-
// provider JSON parsing for all three, so this file does not re-prove
// that.
package sso_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// newCookieClient is a *http.Client whose jar carries the D2745 P1-3
// login-nonce cookie /authorize sets to the following /callback POST --
// the same technique saml_integration_test.go's TestSAMLACSHonoursAllowIdpInitiatedFalse
// (D2744/D2745, already landed) uses for SAML's own login-nonce cookie.
func newCookieClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Jar: jar}
}

// fakeGitLab is a minimal fake self-hosted GitLab: a token endpoint that
// exchanges any code for a fixed access token (recording what it
// received, for the client_secret round-trip assertion), and a
// /api/v4/user endpoint returning whatever profile the test set.
type fakeGitLab struct {
	server *httptest.Server

	email, username, fullName, avatarURL, userID string
	tokenStatus, userStatus                      int // 0 means 200.
	// confirmed is D2745 P1-1: GitLab's own confirmed_at signal. true by
	// default so every EXISTING test (a verified account) is unaffected;
	// set false only by the test that specifically exercises the refusal.
	confirmed bool

	receivedClientID, receivedClientSecret, receivedRedirectURI string
}

func startFakeGitLab(t *testing.T) *fakeGitLab {
	t.Helper()
	// D2745 P1-2/D2752: this fixture's own base_url is a real
	// http://127.0.0.1 loopback (httptest.Server never serves https),
	// which validateOAuthConfigHTTPS (oauth.go) allows unconditionally --
	// D2752 removed the env-var gate: loopback is always accepted, no
	// switch of any kind.
	g := &fakeGitLab{email: "gitlab.user@example.test", username: "gluser", fullName: "GitLab User", userID: "42", confirmed: true}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", g.serveToken)
	mux.HandleFunc("GET /api/v4/user", g.serveUser)
	g.server = httptest.NewServer(mux)
	t.Cleanup(g.server.Close)
	return g
}

func (g *fakeGitLab) serveToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.receivedClientID = r.Form.Get("client_id")
	g.receivedClientSecret = r.Form.Get("client_secret")
	g.receivedRedirectURI = r.Form.Get("redirect_uri")
	if g.tokenStatus != 0 {
		w.WriteHeader(g.tokenStatus)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": "test-gitlab-access-token", "token_type": "bearer", "expires_in": 7200,
	})
}

func (g *fakeGitLab) serveUser(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer test-gitlab-access-token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if g.userStatus != 0 {
		w.WriteHeader(g.userStatus)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
		return
	}
	body := map[string]any{
		"id": g.userID, "email": g.email, "username": g.username, "name": g.fullName, "avatar_url": g.avatarURL,
	}
	if g.confirmed {
		body["confirmed_at"] = "2026-01-01T00:00:00Z"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func gitlabConfig(baseURL string) string {
	return fmt.Sprintf(`{"client_id":"gitlab-client","scopes":["read_user","email"],"base_url":%q}`, baseURL)
}

func TestOAuthGitLabFullRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "new.gitlab.user@allowed.example"
	gitlab.avatarURL = "https://gitlab.example/avatar.png"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
		clientSecretEncryptedFallback: "gitlab-secret",
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	if authResp["state"] == "" {
		t.Fatalf("authorize: missing state, got %v", authResp)
	}
	authURL, _ := url.Parse(authResp["authorization_url"].(string))
	if got := authURL.Query().Get("client_id"); got != "gitlab-client" {
		t.Fatalf("authorization_url client_id=%q, want gitlab-client", got)
	}
	if got := authURL.Query().Get("scope"); got != "read_user email" {
		t.Fatalf("authorization_url scope=%q, want %q", got, "read_user email")
	}

	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusOK {
		t.Fatalf("callback: status=%d body=%v", status, body)
	}
	if body["access_token"] == "" || body["refresh_token"] == "" {
		t.Fatalf("callback: expected a token pair, got %v", body)
	}
	if body["email"] != gitlab.email {
		t.Fatalf("callback: email=%v, want %s", body["email"], gitlab.email)
	}
	if gitlab.receivedClientSecret != "gitlab-secret" {
		t.Fatalf("gitlab token endpoint received client_secret=%q, want the decrypted stored value", gitlab.receivedClientSecret)
	}

	// r1 review (CHAOS-6986, P2): fetchOAuthUserInfo previously never
	// populated AvatarURL at all for any provider, so this column was
	// silently NULL forever (an earlier version of this test asserted
	// exactly that NULL, which proved the bug rather than a feature).
	var count int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = $1 AND auth_provider = 'gitlab' AND avatar_url = $2`,
		gitlab.email, gitlab.avatarURL).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("users: got %d rows for the new auto-provisioned user, want 1 (auth_provider='gitlab')", count)
	}
	var lastLogin any
	if err := st.pool.QueryRow(ctx, `SELECT last_login_at FROM sso_providers WHERE id = $1`, providerID).Scan(&lastLogin); err != nil {
		t.Fatal(err)
	}
	if lastLogin == nil {
		t.Fatal("record_login: sso_providers.last_login_at was not set")
	}
	var auditCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'sso_login' AND status = 'success'`,
		orgID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("audit_logs: got %d success sso_login rows, want 1", auditCount)
	}
}

// TestOAuthTwoConcurrentAuthorizeFlowsBothComplete pins D2759's P1-B fix
// directly: two /authorize calls in the SAME browser (one cookie jar --
// two tabs, or a retry) must not collide. Before the fix, both flows set
// the identical fixed cookie name, so the second silently overwrote the
// first in the jar and the first flow's later /callback failed with a
// false "state mismatch" even though its own state was perfectly valid.
func TestOAuthTwoConcurrentAuthorizeFlowsBothComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)

	authorize := func() string {
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		state, _ := authResp["state"].(string)
		if state == "" {
			t.Fatal("authorize: missing state")
		}
		return state
	}

	// Both /authorize calls happen BEFORE either /callback -- exactly the
	// two-tab shape: flow A's cookie must still be there, unclobbered,
	// when flow A finally completes, even though flow B's /authorize ran
	// in between. Both flows are the SAME user (two tabs logging into
	// the same account) -- a distinct fake identity per flow would need
	// its own distinct username/external ID too (the fixture's fake
	// GitLab server has neither vary per call), which is a fixture
	// concern orthogonal to what this test actually pins.
	stateA := authorize()
	stateB := authorize()
	if stateA == stateB {
		t.Fatal("expected two distinct state tokens for two distinct /authorize calls")
	}

	complete := func(state string) {
		t.Helper()
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
			map[string]any{"code": "c", "state": state})
		if status != http.StatusOK {
			t.Fatalf("callback: status=%d body=%v -- the two flows' login-nonce cookies collided", status, body)
		}
		if body["email"] != gitlab.email {
			t.Fatalf("callback: email=%v, want %s", body["email"], gitlab.email)
		}
	}

	// Flow A completes LAST, after flow B's /authorize already ran -- the
	// case the shared fixed cookie name used to break.
	complete(stateB)
	complete(stateA)
}

// TestOAuthCallbackRefusesATamperedState pins D2738 for OAuth: a state
// that fails to authenticate never mutates the provider row, mirroring
// OIDC's own D2738 tests (TestOIDCCallbackRefusesATamperedState et al.).
func TestOAuthCallbackRefusesATamperedState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	state, _ := authResp["state"].(string)
	tampered := flipMiddleByte(state)

	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": tampered})
	if status != http.StatusBadRequest || body["detail"] != "OAuth authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OAuth authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

// TestOAuthCallbackRefusesAMissingLoginNonceCookie and
// TestOAuthCallbackRefusesATamperedLoginNonceCookie pin D2745's P1-3
// browser-binding class ruling: a genuine, unexpired state alone is not
// enough -- the login nonce cookie set at /authorize must also be
// present and match (hashOAuthLoginNonce's doc comment, oauthstate.go).
// A missing cookie is exactly what a cross-site (login-CSRF) submission
// looks like: SameSite=Lax withholds the cookie on a cross-site POST.
func TestOAuthCallbackRefusesAMissingLoginNonceCookie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	// A real /authorize call, but WITHOUT a cookie jar: the Set-Cookie is
	// dropped on the floor, exactly as if the following /callback POST
	// arrived from a different browser.
	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OAuth authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OAuth authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

func TestOAuthCallbackRefusesATamperedLoginNonceCookie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}

	// Tamper with the jar's own cookie value in place -- the state itself
	// stays genuine and unexpired; only the browser-side half of the
	// binding is wrong, which is exactly the case this check exists to
	// catch (a genuine state presented by the wrong browser). D2759: the
	// cookie's Path is now scoped to this provider's own callback route
	// (not "/"), so it must be queried and re-set AT that path -- and its
	// NAME is per-flow (suffixed by a random flow ID), so match by prefix
	// rather than the old fixed literal. jar.Cookies() also does not
	// report a cookie's own Path (Path is not part of a Cookie: request
	// header) -- restoring it before the second SetCookies is what makes
	// that call a REPLACE instead of silently adding a second, untampered
	// cookie of the same name.
	callbackURL, err := url.Parse(st.server.URL + "/api/v1/auth/oauth/" + providerID.String() + "/callback")
	if err != nil {
		t.Fatal(err)
	}
	cookies := client.Jar.Cookies(callbackURL)
	found := false
	for _, c := range cookies {
		if strings.HasPrefix(c.Name, "dho_oauth_login_nonce_") {
			c.Value = flipMiddleByte(c.Value)
			c.Path = callbackURL.Path
			found = true
		}
	}
	if !found {
		t.Fatal("authorize: expected a dho_oauth_login_nonce_<flowID> cookie")
	}
	client.Jar.SetCookies(callbackURL, cookies)

	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OAuth authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OAuth authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

// flipMiddleByte is shared with oidc_integration_test.go/
// saml_integration_test.go (same sso_test package) -- see its doc comment
// there.

func TestOAuthCallbackRefusesAnExpiredState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	st.clock.freeze(time.Now())
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	// oauthStateTTL (oauthstate.go) is 10 minutes.
	st.clock.advance(11 * time.Minute)

	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OAuth authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OAuth authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

func TestOAuthCallbackRefusesADomainNotOnTheAllowlist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "someone@not-allowed.example"
	domains := `["allowed.example"]`
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
		allowedDomains: &domains,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden {
		t.Fatalf("status=%d body=%v, want 403", status, body)
	}
	// D2745 P2-6 (team-lead, D2742 strict): escalated from the prior bare-
	// HTTPException/no-audit shape (still matching OIDC/SAML's own
	// identical, still-unaudited domain check -- unchanged there) to
	// D2742's third bucket: audited, fixed message + reason code, the
	// domain itself never in the client-visible response.
	detail, ok := body["detail"].(map[string]any)
	if !ok {
		t.Fatalf("detail=%v (%T), want a structured {message, reason} object", body["detail"], body["detail"])
	}
	if detail["message"] != "Email domain is not allowed for this provider" {
		t.Fatalf("detail.message=%v, want the fixed message (never the domain itself -- D2745 P2-6)", detail["message"])
	}
	if detail["reason"] != "oauth_domain_not_allowed" {
		t.Fatalf("detail.reason=%v, want oauth_domain_not_allowed", detail["reason"])
	}
	for _, key := range []string{"message", "reason"} {
		if strings.Contains(fmt.Sprint(detail[key]), "not-allowed.example") {
			t.Fatalf("detail.%s=%v leaks the denied domain", key, detail[key])
		}
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "domain_check")
	var errMsg *string
	if err := st.pool.QueryRow(ctx, `SELECT error_message FROM audit_logs WHERE org_id = $1 AND action = 'sso_login' AND status = 'failure'
ORDER BY created_at DESC LIMIT 1`, orgID).Scan(&errMsg); err != nil {
		t.Fatal(err)
	}
	if errMsg == nil || !strings.Contains(*errMsg, "not-allowed.example") {
		t.Fatalf("audit_logs.error_message = %v, want the actual domain recorded there (D2745 P2-6: audit row only)", errMsg)
	}
}

// TestOAuthCallbackRefusesAnUnknownUserWithoutAutoProvision pins D2742's
// third bucket (this file's package doc comment): the caller DID
// authenticate (a genuine state from a real /authorize call), so this is
// audited, unlike the state_auth-stage tests above -- but it still never
// mutates the provider row, since it reflects a per-user access decision,
// not IdP-provider health.
func TestOAuthCallbackRefusesAnUnknownUserWithoutAutoProvision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "nobody@example.test"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: false,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden || body["detail"] != "User not found and auto-provisioning is disabled" {
		t.Fatalf("status=%d body=%v, want 403 User not found and auto-provisioning is disabled", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "provisioning")
}

// TestOAuthCallbackRefusesAnUnverifiedEmail pins D2745's P1-1 ruling:
// GitLab's own account-confirmation signal (confirmed_at) absent means
// this package must refuse the login before ever looking up or creating
// a user by that email -- same bucket shape as auto-provisioning-disabled
// (authenticated, audited, no provider row mutation), its own stage tag.
func TestOAuthCallbackRefusesAnUnverifiedEmail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.confirmed = false
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden || body["detail"] != "No verified email address is available from this OAuth provider" {
		t.Fatalf("status=%d body=%v, want 403 No verified email address is available from this OAuth provider", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "email_verification")

	// Never linked or created: no user row exists at that email at all.
	var count int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = $1`, gitlab.email).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("users: got %d rows for an unverified email, want 0 -- never link or provision by it", count)
	}
}

// TestOAuthCallbackRefusesATokenExchangeFailure pins the other side of
// D2738 for OAuth: a genuinely-authenticated state whose later exchange
// fails DOES flip the provider row. D2742: the public detail is a fixed
// message + a stable reason code, never the upstream error text.
func TestOAuthCallbackRefusesATokenExchangeFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.tokenStatus = http.StatusBadRequest
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest {
		t.Fatalf("status=%d body=%v, want 400", status, body)
	}
	detail, ok := body["detail"].(map[string]any)
	if !ok {
		t.Fatalf("detail=%v (%T), want a structured {message, reason} object", body["detail"], body["detail"])
	}
	if detail["message"] != "OAuth authentication failed" {
		t.Fatalf("detail.message=%v, want the fixed message (never the upstream text -- D2742)", detail["message"])
	}
	if detail["reason"] != "oauth_token_exchange_failed" {
		t.Fatalf("detail.reason=%v, want oauth_token_exchange_failed", detail["reason"])
	}
	var lastError *string
	var providerStatus string
	if err := st.pool.QueryRow(ctx, `SELECT last_error, status FROM sso_providers WHERE id = $1`, providerID).
		Scan(&lastError, &providerStatus); err != nil {
		t.Fatal(err)
	}
	if lastError == nil {
		t.Fatal("last_error: want a recorded exchange-failure reason")
	}
	if providerStatus != "error" {
		t.Fatalf("provider status = %q, want %q: an authenticated-but-failed exchange must still flip it", providerStatus, "error")
	}
}

// TestOAuthCallbackRefusesAUserinfoFetchFailure pins the OTHER reason code
// (D2742): a failure at the userinfo-fetch stage (token exchange already
// succeeded) gets its own distinct, non-echoing reason.
func TestOAuthCallbackRefusesAUserinfoFetchFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.userStatus = http.StatusForbidden
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest {
		t.Fatalf("status=%d body=%v, want 400", status, body)
	}
	detail, ok := body["detail"].(map[string]any)
	if !ok {
		t.Fatalf("detail=%v (%T), want a structured {message, reason} object", body["detail"], body["detail"])
	}
	if detail["reason"] != "oauth_userinfo_fetch_failed" {
		t.Fatalf("detail.reason=%v, want oauth_userinfo_fetch_failed", detail["reason"])
	}
	var providerStatus string
	if err := st.pool.QueryRow(ctx, `SELECT status FROM sso_providers WHERE id = $1`, providerID).Scan(&providerStatus); err != nil {
		t.Fatal(err)
	}
	if providerStatus != "error" {
		t.Fatalf("provider status = %q, want %q", providerStatus, "error")
	}
}

func TestOAuthByTypePrefersTheDefaultProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	nonDefault := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig("https://a.example"), autoProvision: true,
	})
	defaultProvider := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig("https://b.example"), autoProvision: true, isDefault: true,
	})
	_ = nonDefault

	status, body := getJSON(t, st.server, "/api/v1/auth/oauth/gitlab/authorize?org_id="+orgID.String())
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, body)
	}
	authURL, _ := url.Parse(body["authorization_url"].(string))
	if !strings.HasPrefix(authURL.String(), "https://b.example") {
		t.Fatalf("authorization_url=%s, want the is_default provider (https://b.example), got provider tied to %v", authURL, defaultProvider)
	}
}

func TestOAuthByTypeFallsBackToAnyActiveProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig("https://only.example"), autoProvision: true,
	})

	status, body := getJSON(t, st.server, "/api/v1/auth/oauth/gitlab/authorize?org_id="+orgID.String())
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, body)
	}
	authURL, _ := url.Parse(body["authorization_url"].(string))
	if !strings.HasPrefix(authURL.String(), "https://only.example") {
		t.Fatalf("authorization_url=%s, want the sole active provider (https://only.example)", authURL)
	}
}

func TestOAuthByTypeRefusesAnInvalidProviderType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	status, body := getJSON(t, st.server, "/api/v1/auth/oauth/bitbucket/authorize?org_id="+orgID.String())
	if status != http.StatusBadRequest || body["detail"] != "Invalid OAuth provider type" {
		t.Fatalf("status=%d body=%v, want 400 Invalid OAuth provider type", status, body)
	}
}

func TestOAuthByType404sWhenNoneFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	status, body := getJSON(t, st.server, "/api/v1/auth/oauth/github/authorize?org_id="+orgID.String())
	if status != http.StatusNotFound {
		t.Fatalf("status=%d body=%v, want 404", status, body)
	}
}

// TestOAuthPairDispatch404sAnUnmatchedShape is D2743's precondition (2):
// the deploy-side collapse of the 4 two-segment oauth ingress paths
// (providers/{id} PATCH, {id}/authorize POST, {id}/callback POST,
// {type}/authorize GET) into one wildcard `/api/v1/auth/oauth/{a}/{b}`
// entry is only safe because go-api's own dispatcher (sso.go's oauthPair)
// refuses any OTHER two-segment shape on its own -- proving that here,
// not just asserting it by reading the code.
func TestOAuthPairDispatch404sAnUnmatchedShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	// No pairRoute's matches() checks a second segment of "frobnicate"
	// against any method, so this falls all the way through to
	// oauthPair's own h.Write(w, r, httpapi.CodeNotFound) (sso.go).
	status, _ := getJSON(t, st.server, "/api/v1/auth/oauth/some-id/frobnicate")
	if status != http.StatusNotFound {
		t.Fatalf("GET .../some-id/frobnicate: status=%d, want 404", status)
	}
	status, _ = postJSON(t, st.server, "/api/v1/auth/oauth/some-id/frobnicate", map[string]any{})
	if status != http.StatusNotFound {
		t.Fatalf("POST .../some-id/frobnicate: status=%d, want 404", status)
	}
}

// TestOAuthPairDispatch405sAWrongMethod is D2743's precondition (2), the
// other half: a path oauthPair DOES recognize, on a method none of its
// routes registered for that shape, gets a 405 (Allow header naming the
// real method), never silently a 200 or a 404 that could mask a real
// route existing.
func TestOAuthPairDispatch405sAWrongMethod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	// .../{provider_id}/callback only registers POST (sso.go's oauthPair).
	req, err := http.NewRequest(http.MethodGet, st.server.URL+"/api/v1/auth/oauth/some-id/callback", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := st.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET .../some-id/callback: status=%d, want 405", resp.StatusCode)
	}
	if resp.Header.Get("Allow") != http.MethodPost {
		t.Fatalf("Allow header = %q, want %q", resp.Header.Get("Allow"), http.MethodPost)
	}
}

func TestOAuthByTypeRefusesANonEntitledOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "team")
	seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig("https://x.example"), autoProvision: true, isDefault: true,
	})
	status, body := getJSON(t, st.server, "/api/v1/auth/oauth/gitlab/authorize?org_id="+orgID.String())
	if status != http.StatusPaymentRequired {
		t.Fatalf("status=%d body=%v, want 402", status, body)
	}
}

// TestOAuthConfigRefusesAnInsecureHTTPBaseURL pins D2745 P1-2/D2752: a
// non-loopback http:// base_url override is refused at config-load
// time -- before initiateOAuthAuth ever builds an authorization_url --
// unlike the loopback host every OTHER GitLab test in this file relies
// on (startFakeGitLab's own http://127.0.0.1 fake server), which
// validateOAuthConfigHTTPS accepts unconditionally by host, no switch.
func TestOAuthConfigRefusesAnInsecureHTTPBaseURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig("http://gitlab.example.internal"), autoProvision: true,
	})

	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusInternalServerError {
		t.Fatalf("authorize: status=%d body=%v, want 500 (an insecure, non-loopback base_url refused at config-load time)", status, body)
	}
	if _, ok := body["authorization_url"]; ok {
		t.Fatalf("authorize: got an authorization_url for a config this package should have refused: %v", body)
	}
}

// TestOAuthAuthorizationURLShapePerProvider covers GitHub and Google's
// request-building without a network round trip: github.com and
// accounts.google.com are hardcoded endpoints in both planes (this file's
// package doc comment), so there is no fake-server substitute for them.
func TestOAuthAuthorizationURLShapePerProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")

	cases := []struct {
		protocol, host   string
		wantScope        string
		wantGoogleParams bool
	}{
		{"oauth_github", "github.com", "read:user user:email", false},
		{"oauth_google", "accounts.google.com", "openid email profile", true},
	}
	for _, tc := range cases {
		config := fmt.Sprintf(`{"client_id":"%s-client","scopes":%s}`, tc.protocol, mustScopesJSON(tc.wantScope))
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: tc.protocol, status: "active", config: config, autoProvision: true,
		})
		status, resp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("%s: authorize status=%d body=%v", tc.protocol, status, resp)
		}
		authURL, err := url.Parse(resp["authorization_url"].(string))
		if err != nil {
			t.Fatalf("%s: invalid authorization_url: %v", tc.protocol, err)
		}
		if !strings.Contains(authURL.Host, tc.host) {
			t.Fatalf("%s: authorization_url host=%s, want %s", tc.protocol, authURL.Host, tc.host)
		}
		if got := authURL.Query().Get("scope"); got != tc.wantScope {
			t.Fatalf("%s: scope=%q, want %q", tc.protocol, got, tc.wantScope)
		}
		if tc.wantGoogleParams {
			if authURL.Query().Get("access_type") != "offline" || authURL.Query().Get("prompt") != "consent" {
				t.Fatalf("%s: google override params missing: %v", tc.protocol, authURL.Query())
			}
		}
	}
}

func mustScopesJSON(spaceJoined string) string {
	parts := strings.Split(spaceJoined, " ")
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}
