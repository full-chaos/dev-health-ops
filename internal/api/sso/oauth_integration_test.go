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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeGitLab is a minimal fake self-hosted GitLab: a token endpoint that
// exchanges any code for a fixed access token (recording what it
// received, for the client_secret round-trip assertion), and a
// /api/v4/user endpoint returning whatever profile the test set.
type fakeGitLab struct {
	server *httptest.Server

	email, username, fullName, avatarURL, userID string
	tokenStatus, userStatus                      int // 0 means 200.

	receivedClientID, receivedClientSecret, receivedRedirectURI string
}

func startFakeGitLab(t *testing.T) *fakeGitLab {
	t.Helper()
	g := &fakeGitLab{email: "gitlab.user@example.test", username: "gluser", fullName: "GitLab User", userID: "42"}
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": g.userID, "email": g.email, "username": g.username, "name": g.fullName, "avatar_url": g.avatarURL,
	})
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
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

	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	state, _ := authResp["state"].(string)
	tampered := flipMiddleByte(state)

	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": tampered})
	if status != http.StatusBadRequest || body["detail"] != "OAuth authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OAuth authentication failed", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "state_auth")
}

// flipMiddleByte tampers a base64url-encoded AEAD token reliably: it
// decodes to raw bytes, flips every bit of the MIDDLE byte, and
// re-encodes. Unlike flipping the string's LAST base64 character (as
// oidc_integration_test.go's flipLastRune does), which can land on a
// padding-only bit range of the final character and silently decode to
// the SAME underlying bytes depending on the payload's length modulo 3 --
// observed here to intermittently pass a "tampered" state's GCM tag check
// roughly half the time -- a middle byte is never a padding position, so
// this always changes the ciphertext GCM authenticates over.
func flipMiddleByte(s string) string {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return s + "x"
	}
	raw[len(raw)/2] ^= 0xFF
	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestOAuthCallbackRefusesAnExpiredState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})

	st.clock.freeze(time.Now())
	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	// oauthStateTTL (oauthstate.go) is 10 minutes.
	st.clock.advance(11 * time.Minute)

	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden {
		t.Fatalf("status=%d body=%v, want 403", status, body)
	}
	// router.py's domain check is a bare HTTPException, outside the
	// try/except OAuthProviderError block, matching OIDC/SAML's identical
	// shape: no audit row, no provider mutation.
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	var auditCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'sso_login'`, orgID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("audit_logs: got %d sso_login rows, want 0 (a bare HTTPException, no audit call)", auditCount)
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden || body["detail"] != "User not found and auto-provisioning is disabled" {
		t.Fatalf("status=%d body=%v, want 403 User not found and auto-provisioning is disabled", status, body)
	}
	assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
	assertSSOAuditStage(t, ctx, st.pool, orgID, "provisioning")
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
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

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
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
