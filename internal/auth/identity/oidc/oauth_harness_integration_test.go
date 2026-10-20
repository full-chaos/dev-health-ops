//go:build integration

package oidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
)

// fakeAccount is one account at a fake OAuth provider.
type fakeAccount struct {
	ID       string // numeric for GitHub and GitLab, any text for Google
	Email    string
	Verified bool
}

// fakeGrant is an authorization code the fake will exchange once the right
// client and, for PKCE, the right verifier present it.
type fakeGrant struct {
	client    string
	challenge string // "" = the authorize request carried no PKCE challenge
	account   fakeAccount
}

// fakeOAuth plays GitHub (/gh), GitLab (/gitlab and any base under it) and
// Google (/google) for the endpoints the OAuth flow and the social client use.
type fakeOAuth struct {
	mu       sync.Mutex
	secrets  map[string]string // client id -> secret
	grants   map[string]fakeGrant
	tokens   map[string]fakeGrant // access token -> grant
	hits     int
	verifier []string // code_verifier values the token endpoints received
	// tokenClient overrides the client a token is reported as issued to.
	tokenClient string
}

func newFakeOAuth() *fakeOAuth {
	return &fakeOAuth{secrets: map[string]string{}, grants: map[string]fakeGrant{}, tokens: map[string]fakeGrant{}}
}

func (f *fakeOAuth) endpoints(base string) *OAuthEndpoints {
	return &OAuthEndpoints{
		GitHubAuthorize: base + "/gh/authorize", GitHubToken: base + "/gh/token",
		GoogleAuthorize: base + "/google/authorize", GoogleToken: base + "/google/token",
		Social: social.Endpoints{GitHubAPI: base + "/gh/api", GitLabBase: base + "/gitlab",
			GoogleUserInfo: base + "/google/userinfo", GoogleTokenInfo: base + "/google/tokeninfo"},
	}
}

func (f *fakeOAuth) routes(mux *http.ServeMux) {
	mux.HandleFunc("/gh/token", f.serveToken)
	mux.HandleFunc("/google/token", f.serveToken)
	mux.HandleFunc("/gitlab/", f.serveGitLab)
	mux.HandleFunc("/gh/api/", f.serveGitHubAPI)
	mux.HandleFunc("/google/tokeninfo", f.serveGoogleTokenInfo)
	mux.HandleFunc("/google/userinfo", f.serveGoogleUserinfo)
}

func (f *fakeOAuth) grant(code string, g fakeGrant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grants[code] = g
}

func writeFakeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeOAuth) serveToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeFakeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_request"})
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	verifier := r.PostForm.Get("code_verifier")
	f.verifier = append(f.verifier, verifier)
	g, ok := f.grants[r.PostForm.Get("code")]
	delete(f.grants, r.PostForm.Get("code"))
	client := r.PostForm.Get("client_id")
	ok = ok && client == g.client && r.PostForm.Get("client_secret") == f.secrets[client]
	if g.challenge != "" {
		sum := sha256.Sum256([]byte(verifier))
		ok = ok && base64.RawURLEncoding.EncodeToString(sum[:]) == g.challenge
	} else {
		ok = ok && verifier == ""
	}
	if !ok {
		writeFakeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant"})
		return
	}
	token := "tok-" + uuid.NewString()
	f.tokens[token] = g
	writeFakeJSON(w, http.StatusOK, map[string]any{"access_token": token, "token_type": "bearer"})
}

func (f *fakeOAuth) bearer(r *http.Request) (fakeGrant, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	g, ok := f.tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	return g, ok
}

func (f *fakeOAuth) issuedTo(g fakeGrant) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tokenClient != "" {
		return f.tokenClient
	}
	return g.client
}

func (f *fakeOAuth) serveGitLab(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/oauth/token") {
		f.serveToken(w, r)
		return
	}
	g, ok := f.bearer(r)
	if !ok {
		writeFakeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid_token"})
		return
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/oauth/token/info"):
		writeFakeJSON(w, http.StatusOK, map[string]any{"resource_owner_id": json.Number(g.account.ID),
			"application": map[string]any{"uid": f.issuedTo(g)}})
	case strings.HasSuffix(r.URL.Path, "/api/v4/user"):
		var confirmed any
		if g.account.Verified {
			confirmed = "2026-01-01T00:00:00Z"
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{"id": json.Number(g.account.ID), "username": "u" + g.account.ID,
			"name": "Fake User", "email": g.account.Email, "confirmed_at": confirmed})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeOAuth) serveGitHubAPI(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/gh/api/applications/") && strings.HasSuffix(r.URL.Path, "/token") {
		var body struct {
			AccessToken string `json:"access_token"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		client, secret, _ := r.BasicAuth()
		f.mu.Lock()
		f.hits++
		g, ok := f.tokens[body.AccessToken]
		good := ok && f.secrets[client] == secret
		f.mu.Unlock()
		if !good {
			writeFakeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{"app": map[string]any{"client_id": f.issuedTo(g)},
			"user": map[string]any{"id": json.Number(g.account.ID)}})
		return
	}
	g, ok := f.bearer(r)
	if !ok {
		writeFakeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Bad credentials"})
		return
	}
	switch r.URL.Path {
	case "/gh/api/user":
		writeFakeJSON(w, http.StatusOK, map[string]any{"id": json.Number(g.account.ID), "login": "u" + g.account.ID,
			"name": "Fake User", "email": nil})
	case "/gh/api/user/emails":
		writeFakeJSON(w, http.StatusOK, []map[string]any{{"email": g.account.Email, "primary": true, "verified": g.account.Verified}})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeOAuth) serveGoogleTokenInfo(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	f.hits++
	g, ok := f.tokens[r.PostForm.Get("access_token")]
	f.mu.Unlock()
	if !ok {
		writeFakeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_token"})
		return
	}
	writeFakeJSON(w, http.StatusOK, map[string]any{"issued_to": f.issuedTo(g), "audience": f.issuedTo(g), "user_id": g.account.ID})
}

func (f *fakeOAuth) serveGoogleUserinfo(w http.ResponseWriter, r *http.Request) {
	g, ok := f.bearer(r)
	if !ok {
		writeFakeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid_token"})
		return
	}
	writeFakeJSON(w, http.StatusOK, map[string]any{"id": g.account.ID, "email": g.account.Email,
		"verified_email": g.account.Verified, "name": "Fake User"})
}

// seedOAuthProvider seeds an active OAuth row of protocol ("oauth_gitlab"...)
// with its own client, registered at the fake with its secret.
func (h *harness) seedOAuthProvider(t *testing.T, orgID uuid.UUID, protocol string, extra map[string]any) (uuid.UUID, string) {
	t.Helper()
	id, client, secret := uuid.New(), "client-"+uuid.NewString(), uuid.NewString()
	cfg := map[string]any{"client_id": client, "scopes": []string{"read_user"}}
	for k, v := range extra {
		cfg[k] = v
	}
	raw, _ := json.Marshal(cfg)
	if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.sso_providers
	(id, org_id, name, protocol, status, auto_provision_users, config, encrypted_secrets)
VALUES ($1, $2, $3, $4, 'active', true, $5::json, json_build_object('client_secret', $6::text))`,
		id, orgID, "oauth idp "+id.String(), protocol, string(raw), secret); err != nil {
		t.Fatalf("seed oauth provider: %v", err)
	}
	h.idp.oauth.mu.Lock()
	h.idp.oauth.secrets[client] = secret
	h.idp.oauth.mu.Unlock()
	return id, client
}

type oauthStarted struct {
	state, challenge string
	query            url.Values
	cookie           *http.Cookie
}

func (h *harness) startOAuth(t *testing.T, providerID uuid.UUID) oauthStarted {
	t.Helper()
	result, err := h.svc.AuthorizeOAuth(context.Background(), providerID)
	if err != nil {
		t.Fatalf("AuthorizeOAuth: %v", err)
	}
	u, err := url.Parse(result.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("state") != result.State {
		t.Fatalf("authorization url %s", result.AuthorizationURL)
	}
	return oauthStarted{state: result.State, challenge: q.Get("code_challenge"), query: q, cookie: result.Cookie}
}

// finishOAuth grants a fresh code for account at the fake, bound to the
// flow's challenge and client, and runs the callback with the flow's cookie.
func (h *harness) finishOAuth(t *testing.T, providerID uuid.UUID, client string, s oauthStarted, account fakeAccount, withCookie bool) (Login, *Error) {
	t.Helper()
	code := "code-" + uuid.NewString()
	h.idp.oauth.grant(code, fakeGrant{client: client, challenge: s.challenge, account: account})
	login, clear, err := h.svc.CallbackOAuth(context.Background(), providerID, CallbackInput{
		Code: code, State: s.state, RequestID: &h.requestID,
		Cookie: func(name string) (string, bool) {
			if withCookie && s.cookie != nil && name == s.cookie.Name {
				return s.cookie.Value, true
			}
			return "", false
		},
	})
	if err == nil {
		if clear == nil || clear.MaxAge >= 0 {
			t.Fatalf("success did not clear the login-nonce cookie: %+v", clear)
		}
		return login, nil
	}
	failure, ok := err.(*Error)
	if !ok {
		t.Fatalf("CallbackOAuth returned %T %v", err, err)
	}
	return Login{}, failure
}

func fakeAccountWith(email string, verified bool) fakeAccount {
	return fakeAccount{ID: strconv.FormatInt(rand.Int64N(1<<40)+1000, 10), Email: email, Verified: verified}
}
