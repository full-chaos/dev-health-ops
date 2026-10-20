package social

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	testClientID     = "acp-client"
	testClientSecret = "social-test-only-not-a-credential"
	otherClientID    = "some-other-app"
)

// account is one provider account a fake token belongs to.
type account struct {
	provider Provider
	id       int64
	googleID string
	email    string
	// verified: GitHub = the address is verified (served from /user/emails,
	// not the public profile field); GitLab = confirmed_at set; Google =
	// verified_email.
	verified bool
	// public puts a GitHub email in the public profile field.
	public bool
	// clientID is the OAuth client the token was issued to.
	clientID string
	// profileOf serves this other token's profile from the user endpoint
	// while the token check reports this account.
	profileOf string
}

// fakeProviders serves GitHub, GitLab and Google from one httptest server.
type fakeProviders struct {
	t        *testing.T
	server   *httptest.Server
	mu       sync.Mutex
	accounts map[string]account
}

func newFakeProviders(t *testing.T) *fakeProviders {
	t.Helper()
	f := &fakeProviders{t: t, accounts: map[string]account{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /github/applications/{client}/token", f.githubCheck)
	mux.HandleFunc("GET /github/user", f.githubUser)
	mux.HandleFunc("GET /github/user/emails", f.githubEmails)
	mux.HandleFunc("GET /gitlab/oauth/token/info", f.gitlabInfo)
	mux.HandleFunc("GET /gitlab/api/v4/user", f.gitlabUser)
	mux.HandleFunc("POST /google/tokeninfo", f.googleInfo)
	mux.HandleFunc("GET /google/userinfo", f.googleUser)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// token registers an account and returns a token for it.
func (f *fakeProviders) token(name string, a account) contracts.SecretString {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a.clientID == "" {
		a.clientID = testClientID
	}
	f.accounts[name] = a
	return secretOf(name)
}

func (f *fakeProviders) client() *ProviderClient {
	creds := NewClientCredentials(testClientID, testClientSecret)
	return &ProviderClient{
		HTTP: f.server.Client(),
		Endpoints: Endpoints{
			GitHubAPI:       f.server.URL + "/github",
			GitLabBase:      f.server.URL + "/gitlab",
			GoogleUserInfo:  f.server.URL + "/google/userinfo",
			GoogleTokenInfo: f.server.URL + "/google/tokeninfo",
		},
		Clients: map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds},
	}
}

func (f *fakeProviders) lookup(token string, p Provider) (account, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.accounts[token]
	return a, ok && a.provider == p
}

// profile is the account whose profile a token's user endpoint serves.
func (f *fakeProviders) profile(token string, p Provider) (account, bool) {
	a, ok := f.lookup(token, p)
	if ok && a.profileOf != "" {
		return f.lookup(a.profileOf, p)
	}
	return a, ok
}

func bearerToken(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}

func reply(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakeProviders) githubCheck(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != r.PathValue("client") || pass != testClientSecret {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	a, found := f.lookup(body.AccessToken, GitHub)
	if !found || a.clientID != user {
		http.Error(w, "{}", http.StatusNotFound)
		return
	}
	reply(w, map[string]any{"app": map[string]any{"client_id": a.clientID}, "user": map[string]any{"id": a.id}})
}

func (f *fakeProviders) githubUser(w http.ResponseWriter, r *http.Request) {
	a, ok := f.profile(bearerToken(r), GitHub)
	if !ok {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	body := map[string]any{"id": a.id, "login": "user" + url.PathEscape(a.email), "name": "Test User", "email": nil}
	if a.public {
		body["email"] = a.email
	}
	reply(w, body)
}

func (f *fakeProviders) githubEmails(w http.ResponseWriter, r *http.Request) {
	a, ok := f.profile(bearerToken(r), GitHub)
	if !ok {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	reply(w, []map[string]any{{"email": a.email, "primary": true, "verified": a.verified}})
}

func (f *fakeProviders) gitlabInfo(w http.ResponseWriter, r *http.Request) {
	a, ok := f.lookup(bearerToken(r), GitLab)
	if !ok {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	reply(w, map[string]any{"resource_owner_id": a.id, "application": map[string]any{"uid": a.clientID}})
}

func (f *fakeProviders) gitlabUser(w http.ResponseWriter, r *http.Request) {
	a, ok := f.profile(bearerToken(r), GitLab)
	if !ok {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	var confirmed any
	if a.verified {
		confirmed = "2026-01-01T00:00:00Z"
	}
	reply(w, map[string]any{"id": a.id, "username": "u", "name": "Test User", "email": a.email, "confirmed_at": confirmed})
}

func (f *fakeProviders) googleInfo(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "{}", http.StatusBadRequest)
		return
	}
	a, ok := f.lookup(r.PostForm.Get("access_token"), Google)
	if !ok {
		http.Error(w, "{}", http.StatusBadRequest)
		return
	}
	reply(w, map[string]any{"issued_to": a.clientID, "audience": a.clientID, "user_id": a.googleID})
}

func (f *fakeProviders) googleUser(w http.ResponseWriter, r *http.Request) {
	a, ok := f.profile(bearerToken(r), Google)
	if !ok {
		http.Error(w, "{}", http.StatusUnauthorized)
		return
	}
	reply(w, map[string]any{"id": a.googleID, "email": a.email, "verified_email": a.verified, "name": "Test User"})
}
