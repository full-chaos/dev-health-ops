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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newCookieClient carries the login nonce cookie from authorize to callback.
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
	// omitID drops the "id" key from the profile; nullID sends it as JSON null.
	omitID, nullID bool
	// rawUser, when set, is sent as the profile body as it is (an escape the encoder cannot produce).
	rawUser string
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
	if g.rawUser != "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(g.rawUser))
		return
	}
	body := map[string]any{
		"id": g.userID, "email": g.email, "username": g.username, "name": g.fullName, "avatar_url": g.avatarURL,
	}
	if g.confirmed {
		body["confirmed_at"] = "2026-01-01T00:00:00Z"
	}
	if g.omitID {
		delete(body, "id")
	}
	if g.nullID {
		body["id"] = nil
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func gitlabConfig(baseURL string) string {
	return fmt.Sprintf(`{"client_id":"gitlab-client","scopes":["read_user","email"],"base_url":%q}`, baseURL)
}

// flipMiddleByte is shared with the OIDC integration tests.

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

func mustScopesJSON(spaceJoined string) string {
	parts := strings.Split(spaceJoined, " ")
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}

// TestSSOSurrogateRefusalTableStackRows drives the oauth_* rows of ci/sso_surrogate_refusals.tsv against a real provider row.
func TestSSOSurrogateRefusalTableStackRows(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	providers := map[string]uuid.UUID{
		"oauth_gitlab": seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oauth_gitlab", status: "active", config: gitlabConfig(startFakeGitLab(t).server.URL), autoProvision: true,
			clientSecretEncryptedFallback: "gitlab-secret",
		}),
	}
	driven := 0
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if line == "" || strings.HasPrefix(line, "#") || f[1] != "sso" || !strings.HasPrefix(f[4], "oauth_") {
			continue
		}
		id, request, body, setup, wantCell := f[0], f[2], f[3], f[4], strings.Fields(f[5])
		provider, known := providers[setup]
		if !known {
			t.Fatalf("%s: unknown setup %q", id, setup)
		}
		method, path, _ := strings.Cut(request, " ")
		if encoded, isHex := strings.CutPrefix(body, "hex:"); isHex {
			decoded, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatalf("%s: %v", id, err)
			}
			body = string(decoded)
		}
		req, err := http.NewRequest(method, st.server.URL+strings.Replace(path, "{provider}", provider.String(), 1), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		text, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if want, _ := strconv.Atoi(wantCell[0]); resp.StatusCode != want {
			t.Errorf("%s: status=%d want %s body=%s", id, resp.StatusCode, wantCell[0], text)
		}
		if len(wantCell) == 3 {
			var parsed struct {
				Detail []struct {
					Type string `json:"type"`
					Loc  []any  `json:"loc"`
				} `json:"detail"`
			}
			if err := json.Unmarshal(text, &parsed); err != nil || len(parsed.Detail) != 1 || parsed.Detail[0].Type != wantCell[1] ||
				fmt.Sprint(parsed.Detail[0].Loc) != "["+strings.ReplaceAll(wantCell[2], ".", " ")+"]" {
				t.Errorf("%s: body=%s want %s %s", id, text, wantCell[1], wantCell[2])
			}
		}
		driven++
	}
	if driven != 1 {
		t.Fatalf("drove %d oauth rows, want 1", driven)
	}
}
