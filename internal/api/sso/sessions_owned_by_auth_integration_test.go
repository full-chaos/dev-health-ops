//go:build integration

package sso_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

func ssoIssueClient(t *testing.T, base string) *sessionissue.Client {
	t.Helper()
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	return workloadIssueClient(t, parsed)
}

// oidcLogin runs authorize and callback for a new user and returns the
// callback's status and body.
func oidcLogin(t *testing.T, ctx context.Context, st stack, email string) (int, map[string]any) {
	t.Helper()
	idp := startFakeIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
		idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oidc", status: "active", config: config, autoProvision: true,
		clientSecretEncryptedFallback: "test-client-secret",
	})
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: status=%d body=%v", status, authResp)
	}
	authURL, _ := authResp["authorization_url"].(string)
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	idp.set(parsed.Query().Get("nonce"), email, "Issued User", "idp-subject-"+email)
	return postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "test-auth-code", "state": authResp["state"]})
}

func ssoRefreshRows(t *testing.T, ctx context.Context, st stack) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSSOLoginTakesItsPairFromTheIssueRouteWhenAuthOwnsSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var mu sync.Mutex
	var bodies []map[string]any
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		_, _ = io.WriteString(w, `{"access_token":"issued.by.auth","refresh_token":"dhr1_issued_by_auth","token_type":"bearer","expires_in":600}`)
	}))
	defer auth.Close()
	st := startStackWith(t, ctx, ssoIssueClient(t, auth.URL))

	status, body := oidcLogin(t, ctx, st, "issued.user@allowed.example")
	if status != http.StatusOK || body["access_token"] != "issued.by.auth" || body["refresh_token"] != "dhr1_issued_by_auth" || body["expires_in"] != float64(600) {
		t.Fatalf("callback: %d %v, want the issued pair", status, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("%d issue calls, want 1", len(bodies))
	}
	if amr, _ := bodies[0]["amr"].([]any); len(amr) != 1 || amr[0] != "fed" {
		t.Fatalf("amr %v, want fed", bodies[0]["amr"])
	}
	if n := ssoRefreshRows(t, ctx, st); n != 0 {
		t.Fatalf("%d refresh_tokens rows; go-api must write none", n)
	}
}

func TestSSOLoginAnswers503WhenTheIssueRouteIsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "http://" + closed.Addr().String()
	_ = closed.Close()
	st := startStackWith(t, ctx, ssoIssueClient(t, down))

	status, body := oidcLogin(t, ctx, st, "down.user@allowed.example")
	raw, _ := json.Marshal(body)
	if status != http.StatusServiceUnavailable || !strings.Contains(string(raw), sessionissue.UnavailableText) || body["access_token"] != nil {
		t.Fatalf("callback: %d %s, want 503 %q and no token", status, raw, sessionissue.UnavailableText)
	}
	if n := ssoRefreshRows(t, ctx, st); n != 0 {
		t.Fatalf("%d refresh_tokens rows with the issue route down", n)
	}
}

// samlLogin posts a signed assertion for a new user to the ACS route and
// returns the status and body.
func samlLogin(t *testing.T, ctx context.Context, st stack, email string) (int, map[string]any) {
	t.Helper()
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	idp := newSAMLIdP(t)
	const (
		spEntityID = "https://sp.test/saml/metadata"
		acsURL     = "https://sp.test/api/v1/auth/saml/{provider_id}/acs"
	)
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		idp.EntityID(), idp.certConfigValue(), spEntityID, acsURL)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})
	response := idp.signedSAMLResponse(t, samlResponseOpts{spEntityID: spEntityID, acsURL: samlACSOverrideFor(providerID), email: email, fullName: "SAML User", nameID: email})
	return postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
}

func TestSAMLLoginTakesItsPairFromTheIssueRouteWhenAuthOwnsSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var mu sync.Mutex
	var calls int
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		_, _ = io.WriteString(w, `{"access_token":"issued.by.auth","refresh_token":"dhr1_issued_by_auth","token_type":"bearer","expires_in":600}`)
	}))
	defer auth.Close()
	st := startStackWith(t, ctx, ssoIssueClient(t, auth.URL))
	status, body := samlLogin(t, ctx, st, "saml.issued@allowed.example")
	if status != http.StatusOK || body["access_token"] != "issued.by.auth" || body["refresh_token"] != "dhr1_issued_by_auth" || body["expires_in"] != float64(600) {
		t.Fatalf("acs: %d %v, want the issued pair", status, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("%d issue calls, want 1", calls)
	}
	if n := ssoRefreshRows(t, ctx, st); n != 0 {
		t.Fatalf("%d refresh_tokens rows; go-api must write none", n)
	}
}

func TestSAMLLoginAnswers503WhenTheIssueRouteIsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "http://" + closed.Addr().String()
	_ = closed.Close()
	st := startStackWith(t, ctx, ssoIssueClient(t, down))
	status, body := samlLogin(t, ctx, st, "saml.down@allowed.example")
	raw, _ := json.Marshal(body)
	if status != http.StatusServiceUnavailable || !strings.Contains(string(raw), sessionissue.UnavailableText) || body["access_token"] != nil {
		t.Fatalf("acs: %d %s, want 503 %q and no token", status, raw, sessionissue.UnavailableText)
	}
	if n := ssoRefreshRows(t, ctx, st); n != 0 {
		t.Fatalf("%d refresh_tokens rows with the issue route down", n)
	}
}

// workloadIssueClient is go-api's session issue client presenting a workload token: its exchange (a local server)
// answers a claims-valid token, which the fake issue route takes as any bearer. Kept local (no shared test API); it
// folds into internal/testsupport/workloadclient once CHAOS-12782 lands.
func workloadIssueClient(t *testing.T, base *url.URL) *sessionissue.Client {
	t.Helper()
	const issuer = "https://auth.example.test"
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		issuer, 7, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	minted := input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": minted, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
	}))
	t.Cleanup(exchange.Close)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+enc.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	workload, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: issuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := sessionissue.NewWorkload(base, workload, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
