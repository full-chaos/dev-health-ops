//go:build integration

// Real-producer coverage for CHAOS-6658 (initiateOIDCAuth / oidcCallback).
//
// AGENTS.md requires a differential oracle for cross-implementation work,
// but there is no live Python producer to diff the substantive OIDC
// exchange against: process_oidc_callback is structurally unreachable as
// shipped (oidc.go's doc comment, delta 2), so oracle-testing this fix
// against the thing it fixes would prove nothing, not honestly satisfy the
// rule. What Python and Go still agree on -- that the other 15 routes,
// and these two without Cipher/Signer wired, answer a 402 shim -- stays
// pinned by the existing sso_venue_oracle_integration_test.go.
//
// This file instead drives the full round trip against a self-signed test
// IdP fixture (a real RSA-signed JWKS/token/userinfo triple, per the
// lead's STEP-0 answer), against a real containerized Postgres and the
// real handler chain (policy.Guard, http.ServeMux, sso.Routes), asserting
// the state the system exists to reach (a minted token pair, a real users
// row, a real audit_logs row) rather than that the code ran.
package sso_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sso"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const testJWTKey = "oidc-6658-test-" + "0123456789abcdef0123456789abcdef"

// fakeIdP is a minimal, real (RSA-signed) OIDC provider fixture: a JWKS
// endpoint, a token endpoint that mints an id_token embedding whatever
// nonce the test tells it to for the next exchange, and a userinfo
// endpoint. One instance is shared by every subtest that needs a live
// exchange; nextNonce/nextnEmail are set immediately before each callback.
type fakeIdP struct {
	key      *rsa.PrivateKey
	server   *httptest.Server
	mu       sync.Mutex
	nonce    string
	email    string
	fullName string
	sub      string
	// receivedVerifier is the code_verifier the token endpoint actually
	// received on the last exchange, for the PKCE round-trip assertion.
	receivedVerifier string
	// receivedSecret is the client_secret actually received, for the
	// decrypt-and-forward assertion.
	receivedSecret string
}

func startFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIdP{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", idp.serveJWKS)
	mux.HandleFunc("/token", idp.serveToken)
	mux.HandleFunc("/userinfo", idp.serveUserinfo)
	idp.server = httptest.NewServer(mux)
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *fakeIdP) set(nonce, email, fullName, sub string) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	idp.nonce, idp.email, idp.fullName, idp.sub = nonce, email, fullName, sub
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (idp *fakeIdP) serveJWKS(w http.ResponseWriter, r *http.Request) {
	pub := idp.key.PublicKey
	jwk := map[string]any{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "test-key-1",
		"n": b64url(pub.N.Bytes()),
		"e": b64url(bigIntToBytes(pub.E)),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{jwk}})
}

func bigIntToBytes(e int) []byte {
	// Standard RSA public exponent 65537 = 0x010001.
	if e == 65537 {
		return []byte{0x01, 0x00, 0x01}
	}
	buf := []byte{byte(e >> 16), byte(e >> 8), byte(e)}
	return buf
}

func (idp *fakeIdP) serveToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idp.mu.Lock()
	idp.receivedVerifier = r.Form.Get("code_verifier")
	idp.receivedSecret = r.Form.Get("client_secret")
	nonce, email, fullName, sub := idp.nonce, idp.email, idp.fullName, idp.sub
	idp.mu.Unlock()

	now := time.Now()
	claims := jwt.MapClaims{
		"iss": idp.server.URL, "aud": r.Form.Get("client_id"), "sub": sub,
		"exp": now.Add(5 * time.Minute).Unix(), "iat": now.Unix(), "nonce": nonce,
		"email": email, "name": fullName,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-1"
	idToken, err := token.SignedString(idp.key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": "test-access-token", "id_token": idToken, "token_type": "Bearer", "expires_in": 300,
	})
}

func (idp *fakeIdP) serveUserinfo(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer test-access-token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	idp.mu.Lock()
	email, fullName, sub := idp.email, idp.fullName, idp.sub
	idp.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"email": email, "name": fullName, "sub": sub})
}

// testClock is a settable clock: real time until a test freezes/advances
// it, so an expiry test does not sleep 10 minutes.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.t.IsZero() {
		return time.Now()
	}
	return c.t
}

func (c *testClock) freeze(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.t.IsZero() {
		c.t = time.Now()
	}
	c.t = c.t.Add(d)
}

// stack is the real handler chain over a real, migrated Postgres.
type stack struct {
	pool   *pgxpool.Pool
	server *httptest.Server
	clock  *testClock
}

func startStack(t *testing.T, ctx context.Context) stack {
	t.Helper()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	verifier, err := edgetoken.New(testJWTKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(testJWTKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, logger)

	clock := &testClock{}
	routes := sso.Routes(sso.Deps{
		Pool: pool, Guard: guard, Logger: logger, Now: clock.now,
		// No Cipher: decryptProviderSecret's own legacy-plaintext fallback
		// (oidc.go) covers a test provider's un-encrypted stored
		// client_secret without one. StateSecret is the production
		// wiring's actual secret (deps.GitHubStateSigner.Secret, itself
		// cfg.APIJWTSecret.Reveal()): reusing testJWTKey here mirrors that
		// exactly, rather than a second, differently-named test constant.
		StateSecret: testJWTKey, Signer: signer,
		// The production default (defaultOIDCClient, oidc.go) dials over
		// externalurl.GuardedTransport(), which -- correctly -- refuses a
		// loopback address, exactly what every httptest.Server in this
		// file binds to. That guard is externalurl's own, independently
		// tested; this override exists only so the fixture IdP is
		// reachable, not to weaken what a real deployment dials.
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	})
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return stack{pool: pool, server: server, clock: clock}
}

// seedOrg inserts one organization at the given tier ("enterprise" is
// entitled to sso_saml; "team" and "community" are not) and returns its id.
func seedOrg(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tier string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	slug := "org-" + id.String()[:8]
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, '{}', now(), now())`, id, slug, tier); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	return id
}

type providerOpts struct {
	protocol, status, config      string
	allowedDomains                *string
	autoProvision                 bool
	clientSecretEncryptedFallback string // stored as-is (no cipher configured in this test), simulating legacy plaintext.
}

func seedProvider(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, opts providerOpts) uuid.UUID {
	t.Helper()
	id := uuid.New()
	secrets := "null"
	if opts.clientSecretEncryptedFallback != "" {
		text, err := json.Marshal(map[string]string{"client_secret": opts.clientSecretEncryptedFallback})
		if err != nil {
			t.Fatal(err)
		}
		secrets = string(text)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sso_providers
	(id, org_id, name, protocol, status, is_default, allow_idp_initiated, auto_provision_users, default_role,
	 config, encrypted_secrets, allowed_domains, created_at, updated_at)
VALUES ($1, $2, $9, $3, $4, false, true, $5, 'member', $6::json, $7::json, $8::json, now(), now())`,
		id, orgID, opts.protocol, opts.status, opts.autoProvision, opts.config, secrets, opts.allowedDomains,
		"Test Provider "+id.String()[:8]); err != nil {
		t.Fatalf("seed provider: %v", err)
	}
	return id
}

func postJSON(t *testing.T, server *httptest.Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(server.URL+path, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestOIDCAuthorizeAndCallbackFullRoundTrip is the happy path: an
// enterprise-entitled org, an active OIDC provider pointed at the fake
// IdP, PKCE on by default, auto-provisioning a brand-new user.
func TestOIDCAuthorizeAndCallbackFullRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := startFakeIdP(t)

	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
		idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oidc", status: "active", config: config, autoProvision: true,
		clientSecretEncryptedFallback: "test-client-secret",
	})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: status=%d body=%v", status, authResp)
	}
	authURL, _ := authResp["authorization_url"].(string)
	state, _ := authResp["state"].(string)
	if authURL == "" || state == "" {
		t.Fatalf("authorize: missing fields: %v", authResp)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	nonce := query.Get("nonce")
	codeChallenge := query.Get("code_challenge")
	if nonce == "" || codeChallenge == "" || query.Get("code_challenge_method") != "S256" {
		t.Fatalf("authorize: expected nonce + S256 PKCE params, got %v", query)
	}

	const testEmail = "new.user@allowed.example"
	idp.set(nonce, testEmail, "New User", "idp-subject-1")

	status, cbResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "test-auth-code", "state": state})
	if status != http.StatusOK {
		t.Fatalf("callback: status=%d body=%v", status, cbResp)
	}
	if cbResp["access_token"] == "" || cbResp["refresh_token"] == "" {
		t.Fatalf("callback: expected a token pair, got %v", cbResp)
	}
	if cbResp["email"] != testEmail {
		t.Fatalf("callback: email=%v, want %s", cbResp["email"], testEmail)
	}

	// The PKCE round trip actually ran: the verifier the callback sent to
	// the token endpoint hashes to the challenge the authorize step gave
	// the caller.
	idp.mu.Lock()
	verifier, secret := idp.receivedVerifier, idp.receivedSecret
	idp.mu.Unlock()
	sum := sha256.Sum256([]byte(verifier))
	if got := base64.RawURLEncoding.EncodeToString(sum[:]); got != codeChallenge {
		t.Fatalf("PKCE: code_verifier %q hashes to %q, want challenge %q", verifier, got, codeChallenge)
	}
	if secret != "test-client-secret" {
		t.Fatalf("client_secret forwarded to token endpoint = %q, want the stored (unencrypted-in-this-test) secret", secret)
	}

	// The state the system exists to reach: a real users row, provisioned
	// from the id_token/userinfo-merged claims.
	var email string
	var fullName *string
	if err := st.pool.QueryRow(ctx, `SELECT email, full_name FROM users WHERE email = $1`, testEmail).Scan(&email, &fullName); err != nil {
		t.Fatalf("provisioned user: %v", err)
	}

	var lastLogin any
	if err := st.pool.QueryRow(ctx, `SELECT last_login_at FROM sso_providers WHERE id = $1`, providerID).Scan(&lastLogin); err != nil {
		t.Fatalf("provider last_login_at: %v", err)
	}
	if lastLogin == nil {
		t.Fatal("record_login: sso_providers.last_login_at was not set")
	}

	var auditCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND action = 'sso_login' AND status = 'success'`,
		orgID).Scan(&auditCount); err != nil {
		t.Fatalf("audit query: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("audit_logs: got %d success sso_login rows, want 1", auditCount)
	}
}

// TestOIDCAuthorizeRefusesANonEntitledOrg pins D2725: an org whose tier
// grants neither the org entitlement nor (community process tier) the
// process fallback gets the existing 402 shape, never reaching the
// provider's protocol/status checks.
func TestOIDCAuthorizeRefusesANonEntitledOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "team")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: "{}", autoProvision: true})

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusPaymentRequired {
		t.Fatalf("status=%d body=%v, want 402", status, body)
	}
	detail, _ := body["detail"].(map[string]any)
	if detail["error"] != "feature_not_licensed" || detail["feature"] != "sso_saml" {
		t.Fatalf("detail=%v", detail)
	}
}

// TestOIDCAuthorizeUnknownProvider404sRegardlessOfEntitlement pins delta 1
// (oidc.go's doc comment): the 404 check runs before the entitlement gate
// can even be evaluated, since there is no org id without a provider row.
func TestOIDCAuthorizeUnknownProvider404sRegardlessOfEntitlement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+uuid.NewString()+"/authorize", map[string]any{})
	if status != http.StatusNotFound {
		t.Fatalf("status=%d body=%v, want 404", status, body)
	}
}

func TestOIDCAuthorizeRefusesWrongProtocolAndInactiveStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")

	samlID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: "{}", autoProvision: true})
	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+samlID.String()+"/authorize", map[string]any{})
	if status != http.StatusBadRequest || body["detail"] != "Provider is not OIDC" {
		t.Fatalf("saml provider on oidc route: status=%d body=%v", status, body)
	}

	inactiveID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "inactive", config: "{}", autoProvision: true})
	status, body = postJSON(t, st.server, "/api/v1/auth/oidc/"+inactiveID.String()+"/authorize", map[string]any{})
	if status != http.StatusBadRequest || body["detail"] != "SSO provider is not active" {
		t.Fatalf("inactive provider: status=%d body=%v", status, body)
	}
}

// TestOIDCCallbackRefusesADomainNotOnTheAllowlist pins the 403 branch.
// Unlike the exchange and provisioning failures, Python's router raises
// this HTTPException as a bare, unwrapped 403 -- outside the
// try/except SSOProcessingError block and its record_error/audit call --
// so no provider error or audit row is written here, matching Python.
func TestOIDCCallbackRefusesADomainNotOnTheAllowlist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := startFakeIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"c","issuer":%q,"token_endpoint":%q,"jwks_uri":%q}`, idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks")
	domains := `["allowed.example"]`
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oidc", status: "active", config: config, autoProvision: true, allowedDomains: &domains,
	})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, _ := url.Parse(authResp["authorization_url"].(string))
	nonce := parsed.Query().Get("nonce")
	idp.set(nonce, "someone@not-allowed.example", "Someone", "sub-2")

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusForbidden {
		t.Fatalf("status=%d body=%v, want 403", status, body)
	}

	var lastError *string
	var providerStatus string
	if err := st.pool.QueryRow(ctx, `SELECT last_error, status FROM sso_providers WHERE id = $1`, providerID).
		Scan(&lastError, &providerStatus); err != nil {
		t.Fatal(err)
	}
	if lastError != nil {
		t.Fatalf("last_error = %v, want nil: a domain rejection is a bare HTTPException in Python, outside record_error", *lastError)
	}
	if providerStatus != "active" {
		t.Fatalf("provider status = %q, want unchanged (active)", providerStatus)
	}
	var auditCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action = 'sso_login'`,
		providerID.String()).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("audit_logs: got %d sso_login rows for this provider, want 0 (a bare HTTPException, no audit call)", auditCount)
	}
}

// TestOIDCCallbackRefusesATamperedState pins the state-signature check: a
// flipped character never reaches the exchange.
func TestOIDCCallbackRefusesATamperedState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: "{}", autoProvision: true})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	state, _ := authResp["state"].(string)
	tampered := state[:len(state)-1] + flipLastRune(state)

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": tampered})
	if status != http.StatusBadRequest || body["detail"] != "OIDC authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OIDC authentication failed", status, body)
	}
}

func flipLastRune(s string) string {
	if s == "" {
		return "x"
	}
	last := s[len(s)-1]
	if last == 'a' {
		return "b"
	}
	return "a"
}

// TestOIDCCallbackRefusesAnUnknownUserWithoutAutoProvision pins the
// provisioning-stage failure: the fixed detail differs from the exchange
// failure's, and the audit row's metadata records the stage.
func TestOIDCCallbackRefusesAnUnknownUserWithoutAutoProvision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := startFakeIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"c","issuer":%q,"token_endpoint":%q,"jwks_uri":%q}`, idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oidc", status: "active", config: config, autoProvision: false,
	})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, _ := url.Parse(authResp["authorization_url"].(string))
	nonce := parsed.Query().Get("nonce")
	idp.set(nonce, "nobody@example.test", "Nobody", "sub-3")

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OIDC user provisioning failed" {
		t.Fatalf("status=%d body=%v, want 400 OIDC user provisioning failed", status, body)
	}

	var meta []byte
	if err := st.pool.QueryRow(ctx, `SELECT request_metadata::text FROM audit_logs
WHERE org_id = $1 AND action = 'sso_login' AND status = 'failure' ORDER BY created_at DESC LIMIT 1`, orgID).Scan(&meta); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(meta), `"stage": "provisioning"`) && !strings.Contains(string(meta), `"stage":"provisioning"`) {
		t.Fatalf("audit request_metadata = %s, want a provisioning stage", meta)
	}
}

// TestOIDCCallbackRefusesAnExpiredState pins D2727-amended's expiry check:
// a state whose auth tag verifies but whose embedded expires_at has
// passed is refused, and the recorded reason distinguishes it from a
// tampered/foreign one.
func TestOIDCCallbackRefusesAnExpiredState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: "{}", autoProvision: true})

	st.clock.freeze(time.Now())
	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	// oidcStateTTL (state.go) is 10 minutes.
	st.clock.advance(11 * time.Minute)

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OIDC authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OIDC authentication failed", status, body)
	}
	var lastError *string
	if err := st.pool.QueryRow(ctx, `SELECT last_error FROM sso_providers WHERE id = $1`, providerID).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || !strings.Contains(*lastError, "expired") {
		t.Fatalf("last_error = %v, want a recorded expiry reason", lastError)
	}
}

// TestOIDCCallbackRefusesStateIssuedForAnotherProvider pins the
// provider-binding check: a state minted for provider A, submitted to
// provider B's callback path, is refused even though the AEAD auth tag
// verifies (it is a genuine token this api minted, just for someone else).
func TestOIDCCallbackRefusesStateIssuedForAnotherProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	providerA := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: "{}", autoProvision: true})
	providerB := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: "{}", autoProvision: true})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerA.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerB.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OIDC authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OIDC authentication failed", status, body)
	}
	var lastError *string
	if err := st.pool.QueryRow(ctx, `SELECT last_error FROM sso_providers WHERE id = $1`, providerB).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || !strings.Contains(*lastError, "mismatch") {
		t.Fatalf("last_error = %v, want a recorded mismatch reason", lastError)
	}
}

// TestOIDCCallbackRefusesAWrongNonce pins the nonce check this package
// added where Python's own is a no-op (state.go's doc comment): an id_token
// whose nonce does not match the one embedded in the state is refused,
// simulating a state stolen and replayed against a different IdP
// authentication session.
func TestOIDCCallbackRefusesAWrongNonce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := startFakeIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"c","issuer":%q,"token_endpoint":%q,"jwks_uri":%q}`, idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks")
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "oidc", status: "active", config: config, autoProvision: true})

	status, authResp := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	// A nonce the fake IdP was never told about: the id_token it mints
	// carries whatever idp.set last recorded, which the real authorize
	// step's own nonce (embedded in the state) will not match.
	idp.set("a-completely-different-nonce", "someone@example.test", "Someone", "sub-4")

	status, body := postJSON(t, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusBadRequest || body["detail"] != "OIDC authentication failed" {
		t.Fatalf("status=%d body=%v, want 400 OIDC authentication failed", status, body)
	}
	var lastError *string
	if err := st.pool.QueryRow(ctx, `SELECT last_error FROM sso_providers WHERE id = $1`, providerID).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || !strings.Contains(*lastError, "nonce") {
		t.Fatalf("last_error = %v, want a recorded nonce-mismatch reason", lastError)
	}
}
