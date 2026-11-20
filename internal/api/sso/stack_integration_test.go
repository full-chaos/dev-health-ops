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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/api/sso"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const testJWTKey = "oidc-6658-test-" + "0123456789abcdef0123456789abcdef"

// fakeIdP is the shared idpstub OpenID provider on a plain HTTP server. It answers any code with the user and nonce
// the test tells it to for the next exchange: set is called immediately before each callback.
type fakeIdP struct {
	*idpstub.OIDC
	server *httptest.Server
}

func startFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	stub, err := idpstub.NewOIDC(idpstub.OIDCOptions{AcceptAnyCode: true, NoPKCE: true, MinimalDiscovery: true})
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIdP{OIDC: stub, server: httptest.NewServer(stub.Handler())}
	stub.SetBaseURL(idp.server.URL)
	stub.SetIssuer(idp.server.URL)
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *fakeIdP) set(nonce, email, fullName, sub string) {
	idp.SetNext(idpstub.OIDCUser{Sub: sub, Email: email, Name: fullName}, nonce)
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
	logs   *syncBuffer // every log line of the stack's handlers
}

func startStack(t *testing.T, ctx context.Context) stack {
	t.Helper()
	return startStackWith(t, ctx, nil)
}

// startStackWith is startStack with issuer as sso.Deps.Issuer: non-nil,
// auth-service owns sessions (AUTH_PROXY_SESSIONS).
func startStackWith(t *testing.T, ctx context.Context, issuer sessionissue.Issuer) stack {
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

	if issuer == nil {
		issuer = stubIssuer{}
	}
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := policy.NewAuthenticator(policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, logger)

	clock := &testClock{}
	routes := sso.Routes(sso.Deps{
		Pool: pool, Guard: guard, Logger: logger, Now: clock.now,
		// No Cipher: decryptProviderSecret's own legacy-plaintext fallback
		// (oidc.go) covers a test provider's un-encrypted stored
		// client_secret without one. StateSecret is any fixed test key
		// (production wires SSO_STATE_SECRET).
		StateSecret: testJWTKey,
		// The production default (defaultOIDCClient, oidc.go) dials over
		// externalurl.GuardedTransport(), which -- correctly -- refuses a
		// loopback address, exactly what every httptest.Server in this
		// file binds to. That guard is externalurl's own, independently
		// tested; this override exists only so the fixture IdP is
		// reachable, not to weaken what a real deployment dials.
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		Issuer:     issuer,
	})
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return stack{pool: pool, server: server, clock: clock, logs: logs}
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
	isDefault                     bool   // CHAOS-6986: initiate_oauth_by_type's is_default preference.
	clientSecretEncryptedFallback string // stored as-is (no cipher configured in this test), simulating legacy plaintext.
}

func seedProvider(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, opts providerOpts) uuid.UUID {
	t.Helper()
	id := uuid.New()
	opts.config = strings.ReplaceAll(opts.config, "{provider_id}", id.String())
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
VALUES ($1, $2, $11, $3, $4, $9, $10, $5, 'member', $6::json, $7::json, $8::json, now(), now())`,
		id, orgID, opts.protocol, opts.status, opts.autoProvision, opts.config, secrets, opts.allowedDomains,
		opts.isDefault, true, "Test Provider "+id.String()[:8]); err != nil {
		t.Fatalf("seed provider: %v", err)
	}
	if opts.protocol == "oauth_gitlab" {
		bindSelfHostedGitLabAccounts(t, ctx, pool)
	}
	return id
}

// bindSelfHostedGitLabAccounts binds the gitlab accounts the fixtures seed to the self-hosted GitLab provider row of
// the stack, as go-api writes them (sso_provider_id), whichever of the account and the provider is seeded first. The
// fixtures' GitLab is a loopback instance, never gitlab.com.
func bindSelfHostedGitLabAccounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE users SET sso_provider_id = (SELECT id FROM sso_providers WHERE protocol = 'oauth_gitlab'
  AND coalesce(config::jsonb->>'base_url', '') <> '' ORDER BY created_at, id LIMIT 1)
WHERE auth_provider = 'gitlab' AND sso_provider_id IS NULL`); err != nil {
		t.Fatalf("bind gitlab accounts: %v", err)
	}
}

func postJSON(t *testing.T, server *httptest.Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return postJSONWithClient(t, http.DefaultClient, server, path, body)
}

// getJSON GETs path and decodes the JSON answer.
func getJSON(t *testing.T, server *httptest.Server, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// postJSONWithClient POSTs body as JSON with client and decodes the answer.
func postJSONWithClient(t *testing.T, client *http.Client, server *httptest.Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post(server.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// assertProviderRowUnchanged is D2738's core invariant: an unauthenticated
// callback failure writes nothing to sso_providers at all.
func assertProviderRowUnchanged(t *testing.T, ctx context.Context, pool *pgxpool.Pool, providerID uuid.UUID, wantStatus string) {
	t.Helper()
	var lastError *string
	var providerStatus string
	if err := pool.QueryRow(ctx, `SELECT last_error, status FROM sso_providers WHERE id = $1`, providerID).
		Scan(&lastError, &providerStatus); err != nil {
		t.Fatal(err)
	}
	if lastError != nil {
		t.Fatalf("last_error = %v, want nil: an unauthenticated state failure must never mutate the provider row (D2738)", *lastError)
	}
	if providerStatus != wantStatus {
		t.Fatalf("provider status = %q, want unchanged %q (D2738: an unauthenticated state failure must never flip it)", providerStatus, wantStatus)
	}
}

// assertSSOAuditStage confirms the most recent failure-status sso_login
// audit row for this org carries the given stage tag -- shared by both
// OIDC's state_auth tests.
func assertSSOAuditStage(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, stage string) {
	t.Helper()
	var meta []byte
	if err := pool.QueryRow(ctx, `SELECT request_metadata::text FROM audit_logs
WHERE org_id = $1 AND action = 'sso_login' AND status = 'failure' ORDER BY created_at DESC LIMIT 1`, orgID).Scan(&meta); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(meta), `"stage":"`+stage+`"`) && !strings.Contains(string(meta), `"stage": "`+stage+`"`) {
		t.Fatalf("audit request_metadata = %s, want a %s stage", meta, stage)
	}
}

// flipMiddleByte tampers a base64url-encoded AEAD token reliably: it
// decodes to raw bytes, flips every bit of the MIDDLE byte, and
// re-encodes. The earlier approach here (flipping the state string's LAST
// base64 character) could land on a padding-only bit range of the final
// character and silently decode to the SAME underlying bytes depending on
// the payload's length modulo 3 -- observed to intermittently (~50% of
// runs) pass a "tampered" state's GCM tag check when writing CHAOS-6986's
// own OAuth state test against a differently-shaped (shorter) payload. A
// middle byte is never a padding position, so this always changes the
// ciphertext GCM authenticates over, regardless of payload length.
func flipMiddleByte(s string) string {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return s + "x"
	}
	raw[len(raw)/2] ^= 0xFF
	return base64.RawURLEncoding.EncodeToString(raw)
}

// stubIssuer stands in for auth-service's issue route when a stack names no issuer: it hands out an opaque pair and
// writes nothing, so a login that needs a session still answers.
type stubIssuer struct{}

func (stubIssuer) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{Access: "stub-access-" + uuid.NewString(), Refresh: "stub-refresh-" + uuid.NewString(), ExpiresIn: 3600}, nil
}
