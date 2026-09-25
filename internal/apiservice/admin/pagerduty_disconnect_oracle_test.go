//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pagerDutyDisconnectVenueEncryptionKey/ClientID mirror
// orgDeletionVenueEncryptionKey/orgDeletionVenuePagerDutyID's own naming
// (both planes must agree on the key; the client id is only ever compared
// to itself across planes, never checked by the fake server).
const (
	pagerDutyDisconnectVenueEncryptionKey = "venue-pd-disconnect-fernet-key-32-bytes!"
	pagerDutyDisconnectVenueClientID      = "venue-pd-disconnect-client-id"
)

// pagerDutyDisconnectFakeServer is the same shape as
// orgdeletion_pagerduty_oracle_test.go's fakePagerDutyRevokeServer, plus a
// per-token failure list: a revoke POST whose token is in failToken answers
// 500 (both planes must retain the pending row and answer 503), everything
// else answers 200.
type pagerDutyDisconnectFakeServer struct {
	mu         sync.Mutex
	calls      []string
	failTokens map[string]bool
	// redirectTo is where a revoke of the redirect token is sent (307, which
	// replays the POST body); captured counts requests that reach it.
	redirectTo string
	captured   int
}

func (f *pagerDutyDisconnectFakeServer) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	values, _ := url.ParseQuery(string(body))
	token := values.Get("token")
	f.mu.Lock()
	f.calls = append(f.calls, token)
	fail := f.failTokens[token]
	redirectTo := f.redirectTo
	f.mu.Unlock()
	if token == "venue-pd-disc-redirect-token" {
		w.Header().Set("Location", redirectTo+"/captured")
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (f *pagerDutyDisconnectFakeServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// TestPagerDutyDisconnectVenueOracle is the venue-oracle proof for POST
// .../disconnect: both planes revoke the same fake server (CHAOS-6306's
// own test seam, VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE -- no new Python test
// seam needed), and the settings/webhook-binding/OAuth-credential rows
// left after the request are compared byte for byte.
func TestPagerDutyDisconnectVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-disconnect-32-by"

	fake := &pagerDutyDisconnectFakeServer{failTokens: map[string]bool{}}
	fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(fakeServer.Close)
	capture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.captured++
		fake.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(capture.Close)
	fake.redirectTo = capture.URL

	type orgSpec struct{ id uuid.UUID }
	orgs := map[string]*orgSpec{}
	for _, slug := range []string{"connected", "empty", "bad-cipher", "with-binding", "revoke-fails", "revoke-redirects", "malformed-json", "extra-key"} {
		orgs[slug] = &orgSpec{id: uuid.New()}
	}
	adminID, memberID := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"PAGER_DUTY_CLIENT_ID=" + pagerDutyDisconnectVenueClientID,
			"SETTINGS_ENCRYPTION_KEY=" + pagerDutyDisconnectVenueEncryptionKey,
			"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE=" + fakeServer.URL,
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, org := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org.id, "pd-disc-"+slug)
			}
			user := func(id uuid.UUID, email string) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			user(adminID, "pd-disc-admin@example.com")
			user(memberID, "pd-disc-member@example.com")

			encrypt := func(plaintext string) string {
				results := v.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{plaintext}})
				var ciphertext string
				if err := json.Unmarshal(results[0], &ciphertext); err != nil {
					t.Fatalf("decode encrypt_value result: %v", err)
				}
				return ciphertext
			}
			credential := func(orgSlug string) uuid.UUID {
				id := uuid.New()
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'default', true, NULL, '{"auth_mode":"oauth","region":"us","subdomain":"acme"}'::json, now(), now())`,
					id, orgs[orgSlug].id.String())
				return id
			}
			oauthTokens := func(orgSlug, token string) {
				plaintext := `{"access_token":"` + token + `","refresh_token":null,"expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), false)`, orgs[orgSlug].id.String(), encrypt(plaintext))
			}

			credential("connected")
			oauthTokens("connected", "venue-pd-disc-connected-token")

			credential("bad-cipher")
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["bad-cipher"].id.String())

			credential("revoke-fails")
			oauthTokens("revoke-fails", "venue-pd-disc-fails-token")
			fake.mu.Lock()
			fake.failTokens["venue-pd-disc-fails-token"] = true
			// The fake refuses the empty token and the extra-key payload's
			// token: a plane that revokes either answers 503 and keeps a
			// pending row, where Python (model validation fails) answers 200.
			fake.failTokens[""] = true
			fake.failTokens["venue-pd-disc-extra-token"] = true
			fake.mu.Unlock()

			// Decryptable but not an OAuthTokens object: Python's model
			// validation fails, so it revokes nothing; a partial JSON decode
			// would read an empty token and queue a revocation for it.
			credential("malformed-json")
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), false)`, orgs["malformed-json"].id.String(), encrypt(`{}`))
			credential("extra-key")
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), false)`, orgs["extra-key"].id.String(),
				encrypt(`{"access_token":"venue-pd-disc-extra-token","expires_at":"2099-01-01T00:00:00Z","surprise":true}`))
			credential("revoke-redirects")
			oauthTokens("revoke-redirects", "venue-pd-disc-redirect-token")

			bindingCredentialID := credential("with-binding")
			oauthTokens("with-binding", "venue-pd-disc-binding-token")
			integrationID := uuid.New()
			exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'pd', '{}'::json, true, now(), now())`, integrationID, orgs["with-binding"].id.String())
			sourceID := uuid.New()
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'pagerduty', 'service', 'ext-1', 'svc', 'svc', '{}'::json, true, now(), now())`,
				sourceID, orgs["with-binding"].id.String(), integrationID)
			exec(`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'sub-1', $5, 'v1', 'active', now(), now())`,
				uuid.New(), orgs["with-binding"].id, sourceID, bindingCredentialID, encrypt("venue-pd-disc-signing-secret"))

			return map[string]map[string]any{
				"admin_connected":        {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["connected"].id.String(), "role": "admin"},
				"admin_empty":            {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["empty"].id.String(), "role": "admin"},
				"admin_bad_cipher":       {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["bad-cipher"].id.String(), "role": "admin"},
				"admin_with_binding":     {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["with-binding"].id.String(), "role": "admin"},
				"admin_malformed_json":   {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["malformed-json"].id.String(), "role": "admin"},
				"admin_extra_key":        {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["extra-key"].id.String(), "role": "admin"},
				"admin_revoke_redirects": {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["revoke-redirects"].id.String(), "role": "admin"},
				"admin_revoke_fails":     {"user_id": adminID.String(), "email": "pd-disc-admin@example.com", "org_id": orgs["revoke-fails"].id.String(), "role": "admin"},
				"member":                 {"user_id": memberID.String(), "email": "pd-disc-member@example.com", "org_id": orgs["connected"].id.String(), "role": "member"},
			}
		},
	})

	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const path = "/api/v1/admin/integrations/pagerduty/disconnect"
	disconnect := func(name, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}

	requests := []venueoracle.Request{
		disconnect("W disconnect connected (revokes)", "admin_connected", `{}`),
		disconnect("W disconnect again is idempotent", "admin_connected", `{}`),
		disconnect("W disconnect empty org has no row", "admin_empty", `{}`),
		disconnect("W disconnect bad cipher skips revoke", "admin_bad_cipher", `{}`),
		disconnect("W disconnect detaches webhook binding", "admin_with_binding", `{}`),
		disconnect("W disconnect revoke fails, pending retry", "admin_revoke_fails", `{}`),
		disconnect("W disconnect revoke redirected is a failure, not followed", "admin_revoke_redirects", `{}`),
		disconnect("W disconnect token JSON is not an OAuthTokens object", "admin_malformed_json", `{}`),
		disconnect("W disconnect token JSON with an extra key", "admin_extra_key", `{}`),
		disconnect("W disconnect credential_name blank", "admin_empty", `{"credential_name":"  "}`),
		disconnect("W disconnect extra field", "admin_empty", `{"bogus":1}`),
		disconnect("W disconnect body list", "admin_empty", `[]`),
		disconnect("W disconnect invalid json", "admin_empty", `{`),
		{Name: "W disconnect unauthenticated", Method: "POST", Path: path, Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{}`)},
		disconnect("W disconnect member refused", "member", `{}`),
	}

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyDisconnectVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: pagerDutyDisconnectVenueClientID, RevokeURL: fakeServer.URL}
	})
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	compare("integration_credentials rows", `SELECT org_id, provider, name, is_active, (credentials_encrypted IS NULL)::text FROM integration_credentials ORDER BY org_id, name`)
	compare("provider_oauth_credentials rows", `SELECT org_id, provider, credential_name FROM provider_oauth_credentials ORDER BY org_id`)
	compare("provider_oauth_revocations rows", `SELECT org_id, provider, credential_name, status FROM provider_oauth_revocations ORDER BY org_id, created_at`)
	compare("pagerduty_webhook_bindings rows", `SELECT org_id, status, (credential_id IS NULL)::text, (revoked_at IS NULL)::text FROM pagerduty_webhook_bindings ORDER BY org_id`)

	// A revoke-fails org's row must still be pending on BOTH planes: the
	// fake server rejecting it, not either plane silently dropping it, is
	// what this assertion actually proves.
	pending := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT count(*) FROM provider_oauth_revocations WHERE org_id = '`+orgs["revoke-fails"].id.String()+`' AND status = 'pending'`)
	if pending != "1" {
		t.Errorf("revoke-fails org: go plane has %s pending revocations, want 1", pending)
	}
	fake.mu.Lock()
	captured := fake.captured
	fake.mu.Unlock()
	if captured != 0 {
		t.Errorf("a revoke redirect was followed and delivered a token: %d request(s) reached the redirect target", captured)
	}
	if fake.count() < 4 {
		t.Errorf("fake pagerduty revoke server saw only %d calls", fake.count())
	}
}
