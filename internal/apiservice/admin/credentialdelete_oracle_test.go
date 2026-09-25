//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestCredentialDeleteVenueOracle is the venue-oracle proof for DELETE
// /admin/credentials/{provider}/{name}: the real Python api and the real Go
// api answer the same requests against two copies of one database, both
// revoking PagerDuty grants against the same fake endpoint, and the rows left
// (credentials, OAuth grants, revocations, webhook bindings) are compared.
func TestCredentialDeleteVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-credential-delete-32-by"

	fake := &pagerDutyDisconnectFakeServer{failTokens: map[string]bool{}}
	fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(fakeServer.Close)

	orgs := map[string]uuid.UUID{}
	for _, slug := range []string{"main", "other", "connected", "empty", "bad-cipher", "with-binding", "revoke-fails"} {
		orgs[slug] = uuid.New()
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
			for slug, id := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "cred-del-"+slug)
			}
			user := func(id uuid.UUID, email string) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			user(adminID, "cred-del-admin@example.com")
			user(memberID, "cred-del-member@example.com")

			encrypt := func(plaintext string) string {
				results := v.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{plaintext}})
				var ciphertext string
				if err := json.Unmarshal(results[0], &ciphertext); err != nil {
					t.Fatalf("decode encrypt_value result: %v", err)
				}
				return ciphertext
			}
			credential := func(org, provider, name string, active bool, payload any) uuid.UUID {
				id := uuid.New()
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, '{"auth_mode":"oauth","region":"us","subdomain":"acme"}'::json, now(), now())`, id, orgs[org].String(), provider, name, active, payload)
				return id
			}
			oauthTokens := func(org, name, token string) {
				plaintext := `{"access_token":"` + token + `","refresh_token":null,"expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', $2, $3, 1, now(), now(), false)`, orgs[org].String(), name, encrypt(plaintext))
			}

			credential("main", "github", "default", true, encrypt(`{"token":"t"}`))
			credential("main", "github", "second", false, nil)
			credential("main", "github", "my name", true, encrypt(`{"token":"t"}`))
			credential("main", "gitlab", "default", true, encrypt(`{"token":"t"}`))
			linked := credential("main", "jira", "linked", true, encrypt(`{"token":"t"}`))
			exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, credential_id, created_at, updated_at)
VALUES ($1, $2, 'jira', 'jira-int', '{}'::json, true, $3, now(), now())`, uuid.New(), orgs["main"].String(), linked)
			credential("other", "github", "default", true, encrypt(`{"token":"t"}`))

			credential("connected", "pagerduty", "default", true, nil)
			oauthTokens("connected", "default", "venue-cred-del-connected-token")
			credential("connected", "pagerduty", "acme", true, nil)
			credential("bad-cipher", "pagerduty", "default", true, nil)
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["bad-cipher"].String())
			credential("revoke-fails", "pagerduty", "default", true, nil)
			oauthTokens("revoke-fails", "default", "venue-cred-del-fails-token")
			fake.mu.Lock()
			fake.failTokens["venue-cred-del-fails-token"] = true
			fake.mu.Unlock()
			bindingCredential := credential("with-binding", "pagerduty", "default", true, nil)
			oauthTokens("with-binding", "default", "venue-cred-del-binding-token")
			integrationID := uuid.New()
			exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'pd', '{}'::json, true, now(), now())`, integrationID, orgs["with-binding"].String())
			sourceID := uuid.New()
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'pagerduty', 'service', 'ext-1', 'svc', 'svc', '{}'::json, true, now(), now())`, sourceID, orgs["with-binding"].String(), integrationID)
			exec(`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'sub-1', $5, 'v1', 'active', now(), now())`, uuid.New(), orgs["with-binding"], sourceID, bindingCredential, encrypt("venue-cred-del-signing-secret"))

			admin_ := func(slug string) map[string]any {
				return map[string]any{"user_id": adminID.String(), "email": "cred-del-admin@example.com", "org_id": orgs[slug].String(), "role": "admin"}
			}
			out := map[string]map[string]any{
				"member": {"user_id": memberID.String(), "email": "cred-del-member@example.com", "org_id": orgs["main"].String(), "role": "member"},
			}
			for _, slug := range []string{"main", "other", "connected", "empty", "bad-cipher", "with-binding", "revoke-fails"} {
				out["admin_"+slug] = admin_(slug)
			}
			return out
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	del := func(name, token, provider, credential string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "DELETE", Path: "/api/v1/admin/credentials/" + provider + "/" + url.PathEscape(credential), Headers: auth(token)}
	}
	requests := []venueoracle.Request{
		del("W delete a github credential", "admin_main", "github", "default"),
		del("W delete it again", "admin_main", "github", "default"),
		del("W delete an inactive credential without a payload", "admin_main", "github", "second"),
		del("W delete a name with a space", "admin_main", "github", "my name"),
		del("W delete a gitlab credential", "admin_main", "gitlab", "default"),
		del("W delete an unknown name", "admin_main", "github", "nope"),
		del("W delete an unknown provider", "admin_main", "bitbucket", "default"),
		del("W delete provider is case sensitive", "admin_main", "GitHub", "default"),
		del("W delete a credential an integration references", "admin_main", "jira", "linked"),
		del("W delete another organisation's credential", "admin_main", "github", "other"),
		del("W delete the other organisation's own", "admin_other", "github", "default"),
		del("W delete a connected pagerduty credential (revokes)", "admin_connected", "pagerduty", "default"),
		del("W delete it again (disconnect is idempotent)", "admin_connected", "pagerduty", "default"),
		del("W delete a pagerduty credential without a grant", "admin_connected", "pagerduty", "acme"),
		del("W delete pagerduty with no credential", "admin_empty", "pagerduty", "default"),
		del("W delete pagerduty with an unreadable grant", "admin_bad-cipher", "pagerduty", "default"),
		del("W delete pagerduty whose revoke is refused", "admin_revoke-fails", "pagerduty", "default"),
		del("W delete pagerduty with a webhook binding", "admin_with-binding", "pagerduty", "default"),
		del("W delete member refused", "member", "github", "default"),
		{Name: "W delete unauthenticated", Method: "DELETE", Path: "/api/v1/admin/credentials/github/default"},
		{Name: "credential post is 405", Method: "POST", Path: "/api/v1/admin/credentials/github/default", Headers: auth("admin_main")},
		{Name: "credential put is 405", Method: "PUT", Path: "/api/v1/admin/credentials/github/default", Headers: auth("admin_main")},
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
			t.Errorf("%s differs after the requests:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	compare("integration_credentials rows", `SELECT org_id, provider, name, is_active, (credentials_encrypted IS NULL)::text FROM integration_credentials ORDER BY org_id, provider, name`)
	compare("provider_oauth_credentials rows", `SELECT org_id, provider, credential_name FROM provider_oauth_credentials ORDER BY org_id, credential_name`)
	compare("provider_oauth_revocations rows", `SELECT org_id, provider, credential_name, purpose, status, attempts FROM provider_oauth_revocations ORDER BY org_id, credential_name, created_at`)
	compare("pagerduty_webhook_bindings rows", `SELECT org_id, status, (credential_id IS NULL)::text, (revoked_at IS NULL)::text FROM pagerduty_webhook_bindings ORDER BY org_id`)
	compare("integrations rows", `SELECT org_id, provider, name, (credential_id IS NULL)::text FROM integrations ORDER BY org_id, name`)
}
