//go:build integration

package synccli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9678: the reasons of the lookup that need a real database, against the
// migrated schema: no row, an empty ciphertext, a query the database refuses, and
// the row that decrypts and builds a credential although its payload carries a key the
// credential does not use (the shape of a credential the web saves).
func TestGitHubCredentialLookupReasonsAgainstMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, baseURL := startDBLookupPostgres(ctx, t)
	orgID := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
		VALUES ($1, 'cred-reasons', 'cred-reasons', 'community', 'manual', true, now(), now())`, orgID); err != nil {
		t.Fatal(err)
	}
	env := envOf(map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey})
	insert := func(name string, ciphertext *string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, 'github', $3, true, $4, '{}'::jsonb, now(), now())`, id, orgID, name, ciphertext); err != nil {
			t.Fatal(err)
		}
		return id
	}
	integrate := func(credentialID *uuid.UUID) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
			VALUES ($1, $2, 'github', $3, 'gh', '{}', true, now(), now())`, uuid.New(), orgID, credentialID); err != nil {
			t.Fatal(err)
		}
	}

	_, err := storedGitHubCredential(ctx, baseURL, orgID, env)
	if failure := failureOf(t, err); failure.Reason != reasonGitHubIntegrationNotFound {
		t.Fatalf("no integration: reason %q, want %q", failure.Reason, reasonGitHubIntegrationNotFound)
	}

	credentialID := insert("default", nil)
	integrate(&credentialID)
	_, err = storedGitHubCredential(ctx, baseURL, orgID, env)
	if failure := failureOf(t, err); failure.Reason != reasonCiphertextEmpty {
		t.Fatalf("NULL ciphertext: reason %q, want %q", failure.Reason, reasonCiphertextEmpty)
	}
	if _, err := pool.Exec(ctx, `UPDATE integration_credentials SET credentials_encrypted = '' WHERE org_id = $1`, orgID); err != nil {
		t.Fatal(err)
	}
	_, err = storedGitHubCredential(ctx, baseURL, orgID, env)
	if failure := failureOf(t, err); failure.Reason != reasonCiphertextEmpty {
		t.Fatalf("empty ciphertext: reason %q, want %q", failure.Reason, reasonCiphertextEmpty)
	}

	// The row a web save writes: a token and an `org` key. It builds the credential and logs the key name.
	log := captureLookupLog(t)
	sealed := sealPayload(t, reasonTestKey, "", `{"token":"ghp_integration_token_value","org":"acme"}`)
	if _, err := pool.Exec(ctx, `UPDATE integration_credentials SET credentials_encrypted = $2 WHERE org_id = $1`, orgID, sealed); err != nil {
		t.Fatal(err)
	}
	credential, err := githubCredentialFromDB(ctx, baseURL, orgID, env)
	if err != nil || credential == nil || credential.Mode != CredentialPAT || credential.Token != "ghp_integration_token_value" {
		t.Fatalf("credential = %+v, %v, want the PAT of a payload with an unused `org` key", credential, err)
	}
	if !strings.Contains(log.String(), `"fields":"org"`) || strings.Contains(log.String(), "ghp_integration_token_value") {
		t.Fatalf("the ignored key name is not logged, or a value is: %s", log.String())
	}

	// A table the database refuses to read: the query fails, and the reason is not "no row".
	if _, err := pool.Exec(ctx, `ALTER TABLE integrations RENAME TO integrations_moved`); err != nil {
		t.Fatal(err)
	}
	_, err = storedGitHubCredential(ctx, baseURL, orgID, env)
	failure := failureOf(t, err)
	if failure.Reason != reasonDatabaseQueryFailed || failure.Class == "" || failure.Type == "" {
		t.Fatalf("missing table: %+v, want %q with an error class and type", failure, reasonDatabaseQueryFailed)
	}
}

// CHAOS-9678: prod's github credential rows are not named "default"; the active github integration points at
// one by credential_id and the verb reads that row (red on main: credential_row_not_found).
func TestGitHubCredentialIsTheOneTheActiveIntegrationPointsAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, baseURL := startDBLookupPostgres(ctx, t)
	orgID := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
		VALUES ($1, 'cred-integration', 'cred-integration', 'community', 'manual', true, now(), now())`, orgID); err != nil {
		t.Fatal(err)
	}
	env := envOf(map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey})
	credential := func(name, payload string) uuid.UUID {
		t.Helper()
		id, sealed := uuid.New(), sealPayload(t, reasonTestKey, "", payload)
		if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, 'github', $3, true, $4, '{}'::jsonb, now(), now())`, id, orgID, name, sealed); err != nil {
			t.Fatal(err)
		}
		return id
	}
	integration := func(credentialID *uuid.UUID, active bool) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
			VALUES ($1, $2, 'github', $3, $4, '{}', $5, now(), now())`, id, orgID, credentialID, "gh-"+id.String(), active); err != nil {
			t.Fatal(err)
		}
		return id
	}
	reset := func() {
		t.Helper()
		for _, statement := range []string{`DELETE FROM integrations`, `DELETE FROM integration_credentials`} {
			if _, err := pool.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("a credential not named default", func(t *testing.T) {
		reset()
		credential("decoy-default-looking", `{"token":"ghp_decoy"}`)
		wanted := credential("github-prod", `{"token":"ghp_wanted_token_value"}`)
		integration(&wanted, true)
		got, err := storedGitHubCredential(ctx, baseURL, orgID, env)
		if err != nil || got == nil || got.Token != "ghp_wanted_token_value" {
			t.Fatalf("credential = %+v, %v, want the token of the credential the integration points at", got, err)
		}
	})

	t.Run("a provider spelled with spaces and capitals", func(t *testing.T) {
		reset()
		wanted := credential("github-prod", `{"token":"ghp_spaced_provider_value"}`)
		id := integration(&wanted, true)
		if _, err := pool.Exec(ctx, `UPDATE integrations SET provider = ' GitHub ' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		got, err := storedGitHubCredential(ctx, baseURL, orgID, env)
		if err != nil || got == nil || got.Token != "ghp_spaced_provider_value" {
			t.Fatalf("credential = %+v, %v, want the integration found whatever the provider's case and surrounding spaces", got, err)
		}
	})

	refusals := []struct {
		name   string
		seed   func() []uuid.UUID
		reason string
	}{
		{"no active github integration", func() []uuid.UUID {
			id := credential("github-prod", `{"token":"ghp_t"}`)
			integration(&id, false)
			return nil
		}, reasonGitHubIntegrationNotFound},
		{"an integration with no credential", func() []uuid.UUID {
			credential("default", `{"token":"ghp_t"}`)
			return []uuid.UUID{integration(nil, true)}
		}, reasonGitHubIntegrationNoCredential},
		{"two active github integrations", func() []uuid.UUID {
			one, two := credential("one", `{"token":"ghp_1"}`), credential("two", `{"token":"ghp_2"}`)
			return []uuid.UUID{integration(&one, true), integration(&two, true)}
		}, reasonGitHubIntegrationsAmbiguous},
	}
	for _, refusal := range refusals {
		t.Run(refusal.name, func(t *testing.T) {
			reset()
			ids := refusal.seed()
			log := captureLookupLog(t)
			_, err := githubCredentialFromDB(ctx, baseURL, orgID, env)
			failure := failureOf(t, err)
			if failure.Reason != refusal.reason {
				t.Fatalf("reason %q, want %q", failure.Reason, refusal.reason)
			}
			if len(failure.IntegrationIDs) != len(ids) {
				t.Fatalf("integration ids %v, want %d", failure.IntegrationIDs, len(ids))
			}
			for _, id := range ids {
				if !strings.Contains(log.String(), id.String()) {
					t.Fatalf("log lacks integration id %s: %s", id, log.String())
				}
			}
			for _, want := range []string{`"reason":"` + refusal.reason + `"`, orgID} {
				if !strings.Contains(log.String(), want) {
					t.Fatalf("log lacks %q: %s", want, log.String())
				}
			}
			if strings.Contains(log.String(), "ghp_") {
				t.Fatalf("log carries a secret: %s", log.String())
			}
		})
	}
}
