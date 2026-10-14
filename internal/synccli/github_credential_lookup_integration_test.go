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
	insert := func(name string, ciphertext *string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, 'github', $3, true, $4, '{}'::jsonb, now(), now())`, uuid.New(), orgID, name, ciphertext); err != nil {
			t.Fatal(err)
		}
	}

	_, err := storedGitHubCredential(ctx, baseURL, orgID, env)
	if failure := failureOf(t, err); failure.Reason != reasonCredentialRowNotFound {
		t.Fatalf("no row: reason %q, want %q", failure.Reason, reasonCredentialRowNotFound)
	}

	insert("default", nil)
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
	if _, err := pool.Exec(ctx, `ALTER TABLE integration_credentials RENAME TO integration_credentials_moved`); err != nil {
		t.Fatal(err)
	}
	_, err = storedGitHubCredential(ctx, baseURL, orgID, env)
	failure := failureOf(t, err)
	if failure.Reason != reasonDatabaseQueryFailed || failure.Class == "" || failure.Type == "" {
		t.Fatalf("missing table: %+v, want %q with an error class and type", failure, reasonDatabaseQueryFailed)
	}
}
