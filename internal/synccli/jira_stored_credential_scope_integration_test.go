//go:build integration

package synccli

import (
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-7132: the verb used to resolve "the org's jira credential" by NAME (the one called default,
// else the first), while the worker resolves the credential the org's jira INTEGRATION points at.
// With two jira credentials the two disagreed: the verb picked a credential no sync uses.

func seedNamedJiraCredential(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, name string, config, secretFields map[string]string) uuid.UUID {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(testEncryptionKey), testEncryptionSalt)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, _ := json.Marshal(secretFields)
	ciphertext, err := decryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	configJSON, _ := json.Marshal(config)
	id := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
		 VALUES ($1, $2, 'jira', $3, true, $4, $5::jsonb, now(), now())`, id, orgID, name, ciphertext.Reveal(), string(configJSON)); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedJiraIntegrationWithoutCredential(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, name string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
		 VALUES ($1, $2, 'jira', NULL, $3, '{}'::json, true, now(), now())`, uuid.New(), orgID, name); err != nil {
		t.Fatal(err)
	}
}

func seedJiraIntegration(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, name string, credentialID uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
		 VALUES ($1, $2, 'jira', $3, $4, '{}'::json, true, now(), now())`, uuid.New(), orgID, credentialID, name); err != nil {
		t.Fatal(err)
	}
}

func scopeTestPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyRealSchema(t, ctx, pool)
	return ctx, pool
}

func TestResolveJiraStoredSettingsUsesTheCredentialTheIntegrationPointsAt(t *testing.T) {
	ctx, pool := scopeTestPool(t)
	tenant := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != atlassianteams.TenantInfoPath {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"cloudId":"tenant-cloud"}`))
	}))
	defer tenant.Close()
	const orgID = "org-two-credentials"
	// A credential named default that no integration uses and that lacks every required field...
	seedNamedJiraCredential(t, ctx, pool, orgID, "default", map[string]string{}, map[string]string{"unrelated": "x"})
	// ...and the one the integration (and so every sync) really uses.
	used := seedNamedJiraCredential(t, ctx, pool, orgID, "JIRA",
		map[string]string{"base_url": tenant.URL, "atlassian_organization_id": "org-123"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})
	seedJiraIntegration(t, ctx, pool, orgID, "bigboy jira", used)

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), tenant.Client(), nil, orgID, envOverrides{})
	if err != nil {
		t.Fatalf("the verb must resolve the integration's credential, not the one named default: %v", err)
	}
	if settings.email != "sync@example.test" || settings.organizationID != "org-123" {
		t.Errorf("resolved the wrong credential: email=%q organization=%q", settings.email, settings.organizationID)
	}
}

func TestResolveJiraStoredSettingsRefusesTwoIntegrationsWithDifferentCredentials(t *testing.T) {
	ctx, pool := scopeTestPool(t)
	const orgID = "org-two-integrations"
	first := seedNamedJiraCredential(t, ctx, pool, orgID, "alpha", map[string]string{"base_url": "https://a.example.test"}, map[string]string{"email": "a@example.test", "api_token": "tok-a"})
	second := seedNamedJiraCredential(t, ctx, pool, orgID, "beta", map[string]string{"base_url": "https://b.example.test"}, map[string]string{"email": "b@example.test", "api_token": "tok-b"})
	seedJiraIntegration(t, ctx, pool, orgID, "site a", first)
	seedJiraIntegration(t, ctx, pool, orgID, "site b", second)

	_, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, nil, orgID, envOverrides{})
	if err == nil {
		t.Fatal("two jira integrations with different credentials must be refused, not resolved by a name default")
	}
	if text := err.Error(); !strings.Contains(text, "2 active jira integrations") || strings.Contains(text, "tok-") {
		t.Errorf("the refusal must name the count and carry no secret: %q", text)
	}
}

// r2: an active integration with NO stored credential (environment-authenticated) is a candidate too:
// ignoring it would silently pick the other integration's credential.
func TestResolveJiraStoredSettingsRefusesAnIntegrationWithoutAStoredCredentialBesideOneWithIt(t *testing.T) {
	ctx, pool := scopeTestPool(t)
	const orgID = "org-null-and-stored"
	stored := seedNamedJiraCredential(t, ctx, pool, orgID, "default", map[string]string{"base_url": "https://a.example.test"}, map[string]string{"email": "default@example.test", "api_token": "tok-a"})
	seedJiraIntegration(t, ctx, pool, orgID, "stored", stored)
	seedJiraIntegrationWithoutCredential(t, ctx, pool, orgID, "env authenticated")

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, nil, orgID, envOverrides{})
	if err == nil {
		t.Fatalf("the verb silently selected stored settings for %q although another active integration has no stored credential", settings.email)
	}
	if text := err.Error(); !strings.Contains(text, "2 active jira integrations") || strings.Contains(text, "tok-") {
		t.Errorf("the refusal must name the count and carry no secret: %q", text)
	}
}

// CHAOS-7132 follow-up: the stored-credential path with the dependencies production really passes. It builds
// the deps with the REAL defaultDeps() (nothing injected) and resolves a stored credential whose base URL
// is the only network endpoint (and which carries the organization and cloud ids, so no live call is made).
// With defaultDeps() leaving doer nil, providerfoundation.NewJiraClient answered a bare "provider credential
// is invalid" and this failed.
func TestResolveJiraStoredSettingsWorksWithTheProductionDeps(t *testing.T) {
	ctx, pool := scopeTestPool(t)
	const orgID = "org-production-deps"
	used := seedNamedJiraCredential(t, ctx, pool, orgID, "JIRA",
		map[string]string{"base_url": "https://jira.example.test", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})
	seedJiraIntegration(t, ctx, pool, orgID, "production deps", used)

	d := defaultDeps()
	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), fakehttp.Client(d.doer), d.newOrganizationResolver, orgID, envOverrides{})
	if err != nil {
		t.Fatalf("the stored-credential path failed with the production deps: %v", err)
	}
	if settings.email != "sync@example.test" || settings.organizationID != "org-123" {
		t.Errorf("resolved the wrong settings: email=%q organization=%q", settings.email, settings.organizationID)
	}
}
