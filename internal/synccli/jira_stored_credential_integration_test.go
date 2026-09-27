//go:build integration

package synccli

import (
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

const (
	testEncryptionKey  = "gwc-atlassian-teams-test-key"
	testEncryptionSalt = "gwc-atlassian-teams-test-salt"
)

// seedJiraCredential creates the minimal integration_credentials row
// PostgresCredentialRepository.ResolveEncrypted reads -- the same table
// work-items sync and the worker's post-sync team_autoimport job resolve
// their credential from (D2770).
func seedJiraCredential(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string, config map[string]string, secretFields map[string]string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS integration_credentials (
	id uuid PRIMARY KEY,
	org_id text NOT NULL,
	provider text NOT NULL,
	name text NOT NULL,
	is_active boolean NOT NULL,
	credentials_encrypted text,
	config jsonb
)`)
	if err != nil {
		t.Fatalf("create integration_credentials: %v", err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(testEncryptionKey), testEncryptionSalt)
	if err != nil {
		t.Fatalf("build test decryptor: %v", err)
	}
	plaintext, err := json.Marshal(secretFields)
	if err != nil {
		t.Fatalf("marshal secret fields: %v", err)
	}
	ciphertext, err := decryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("encrypt test credential: %v", err)
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config)
		 VALUES ($1, $2, 'jira', 'default', true, $3, $4::jsonb)`,
		uuid.New(), orgID, ciphertext.Reveal(), string(configJSON))
	if err != nil {
		t.Fatalf("insert test credential: %v", err)
	}
}

func testDecryptor() providerfoundation.CredentialDecryptor {
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(testEncryptionKey), testEncryptionSalt)
	if err != nil {
		panic(err)
	}
	return decryptor
}

// TestResolveJiraStoredSettingsUsesTheStoredCredential is the D2770 proof: no
// ATLASSIAN_* environment at all, only a stored integration_credentials row
// -- the exact bigboy shape the read-only readback found NOT proven.
func TestResolveJiraStoredSettingsUsesTheStoredCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(ctx) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	tenantInfo := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != atlassianteams.TenantInfoPath {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"cloudId":"tenant-info-cloud-id"}`))
	}))
	defer tenantInfo.Close()
	tenantURL := tenantInfo.URL // http://127.0.0.1:PORT

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": tenantURL, "atlassian_organization_id": "atlassian-org-123"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), tenantInfo.Client(), orgID)
	if err != nil {
		t.Fatalf("resolveJiraStoredSettings: %v", err)
	}
	if settings.organizationID != "atlassian-org-123" {
		t.Errorf("organizationID = %q, want the stored config value", settings.organizationID)
	}
	if settings.cloudID != "tenant-info-cloud-id" {
		t.Errorf("cloudID = %q, want the tenant_info-derived value", settings.cloudID)
	}
	if settings.email != "sync@example.test" || settings.token != "s3cr3t-token" {
		t.Errorf("email/token = %q/%q, want the stored values", settings.email, settings.token)
	}
	if !strings.HasSuffix(settings.gatewayURL, gatewayPath) || !strings.HasPrefix(settings.gatewayURL, tenantURL) {
		t.Errorf("gatewayURL = %q", settings.gatewayURL)
	}
}

// TestResolveJiraStoredSettingsRespectsAConfigCloudIDOverride proves the
// atlassian_cloud_id config key wins over a live tenant_info call, so an
// operator can pin the site id without depending on that endpoint.
func TestResolveJiraStoredSettingsRespectsAConfigCloudIDOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(ctx) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	calledTenantInfo := false
	tenantInfo := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calledTenantInfo = true
		_, _ = w.Write([]byte(`{"cloudId":"should-not-be-used"}`))
	}))
	defer tenantInfo.Close()

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": tenantInfo.URL, "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), tenantInfo.Client(), orgID)
	if err != nil {
		t.Fatalf("resolveJiraStoredSettings: %v", err)
	}
	if settings.cloudID != "pinned-cloud-id" {
		t.Errorf("cloudID = %q, want the pinned config override", settings.cloudID)
	}
	if calledTenantInfo {
		t.Error("tenant_info was called even though atlassian_cloud_id was already configured")
	}
}

// TestResolveJiraStoredSettingsRefusesWithoutAnOrganizationID plants exactly
// the D2770 defect this guard exists to catch: a stored jira credential with
// email/token/base_url (everything the OLD verb needed) but no
// atlassian_organization_id -- proving the guard actually fires rather than
// silently proceeding with an empty organization id the Teams API would
// reject anyway.
func TestResolveJiraStoredSettingsRefusesWithoutAnOrganizationID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(ctx) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	_, err = resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, orgID)
	if err == nil {
		t.Fatal("expected a refusal: the stored credential has no atlassian_organization_id")
	}
	if !strings.Contains(err.Error(), "atlassian_organization_id") {
		t.Errorf("error does not name the missing field: %v", err)
	}
}

// TestResolveJiraStoredSettingsRefusesWithNoRow is the negative control for
// the credential-resolution guard itself: an org with no jira integration at
// all must refuse, not silently produce empty settings.
func TestResolveJiraStoredSettingsRefusesWithNoRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(ctx) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS integration_credentials (
		id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, name text NOT NULL,
		is_active boolean NOT NULL, credentials_encrypted text, config jsonb)`); err != nil {
		t.Fatal(err)
	}

	_, err = resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, "org-with-no-integration")
	if err == nil {
		t.Fatal("expected a refusal: no stored jira credential exists for this org")
	}
}
