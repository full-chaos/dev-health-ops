//go:build integration

package synccli

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fakeOrganizationResolver serves ResolveOrganizationID a canned Execute
// result or error, so these tests never call a live Atlassian tenant.
type fakeOrganizationResolver struct {
	calls  int
	result *atlassian.Result
	err    error
}

func (f *fakeOrganizationResolver) Execute(context.Context, string, map[string]any, string, []string, int) (*atlassian.Result, error) {
	f.calls++
	return f.result, f.err
}

func fakeResolvedOrganizationID(orgID string) *atlassian.Result {
	return &atlassian.Result{Data: map[string]any{
		"tenantContexts": []any{map[string]any{"orgId": orgID, "cloudId": "resolved-cloud-id"}},
	}}
}

const (
	testEncryptionKey  = "gwc-atlassian-teams-test-key"
	testEncryptionSalt = "gwc-atlassian-teams-test-salt"
)

// applyRealSchema builds the REAL Postgres schema -- the pgmigrate baseline
// and chain, the same schema the Alembic heads produce -- so
// integration_credentials exists exactly as production has it (Trap #412:
// integration tests never hand-write DDL for production tables; see
// internal/api/externalingest/accept_batch_integration_test.go's identical
// pattern and internal/pgmigrate's TestHandWrittenTestDDLMatchesTheMigratedSchema).
func applyRealSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	conn, err := pgx.Connect(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatalf("connect for the schema: %v", err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
}

// seedJiraCredential inserts the integration_credentials row
// PostgresCredentialRepository.ResolveEncrypted reads -- the same table
// work-items sync and the worker's post-sync team_autoimport job resolve
// their credential from (D2770). Callers apply the real schema first via
// applyRealSchema.
func seedJiraCredential(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string, config map[string]string, secretFields map[string]string) {
	t.Helper()
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
		`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
		 VALUES ($1, $2, 'jira', 'default', true, $3, $4::jsonb, now(), now())`,
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
	applyRealSchema(t, ctx, pool)

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

	poisonResolver := func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		t.Fatal("the resolver must not be called when the config already has an organization id")
		return nil
	}
	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), tenantInfo.Client(), poisonResolver, orgID, envOverrides{})
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
	applyRealSchema(t, ctx, pool)

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

	poisonResolver := func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		t.Fatal("the resolver must not be called when the config already has an organization id")
		return nil
	}
	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), tenantInfo.Client(), poisonResolver, orgID, envOverrides{})
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

// TestResolveJiraStoredSettingsResolvesTheOrganizationIDWhenNotConfigured is
// the D2817/CHAOS-7020 proof: a stored jira credential with email/token/
// base_url but no atlassian_organization_id no longer refuses outright --
// it derives one live via the AGG gateway (faked here) using the same
// credential, so a bigboy integration chris never manually configured still
// gets real ARI-shaped rows.
func TestResolveJiraStoredSettingsResolvesTheOrganizationIDWhenNotConfigured(t *testing.T) {
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
	applyRealSchema(t, ctx, pool)

	const orgID = "org-under-test"
	// atlassian_cloud_id is pinned so this test exercises organization id
	// resolution alone, with no live tenant_info call in the mix.
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	resolver := &fakeOrganizationResolver{result: fakeResolvedOrganizationID("resolved-org-456")}
	newResolver := func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return resolver
	}

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, newResolver, orgID, envOverrides{})
	if err != nil {
		t.Fatalf("resolveJiraStoredSettings: %v", err)
	}
	if settings.organizationID != "resolved-org-456" {
		t.Errorf("organizationID = %q, want the resolved value", settings.organizationID)
	}
	if resolver.calls != 1 {
		t.Errorf("resolver called %d times, want 1", resolver.calls)
	}
}

// TestResolveJiraStoredSettingsNamesTheFieldWhenResolutionFails plants the
// permission-denied case ResolveOrganizationID names: a stored jira
// credential with no atlassian_organization_id whose resolution attempt
// fails must still refuse by naming the field, not surface a bare gateway
// error the caller has to decode.
func TestResolveJiraStoredSettingsNamesTheFieldWhenResolutionFails(t *testing.T) {
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
	applyRealSchema(t, ctx, pool)

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	resolver := &fakeOrganizationResolver{err: atlassianteams.ErrOrganizationPermission}
	newResolver := func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return resolver
	}

	_, err = resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, newResolver, orgID, envOverrides{})
	if err == nil {
		t.Fatal("expected a refusal: resolving the organization id failed")
	}
	if !strings.Contains(err.Error(), "atlassian_organization_id") {
		t.Errorf("error does not name the field: %v", err)
	}
	if !errors.Is(err, atlassianteams.ErrOrganizationPermission) {
		t.Errorf("error does not wrap ErrOrganizationPermission: %v", err)
	}
}

// TestResolveJiraStoredSettingsEnvOverrideBypassesAFailedResolution is the
// r1 P1 guard-failing proof (codex, gpt-6-luna xhigh): with no config value
// and a resolver that would fail exactly like
// TestResolveJiraStoredSettingsNamesTheFieldWhenResolutionFails above, an
// explicit ATLASSIAN_ORGANIZATION_ID must win BEFORE resolution is ever
// attempted -- an operator who already supplied the documented override must
// never be blocked by the AGG failure that override exists to bypass. Same
// resolver-fails setup as the sibling test; the only difference is the env
// override, and that alone must flip the outcome from refusal to success.
func TestResolveJiraStoredSettingsEnvOverrideBypassesAFailedResolution(t *testing.T) {
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
	applyRealSchema(t, ctx, pool)

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	resolver := &fakeOrganizationResolver{err: atlassianteams.ErrOrganizationPermission}
	newResolver := func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return resolver
	}

	settings, err := resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, newResolver, orgID,
		envOverrides{organizationID: "override-org-789"})
	if err != nil {
		t.Fatalf("resolveJiraStoredSettings: %v (the env override should have made resolution unnecessary)", err)
	}
	if settings.organizationID != "override-org-789" {
		t.Errorf("organizationID = %q, want the env override value", settings.organizationID)
	}
	if resolver.calls != 0 {
		t.Errorf("resolver called %d times, want 0 -- the override must win before resolution is ever attempted", resolver.calls)
	}
}

// TestResolveJiraStoredSettingsRefusesWithoutAnOrganizationIDWhenNoResolver
// is the negative control for the guard itself: with no resolver configured
// at all (the caller's own configuration error, distinct from a resolution
// failure), the verb still refuses by naming the field rather than
// proceeding with an empty organization id the Teams API would reject
// anyway.
func TestResolveJiraStoredSettingsRefusesWithoutAnOrganizationIDWhenNoResolver(t *testing.T) {
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
	applyRealSchema(t, ctx, pool)

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "s3cr3t-token"})

	_, err = resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, nil, orgID, envOverrides{})
	if err == nil {
		t.Fatal("expected a refusal: no resolver is configured and the config has no atlassian_organization_id")
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
	applyRealSchema(t, ctx, pool)

	_, err = resolveJiraStoredSettings(ctx, pool, testDecryptor(), http.DefaultClient, nil, "org-with-no-integration", envOverrides{})
	if err == nil {
		t.Fatal("expected a refusal: no stored jira credential exists for this org")
	}
}

// TestResolveTeamsSettingsHonorsAnIndividualTokenOverride is the codex review
// r1 fix proof (finding #6): resolveTeamsSettings documents every ATLASSIAN_*
// field as an INDIVIDUAL override on top of the stored credential, not only
// the organization/cloud id pair -- setting ATLASSIAN_API_TOKEN alone (never
// enough to satisfy the fully-env-configured offline path, so this still
// resolves the stored credential first) must override just the token, not be
// silently ignored in favor of the stored one.
func TestResolveTeamsSettingsHonorsAnIndividualTokenOverride(t *testing.T) {
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
	applyRealSchema(t, ctx, pool)

	const orgID = "org-under-test"
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "atlassian-org-123", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": "stored-token"})

	d := deps{
		openPostgres: func(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return pool, nil },
		decryptor:    func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil },
		doer:         http.DefaultClient,
	}
	env := cli.Env{Lookup: func(key string) (string, bool) {
		switch key {
		case "ATLASSIAN_API_TOKEN":
			return "overridden-token", true
		case PostgresURIKey:
			// d.openPostgres below ignores this value and hands back the
			// already-open test pool; a placeholder just has to satisfy
			// resolveTeamsSettings' "is it configured at all" check.
			return "postgres://ignored", true
		}
		return "", false
	}}
	settings, err := resolveTeamsSettings(ctx, env, d, orgID, "")
	if err != nil {
		t.Fatalf("resolveTeamsSettings: %v", err)
	}
	if settings.token != "overridden-token" {
		t.Errorf("token = %q, want the ATLASSIAN_API_TOKEN override, not the stored token", settings.token)
	}
	// Every other field must still come from the stored credential --
	// setting one override must not disturb the rest.
	if settings.email != "sync@example.test" {
		t.Errorf("email = %q, want the stored value (untouched by the token override)", settings.email)
	}
	if settings.organizationID != "atlassian-org-123" || settings.cloudID != "pinned-cloud-id" {
		t.Errorf("organizationID/cloudID = %q/%q, want the stored values", settings.organizationID, settings.cloudID)
	}
}
