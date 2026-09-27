package synccli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraTeamsScopeIntegrationID is the sentinel TenantScope.Validate requires
// but PostgresCredentialRepository.ResolveEncrypted's own query never reads
// (the same sentinel internal/api/teamsidentity/credentials.go uses for its
// own synchronous, unclaimed admin-discover lookup): `dho sync teams
// --provider jira` runs start to finish inside one CLI invocation, with no
// claimed sync-unit identity to name.
const jiraTeamsScopeIntegrationID = "cli-sync-teams"

// jiraTeamsTokenAliases mirrors providerfoundation's own unexported
// jiraAPITokenAliases (credentials.go). Duplicated rather than exported
// because it is read here ONLY to build the error-redaction boundary --
// providerfoundation.NewJiraClient below independently re-resolves the same
// aliases for the actual HTTP auth, so the two can never disagree about
// WHICH value is in use, only redundantly agree on it.
var jiraTeamsTokenAliases = []string{"api_token", "apiToken", "token"}

// atlassianTenantInfoPath is Atlassian's own unauthenticated, per-tenant
// endpoint that maps a site's base URL to its cloud id -- the same
// mechanism Atlassian Connect/Forge apps use to resolve cloudId without a
// stored value. Nothing in ops or the vendored atlassian client calls it
// yet.
const atlassianTenantInfoPath = "/_edge/tenant_info"

// resolveJiraStoredSettings resolves the org's stored Jira integration
// credential into this verb's settings shape -- the SAME
// providerfoundation.CredentialResolver -> PostgresCredentialRepository ->
// providerfoundation.NewJiraClient path work-items sync and the worker's
// post-sync team_autoimport job use (D2770), so `dho sync teams --provider
// jira` can never drift from the credential the rest of the jira sync path
// already resolves for this org. atlassian_organization_id (and,
// optionally, atlassian_cloud_id) are read from the SAME credential config
// JSON base_url already lives in; cloud id, when not stored, is derived
// live from the tenant's own base URL via atlassianTenantInfoPath.
func resolveJiraStoredSettings(
	ctx context.Context,
	pool *pgxpool.Pool,
	decryptor providerfoundation.CredentialDecryptor,
	doer providerfoundation.HTTPDoer,
	orgID string,
) (settings, error) {
	if pool == nil {
		return settings{}, errors.New(PostgresURIKey + " is not set: required to resolve the org's stored jira credential")
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	resolver := providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: pool},
		Decryptor:  decryptor,
	}
	scope := providerfoundation.TenantScope{OrgID: orgID, Provider: "jira", IntegrationID: jiraTeamsScopeIntegrationID}
	credential, err := resolver.Resolve(ctx, lease, scope)
	if err != nil {
		return settings{}, fmt.Errorf("resolve the org's stored jira credential: %w", err)
	}
	client, err := providerfoundation.NewJiraClient(credential, doer, providerfoundation.DefaultRetryPolicy(), lease)
	if err != nil {
		return settings{}, fmt.Errorf("build a jira client from the stored credential: %w", err)
	}
	if client.BaseURL == nil {
		return settings{}, errors.New("the stored jira credential has no base URL configured")
	}
	tenant, err := normalizeBase(client.BaseURL.String())
	if err != nil {
		return settings{}, fmt.Errorf("the stored jira credential's base url is not valid: %w", err)
	}

	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	if organizationID == "" {
		return settings{}, fmt.Errorf(
			"the stored jira integration's config has no atlassian_organization_id (set it on the integration, or set %s)",
			OrganizationIDKey)
	}
	cloudID := strings.TrimSpace(credential.Config["atlassian_cloud_id"])
	if cloudID == "" {
		cloudID, err = resolveCloudIDForTenant(ctx, doer, tenant)
		if err != nil {
			return settings{}, fmt.Errorf("resolve the atlassian cloud id: %w", err)
		}
	}

	email, _ := credential.Secret("email")
	token := ""
	for _, alias := range jiraTeamsTokenAliases {
		if value, ok := credential.Secret(alias); ok && value.Configured() {
			token = value.Reveal()
			break
		}
	}
	if !email.Configured() || token == "" {
		return settings{}, errors.New("the stored jira credential is missing email or an api token")
	}
	return settings{
		organizationID: organizationID,
		cloudID:        cloudID,
		email:          email.Reveal(),
		token:          token,
		gatewayURL:     tenant.String() + gatewayPath,
	}, nil
}

// resolveCloudIDForTenant calls atlassianTenantInfoPath first (a real
// Atlassian cloud id), falling back to the tenant subdomain -- the same
// derivation the env-only path has always used -- only when that call
// fails, so an outage of that endpoint does not newly break what an
// explicit override already worked around.
func resolveCloudIDForTenant(ctx context.Context, doer providerfoundation.HTTPDoer, tenant *url.URL) (string, error) {
	if id, err := fetchTenantCloudID(ctx, doer, tenant); err == nil && id != "" {
		return id, nil
	}
	host := tenant.Hostname()
	if i := strings.Index(host, "."); i > 0 {
		return host[:i], nil
	}
	return "", fmt.Errorf("the tenant_info endpoint failed and %q has no subdomain to fall back to", host)
}

func fetchTenantCloudID(ctx context.Context, doer providerfoundation.HTTPDoer, tenant *url.URL) (string, error) {
	if doer == nil {
		doer = &http.Client{Timeout: 15 * time.Second}
	}
	target := *tenant
	target.Path = atlassianTenantInfoPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return "", err
	}
	response, err := doer.Do(request)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tenant_info returned status %d", response.StatusCode)
	}
	var payload struct {
		CloudID string `json:"cloudId"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode tenant_info response: %w", err)
	}
	cloudID := strings.TrimSpace(payload.CloudID)
	if cloudID == "" {
		return "", errors.New("tenant_info response had no cloudId")
	}
	return cloudID, nil
}

func openPostgresPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, pgxDSN(dsn))
}

// pgxDSN normalizes the asyncpg dialect prefix POSTGRES_URI may carry
// (matching dblookup.go's syncPostgresDSN, but without that function's
// wider Python-parity restrictions -- this is a plain pgxpool connection,
// not a parity boundary).
func pgxDSN(dsn string) string {
	dsn = strings.Replace(dsn, "postgresql+asyncpg://", "postgresql://", 1)
	return strings.Replace(dsn, "postgres+asyncpg://", "postgresql://", 1)
}
