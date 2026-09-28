package synccli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
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

// resolveJiraStoredSettings resolves the org's stored Jira integration
// credential into this verb's settings shape -- the SAME
// providerfoundation.CredentialResolver -> PostgresCredentialRepository ->
// providerfoundation.NewJiraClient path work-items sync and the worker's
// post-sync team_autoimport job use (D2770), so `dho sync teams --provider
// jira` can never drift from the credential the rest of the jira sync path
// already resolves for this org. atlassian_organization_id (and,
// optionally, atlassian_cloud_id) are read from the SAME credential config
// JSON base_url already lives in; cloud id, when not stored, is derived
// live from the tenant's own base URL via atlassianteams.ResolveCloudID --
// shared with the automatic post-sync team-catalog collector
// (internal/workerservice), so the two paths can never derive it two
// different ways.
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
		cloudID, err = atlassianteams.ResolveCloudID(ctx, doer, tenant)
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

// openPostgresPool opens the domain database the stored jira credential is
// resolved from. The connect failure text can carry the effective login
// (CHAOS-6665): pgstorage.Boundary resolves it the same way pgx itself does
// (DSN, PGUSER/PGPASSWORD, service files), not just the DSN's own
// components, so a login that pgx picked up from the environment is
// redacted too.
func openPostgresPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, pgxDSN(dsn))
	if err != nil {
		return nil, pgstorage.Boundary(dsn).Redact(err)
	}
	return pool, nil
}

// pgxDSN normalizes the asyncpg dialect prefix POSTGRES_URI may carry
// (matching dblookup.go's syncPostgresDSN, but without that function's
// wider Python-parity restrictions -- this is a plain pgxpool connection,
// not a parity boundary).
func pgxDSN(dsn string) string {
	dsn = strings.Replace(dsn, "postgresql+asyncpg://", "postgresql://", 1)
	return strings.Replace(dsn, "postgres+asyncpg://", "postgresql://", 1)
}
