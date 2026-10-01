package synccli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"

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
// different ways. Organization id, when not stored, is likewise derived live
// via atlassianteams.ResolveOrganizationID (D2817/CHAOS-7020): the stored
// config value is an OVERRIDE only now, never the sole path -- newResolver
// builds the AGG gateway client that call needs (nil production default: a
// real *graph.Client; tests inject a fake). overrides carries the caller's
// already-parsed ATLASSIAN_* environment: an explicit ATLASSIAN_ORGANIZATION_ID/
// ATLASSIAN_CLOUD_ID is checked BEFORE any live resolution is attempted, not
// only reapplied by the caller afterward -- a resolution failure must never
// block an override the operator already supplied (P1, CHAOS-7020 r1).
func resolveJiraStoredSettings(
	ctx context.Context,
	pool *pgxpool.Pool,
	decryptor providerfoundation.CredentialDecryptor,
	doer providerfoundation.HTTPDoer,
	newResolver func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.OrganizationResolver,
	orgID string,
	overrides envOverrides,
) (settings, error) {
	if pool == nil {
		return settings{}, errors.New(PostgresURIKey + " is not set: required to resolve the org's stored jira credential")
	}
	// A process with no encryption key cannot open ANY stored credential; say so up front instead of
	// failing the first resolve with a bare "credential is invalid" (CHAOS-7132).
	if keyed, ok := decryptor.(interface{ Configured() bool }); ok && !keyed.Configured() {
		return settings{}, errors.New("encryption_key_not_configured: SETTINGS_ENCRYPTION_KEY is not set, so the org's stored jira credential cannot be opened")
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	resolver := providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: pool},
		Decryptor:  decryptor,
	}
	scope := providerfoundation.TenantScope{OrgID: orgID, Provider: "jira", IntegrationID: jiraTeamsScopeIntegrationID}
	// The credential is the one the org's jira integration points at -- the credential every sync
	// of the org resolves -- not "the one named default, else the first" (CHAOS-7132: with two jira
	// credentials the verb picked one no sync uses). No integration row keeps the by-name lookup.
	credentialID, err := jiraIntegrationCredentialID(ctx, pool, orgID)
	if err != nil {
		return settings{}, err
	}
	scope.CredentialID = credentialID
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

	// An explicit env override wins before any live resolution is even
	// attempted (P1, CHAOS-7020 r1): resolveTeamsSettings applies
	// overrides.cloudID/organizationID again below AFTER this function
	// returns, but only on a SUCCESSFUL return -- a live resolution failure
	// here used to return before the caller ever got a chance to apply the
	// override that would have made the failure moot. Checking both here
	// first means a documented override always works, never mind whether the
	// network resolution it exists to bypass would have failed.
	cloudID := strings.TrimSpace(credential.Config["atlassian_cloud_id"])
	if cloudID == "" {
		cloudID = strings.TrimSpace(overrides.cloudID)
	}
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
	gatewayURL := tenant.String() + gatewayPath

	// atlassian_organization_id is now an OVERRIDE only (D2817/CHAOS-7020):
	// when the integration's config doesn't carry one, resolve it live via
	// the AGG tenantContexts query using the SAME stored credential, rather
	// than refusing outright the way this verb always used to.
	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	if organizationID == "" {
		organizationID = strings.TrimSpace(overrides.organizationID)
	}
	if organizationID == "" {
		if newResolver == nil {
			return settings{}, fmt.Errorf(
				"the stored jira integration's config has no atlassian_organization_id and no resolver is configured to derive one (set it on the integration, or set %s)",
				OrganizationIDKey)
		}
		auth := atlassian.BasicAPITokenAuth{Email: email.Reveal(), Token: token}
		organizationID, err = atlassianteams.ResolveOrganizationID(ctx, newResolver(gatewayURL, auth), cloudID)
		if err != nil {
			return settings{}, fmt.Errorf(
				"the stored jira integration's config has no atlassian_organization_id, and resolving one failed: %w (set it on the integration, or set %s)",
				err, OrganizationIDKey)
		}
	}
	return settings{
		organizationID: organizationID,
		cloudID:        cloudID,
		email:          email.Reveal(),
		token:          token,
		gatewayURL:     gatewayURL,
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

// jiraIntegrationCredentialID returns the credential id of the org's active jira integration(s): "" when
// there is none or its only one has no stored credential (the by-name lookup then applies), the id when
// every integration shares one credential, and a loud refusal when they differ (an integration without a
// stored credential counts as different) -- the verb must not guess.
func jiraIntegrationCredentialID(ctx context.Context, pool *pgxpool.Pool, orgID string) (string, error) {
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT COALESCE(credential_id::text, '') FROM integrations
		 WHERE org_id = $1 AND lower(provider) = 'jira' AND is_active = TRUE
		 ORDER BY 1`, orgID)
	if err != nil {
		return "", fmt.Errorf("read the org's jira integrations: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", fmt.Errorf("read the org's jira integrations: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read the org's jira integrations: %w", err)
	}
	switch len(ids) {
	case 0:
		return "", nil
	case 1:
		// "" is the one integration that has no stored credential (environment-authenticated): the
		// by-name lookup applies, as before.
		return ids[0], nil
	}
	// An integration with no stored credential counts as a different candidate: skipping it would pick
	// the other one silently (r2, CHAOS-7132).
	shown := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			id = "<no stored credential>"
		}
		shown = append(shown, id)
	}
	return "", fmt.Errorf("the org has %d active jira integrations with different credentials (%s); this verb cannot choose one",
		len(ids), strings.Join(shown, ", "))
}
