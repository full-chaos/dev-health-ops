package main

import (
	"context"
	"errors"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errTeamCatalogUnsupportedProvider means a provider was registered in the
// native collector map without a matching HTTP-client builder below. Fail
// closed rather than hand a collector a nil/wrong client.
var errTeamCatalogUnsupportedProvider = errors.New("team catalog provider has no native HTTP client builder")

// resolveTeamCatalogIntegration ports the exact join
// native_reference_discovery.go's resolveAuthoritativeProvider already uses
// (sync_runs JOIN integrations, fenced by org_id on both sides), extended to
// also return integration_id and credential_id. A sync run is tied to
// exactly ONE integration; an org may have more than one ACTIVE integration
// for the same provider (observed locally for linear, org
// 70d529e0-3c06-4597-8480-794fd02328b6: two active linear integrations), so
// resolving "the" credential for (org, provider) instead of this run's own
// integration would be ambiguous. Pinning credential_id from here means
// CredentialResolver.Resolve below needs no separate disambiguation.
func resolveTeamCatalogIntegration(
	ctx context.Context, pool *pgxpool.Pool, orgID, runID, provider string,
) (integrationID, credentialID string, err error) {
	if pool == nil || orgID == "" || runID == "" || provider == "" {
		return "", "", providersync.ErrInvalidConfiguration
	}
	queryErr := pool.QueryRow(ctx, `
SELECT integrations.id::text, COALESCE(integrations.credential_id::text, '')
FROM public.sync_runs
JOIN public.integrations ON integrations.id = sync_runs.integration_id
WHERE sync_runs.id = $1::uuid AND sync_runs.org_id = $2 AND integrations.org_id = $2
  AND lower(trim(integrations.provider)) = $3`,
		runID, orgID, provider).Scan(&integrationID, &credentialID)
	if errors.Is(queryErr, pgx.ErrNoRows) {
		return "", "", providersync.ErrInvalidConfiguration
	}
	if queryErr != nil {
		return "", "", queryErr
	}
	if credentialID == "" {
		return "", "", providersync.ErrInvalidConfiguration
	}
	return integrationID, credentialID, nil
}

// teamCatalogClientResolver implements syncdispatchruntime.ProviderClientResolver
// for every native team-catalog provider. Adding a provider here is the
// registration seam lane-4434 (GitHub) and lane-4432 (GitLab) use alongside
// their own entry in the Native collector map.
type teamCatalogClientResolver struct {
	pool        *pgxpool.Pool
	credentials providerfoundation.CredentialResolver
	doer        providerfoundation.HTTPDoer
	retry       providerfoundation.RetryPolicy
}

// teamCatalogLease is a trivial, ctx-bound LeaseGuard. Reference discovery
// here runs once per sync run with no claimed provider-unit lease behind it
// (CHAOS-4431 ruling) -- the only thing worth asserting before a provider
// call is that the caller's context is still live, which is exactly what
// HTTPClient's existing lease-check call sites need.
type teamCatalogLease struct{}

func (teamCatalogLease) Assert(ctx context.Context) error { return ctx.Err() }

func (resolver teamCatalogClientResolver) ResolveClient(
	ctx context.Context, orgID, runID, provider string,
) (providerfoundation.Credential, *providerfoundation.HTTPClient, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	_, credentialID, err := resolveTeamCatalogIntegration(ctx, resolver.pool, orgID, runID, provider)
	if err != nil {
		return providerfoundation.Credential{}, nil, err
	}
	lease := teamCatalogLease{}
	credential, err := resolver.credentials.Resolve(ctx, lease, providerfoundation.TenantScope{
		OrgID: orgID, Provider: provider, CredentialID: credentialID,
	})
	if err != nil {
		return providerfoundation.Credential{}, nil, err
	}
	switch provider {
	case "linear":
		client, err := providerfoundation.NewLinearClient(credential, resolver.doer, resolver.retry, lease)
		return credential, client, err
	default:
		return providerfoundation.Credential{}, nil, errTeamCatalogUnsupportedProvider
	}
}

// teamCatalogSelectionsResolver implements
// syncdispatchruntime.TeamCatalogSelectionsResolver against CHAOS-4323's
// three independent sync_options flags, read from this run's own
// integration's ROOT sync_configurations row (parent_id IS NULL) -- the same
// row and the same three keys native_post_sync.go:567-577 already reads
// (there, only their OR; here, each flag individually).
type teamCatalogSelectionsResolver struct {
	pool *pgxpool.Pool
}

func (resolver teamCatalogSelectionsResolver) ResolveSelections(
	ctx context.Context, orgID, runID, provider string,
) (providersync.TeamCatalogSelections, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	integrationID, _, err := resolveTeamCatalogIntegration(ctx, resolver.pool, orgID, runID, provider)
	if err != nil {
		return providersync.TeamCatalogSelections{}, err
	}
	var teams, projects, members bool
	queryErr := resolver.pool.QueryRow(ctx, `
SELECT
	COALESCE(sync_options->>'auto_import_teams' = 'true', false),
	COALESCE(sync_options->>'auto_import_projects' = 'true', false),
	COALESCE(sync_options->>'auto_import_members' = 'true', false)
FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at, id
LIMIT 1`, orgID, integrationID).Scan(&teams, &projects, &members)
	if errors.Is(queryErr, pgx.ErrNoRows) {
		// No root sync_configurations row at all means every flag is
		// unconfigured -- the OR-gate at native_post_sync.go:577 treats the
		// identical case as "false" via COALESCE; this mirrors that exactly.
		return providersync.TeamCatalogSelections{}, nil
	}
	if queryErr != nil {
		return providersync.TeamCatalogSelections{}, queryErr
	}
	return providersync.TeamCatalogSelections{Teams: teams, Projects: projects, Members: members}, nil
}
