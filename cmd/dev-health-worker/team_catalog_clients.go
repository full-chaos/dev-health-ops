package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// resolveTeamCatalogProvider ports the SAME sync_runs-JOIN-integrations
// lookup native_reference_discovery.go's resolveAuthoritativeProvider uses,
// without a provider to fence by -- the post-sync team-autoimport dispatch
// (below) needs to discover which provider a sync run belongs to BEFORE it
// can decide native vs bridge; resolveTeamCatalogIntegration above requires
// already knowing it.
func resolveTeamCatalogProvider(ctx context.Context, pool *pgxpool.Pool, orgID, runID string) (string, error) {
	if pool == nil || orgID == "" || runID == "" {
		return "", providersync.ErrInvalidConfiguration
	}
	var provider string
	err := pool.QueryRow(ctx, `
SELECT lower(trim(integrations.provider))
FROM public.sync_runs
JOIN public.integrations ON integrations.id = sync_runs.integration_id
WHERE sync_runs.id = $1::uuid AND sync_runs.org_id = $2 AND integrations.org_id = $2`,
		runID, orgID).Scan(&provider)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", providersync.ErrInvalidConfiguration
	}
	if err != nil {
		return "", err
	}
	return provider, nil
}

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
) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	integrationID, credentialID, err := resolveTeamCatalogIntegration(ctx, resolver.pool, orgID, runID, provider)
	if err != nil {
		return providerfoundation.Credential{}, nil, "", err
	}
	lease := teamCatalogLease{}
	// IntegrationID is required: TenantScope.Validate() fails closed
	// ("invalid provider tenant scope") without it -- confirmed via local
	// readback, org 70d529e0, CHAOS-4431.
	credential, err := resolver.credentials.Resolve(ctx, lease, providerfoundation.TenantScope{
		OrgID: orgID, Provider: provider, IntegrationID: integrationID, CredentialID: credentialID,
	})
	if err != nil {
		return providerfoundation.Credential{}, nil, "", err
	}
	switch provider {
	case "linear":
		client, err := providerfoundation.NewLinearClient(credential, resolver.doer, resolver.retry, lease)
		return credential, client, integrationID, err
	default:
		return providerfoundation.Credential{}, nil, "", errTeamCatalogUnsupportedProvider
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
) (providersync.TeamCatalogSelections, map[string]any, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	integrationID, _, err := resolveTeamCatalogIntegration(ctx, resolver.pool, orgID, runID, provider)
	if err != nil {
		return providersync.TeamCatalogSelections{}, nil, err
	}
	// One round trip for both the CHAOS-4323 selections and the raw
	// sync_options a collector may need beyond the resolved credential
	// (team-lead ruling, 2026-08-28) -- same canonical root row
	// native_post_sync.go:567-577 already reads (there, only the OR of the
	// three flags; here, each flag individually plus the full map).
	// sync_configurations.sync_options is `json`, NOT `jsonb` -- a COALESCE
	// default cast as ::jsonb fails closed with "COALESCE could not convert
	// type jsonb to json" (SQLSTATE 42846; confirmed via local readback,
	// org 70d529e0, CHAOS-4431). The column is also NOT NULL with a
	// '{}'::json default already, so COALESCE is defensive, not load-bearing.
	var syncOptionsJSON []byte
	queryErr := resolver.pool.QueryRow(ctx, `
SELECT COALESCE(sync_options, '{}'::json)
FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at, id
LIMIT 1`, orgID, integrationID).Scan(&syncOptionsJSON)
	if errors.Is(queryErr, pgx.ErrNoRows) {
		// No root sync_configurations row at all means every flag is
		// unconfigured -- the OR-gate at native_post_sync.go:577 treats the
		// identical case as "false" via COALESCE; this mirrors that exactly.
		return providersync.TeamCatalogSelections{}, nil, nil
	}
	if queryErr != nil {
		return providersync.TeamCatalogSelections{}, nil, queryErr
	}
	var syncOptions map[string]any
	if err := json.Unmarshal(syncOptionsJSON, &syncOptions); err != nil {
		return providersync.TeamCatalogSelections{}, nil, fmt.Errorf("decode sync_options: %w", err)
	}
	selections := providersync.TeamCatalogSelections{
		Teams:    syncOptionBool(syncOptions, "auto_import_teams"),
		Projects: syncOptionBool(syncOptions, "auto_import_projects"),
		Members:  syncOptionBool(syncOptions, "auto_import_members"),
	}
	return selections, syncOptions, nil
}

// syncOptionBool reads a boolean flag out of a decoded sync_options map,
// matching the SQL COALESCE(sync_options->>'key' = 'true', false) semantics
// this resolver used before moving to a single round trip: an absent key,
// a non-bool value, or a false value are all simply "false".
func syncOptionBool(syncOptions map[string]any, key string) bool {
	value, ok := syncOptions[key].(bool)
	return ok && value
}

// teamCatalogAutoimportBridge decorates a syncdispatchruntime.CoordinatorBridge:
// a sync run whose OWN provider has a registered native collector runs that
// collector directly (gated by CHAOS-4323 selections, mirroring Python's
// non-strict run_team_autoimport -- UNLIKE TeamCatalogDiscoveryExecutor
// above, which mirrors the selection-blind run_team_autoimport_strict used
// by the separate reference-discovery seam) and never calls TeamAutoImport
// on the wrapped bridge at all. Every other provider, and any resolution
// failure, falls through to the wrapped bridge unchanged -- Dispatch/
// Finalize/Discover are untouched pass-throughs via the embedded interface.
type teamCatalogAutoimportBridge struct {
	syncdispatchruntime.CoordinatorBridge
	// resolveProvider discovers the sync run's own provider; production
	// wiring sets this to a closure over resolveTeamCatalogProvider + the
	// domain pool, tests inject a fake directly.
	resolveProvider func(ctx context.Context, orgID, runID string) (string, error)
	native          map[string]providersync.TeamCatalogCollector
	clients         syncdispatchruntime.ProviderClientResolver
	selections      syncdispatchruntime.TeamCatalogSelectionsResolver
	// sources is optional: a collector that does not need run-scoped source
	// ids (Linear today) is unaffected by its absence.
	sources syncdispatchruntime.SourceExternalIDsResolver
	// observer is optional: a nil observer records nothing, the same
	// convention every other telemetry hook in this codebase uses.
	observer jobruntime.TeamCatalogObserver
	now      func() time.Time
}

func (bridge *teamCatalogAutoimportBridge) nowUTC() time.Time {
	if bridge.now != nil {
		return bridge.now().UTC()
	}
	return time.Now().UTC()
}

func (bridge *teamCatalogAutoimportBridge) observeDispatch(provider string, outcome jobruntime.TeamCatalogOutcome) {
	if bridge.observer == nil {
		return
	}
	_ = bridge.observer.ObserveTeamCatalogDispatch(provider, jobruntime.TeamCatalogEntryPointPostSync, outcome)
}

func (bridge *teamCatalogAutoimportBridge) TeamAutoImport(
	ctx context.Context, reference syncdispatchruntime.DomainReference,
) error {
	if bridge == nil || bridge.CoordinatorBridge == nil || bridge.resolveProvider == nil {
		return syncdispatchruntime.ErrInvalidBridge
	}
	orgID, runID := reference.OrganizationID, reference.SyncRunID
	provider, err := bridge.resolveProvider(ctx, orgID, runID)
	if err == nil {
		if collector, ok := bridge.native[provider]; ok {
			// Non-strict (team-lead ruling, 2026-08-28): mirrors Python's
			// run_team_autoimport, which catches every populator exception --
			// including auth/config resolution failures, not only the
			// populate call itself -- and returns a zero summary rather than
			// failing the job. EVERY error from this point on (selections,
			// credential/client, source ids, or the collection call itself)
			// must degrade the same way, or a resolver blip still causes a
			// retry storm exactly like an un-degraded collector error would.
			// The strict reference-discovery seam (TeamCatalogDiscoveryExecutor)
			// has no such decorator and keeps propagating every one of these.
			selections, syncOptions, selectionsErr := bridge.selections.ResolveSelections(ctx, orgID, runID, provider)
			if selectionsErr != nil {
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
				return nil
			}
			if !selections.Any() {
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeSkipped)
				return nil
			}
			credential, client, integrationID, clientErr := bridge.clients.ResolveClient(ctx, orgID, runID, provider)
			if clientErr != nil {
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
				return nil
			}
			var sourceExternalIDs []string
			if bridge.sources != nil {
				var sourcesErr error
				sourceExternalIDs, sourcesErr = bridge.sources.ResolveSourceExternalIDs(ctx, orgID, runID)
				if sourcesErr != nil {
					bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
					return nil
				}
			}
			result, collectErr := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{
				OrgID: orgID, SyncRunID: runID, IntegrationID: integrationID,
				SyncOptions: syncOptions, Strict: false, SourceExternalIDs: sourceExternalIDs,
			}, credential, client, selections, bridge.nowUTC())
			if collectErr != nil {
				// The failure is still visible via the dedicated nonfatal
				// outcome, not silently dropped.
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
				return nil
			}
			if result.RosterPreservationFailed {
				// A collector may choose to continue after its own
				// existing-members pre-read failed rather than hard-failing
				// the whole write (Linear's collector does not -- it always
				// hard-fails instead, see CHAOS-4446). This outcome exists so
				// that choice, if any collector ever makes it, is visible in
				// telemetry rather than indistinguishable from a clean run.
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeRosterPreservationFailed)
			} else {
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeNative)
			}
			if bridge.observer != nil {
				for _, row := range []struct {
					table string
					count int
				}{
					{"teams", result.TeamsWritten}, {"members", result.MembersWritten},
					{"team_memberships", result.MembershipsWritten}, {"projects", result.ProjectsWritten},
					{"team_project_ownership", result.OwnershipWritten},
					{"team_repo_ownership", result.RepoOwnershipWritten},
				} {
					_ = bridge.observer.ObserveTeamCatalogRowsWritten(provider, jobruntime.TeamCatalogTable(row.table), row.count)
				}
			}
			return nil
		}
	}
	// Provider resolution failed, or the provider is not native: fall back
	// to the Python path exactly as before CHAOS-4431.
	bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeBridge)
	return bridge.CoordinatorBridge.TeamAutoImport(ctx, reference)
}

var _ syncdispatchruntime.CoordinatorBridge = &teamCatalogAutoimportBridge{}

// teamCatalogSourceResolver implements
// syncdispatchruntime.SourceExternalIDsResolver against the SAME
// sync_run_units-JOIN-integration_sources join
// src/dev_health_ops/workers/reference_discovery.py:281-303 uses to build
// scope["source_external_ids"], including its fail-closed behavior: a run
// whose sync_run_units references a source_id with no resolvable
// integration_sources.external_id is a data-integrity gap, not a
// "just skip it" case, so this returns an error instead of a partial list.
type teamCatalogSourceResolver struct {
	pool *pgxpool.Pool
}

func (resolver teamCatalogSourceResolver) ResolveSourceExternalIDs(
	ctx context.Context, orgID, runID string,
) ([]string, error) {
	if resolver.pool == nil || orgID == "" || runID == "" {
		return nil, providersync.ErrInvalidConfiguration
	}
	rows, err := resolver.pool.Query(ctx, `
SELECT DISTINCT sru.source_id::text, isrc.external_id
FROM public.sync_run_units sru
LEFT JOIN public.integration_sources isrc ON isrc.id = sru.source_id
WHERE sru.sync_run_id = $1::uuid`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	externalIDs := make(map[string]struct{})
	var unresolved []string
	for rows.Next() {
		var sourceID string
		var externalID *string
		if err := rows.Scan(&sourceID, &externalID); err != nil {
			return nil, err
		}
		if externalID == nil || strings.TrimSpace(*externalID) == "" {
			unresolved = append(unresolved, sourceID)
			continue
		}
		externalIDs[strings.TrimSpace(*externalID)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(unresolved) > 0 {
		sort.Strings(unresolved)
		return nil, fmt.Errorf(
			"%w: reference discovery source inventory incomplete: unresolved_source_ids=%v",
			providersync.ErrInvalidConfiguration, unresolved,
		)
	}
	result := make([]string, 0, len(externalIDs))
	for id := range externalIDs {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

var _ syncdispatchruntime.SourceExternalIDsResolver = teamCatalogSourceResolver{}
