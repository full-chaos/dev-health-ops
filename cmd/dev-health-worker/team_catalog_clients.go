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

// errTeamCatalogCredentialFingerprintMismatch is the distinct, fail-closed
// outcome CHAOS-4431 codex review P1 + team-lead ruling (2026-08-28) require:
// a stamped run's credential_id resolved to DIFFERENT secret content than
// what was witnessed at plan time -- never folded into the generic
// providersync.ErrInvalidConfiguration, so a dashboard/alert can tell "no
// credential at all" apart from "credential content changed mid-run".
var errTeamCatalogCredentialFingerprintMismatch = errors.New("team catalog credential fingerprint mismatch: stamped run auth no longer matches the resolved credential")

// authSourceEnvironment mirrors Python's credentials/fingerprint.py
// AUTH_SOURCE_ENVIRONMENT constant exactly -- the sync_runs.auth_source value
// planner.py:490-523 stamps when Integration.credential_id was NULL at plan
// time (the run authenticates via environment-provided credentials, not a
// stored IntegrationCredential row).
const authSourceEnvironment = "environment"

// errTeamCatalogEnvironmentAuthUnsupported is CHAOS-4431 codex review round
// 3's P1 fix: this Go resolver has no equivalent of sync_bootstrap.py's
// _resolve_env_credentials, so an environment-stamped run fails closed here
// with a distinct, honest error instead of silently falling through to a
// mutable or empty credential_id. Verified safe against live prod traffic
// (team-lead, 2026-08-28: 0 environment-stamped sync_runs in the last 30
// days, 0 NULL-credential integrations) -- fail-closed stands rather than
// building env-credential resolution for an unreached case. A bridge
// fallback for this case specifically (so a future environment-stamped run
// degrades to the Python path instead of erroring) is CHAOS-4198's "bridge
// fallback for environment-stamped auth on native team-catalog routes"
// child, not built here -- this error's text states the fact, it does not
// promise behavior this code does not implement.
var errTeamCatalogEnvironmentAuthUnsupported = errors.New("team catalog native resolver: environment-stamped auth is not supported; run fails closed")

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
//
// CHAOS-4431 codex review P1: which credential_id -- the run's own FROZEN
// stamp (sync_runs.credential_id, CHAOS-2755) or the live, mutable
// integrations.credential_id -- now follows resolve_run_auth's exact branch
// (sync_bootstrap.py:163-216): a run stamped at plan time (auth_source NOT
// NULL) uses ITS OWN credential_id, so a credential rotated mid-run cannot
// change which account this run's native discovery authenticates against,
// same as the claimed units the rest of this run already dispatched. A NULL
// auth_source (legacy/in-flight-at-deploy run) falls back to the mutable
// integrations.credential_id exactly like Python's own fallback branch.
// Deliberately NOT replicated: _verify_stamped_fingerprint's drift warning/
// strict-raise (a stamped credential's content changing mid-run) -- this
// function fails closed the same way a fingerprint check would if the
// stamped id no longer resolves to a live credential (CredentialResolver.
// Resolve errors), just without the explicit mismatch diagnostic. Noted in
// this PR's RISK-NOTES as a bounded, deliberate scope cut.
// stampedFingerprint is empty whenever resolve_run_auth's
// _verify_stamped_fingerprint would be a no-op: an unstamped run (auth_source
// NULL) or a stamped run that simply never recorded a fingerprint. A
// non-empty return means the caller MUST verify it once the credential is
// decrypted (ResolveClient below) -- resolveTeamCatalogIntegration itself
// never sees decrypted credential content, so it cannot compute the witness.
// resolveTeamCatalogIntegrationID looks up the integration row for a sync
// run, independent of credential resolution. ResolveSelections only ever
// needs the integration id (to read sync_configurations/integrations.config
// for CHAOS-4323 selections and the scope fallback) and must not fail just
// because this run has no resolvable credential -- reference_discovery.py
// resolves credentials (resolve_run_auth, line ~273) and sync_options
// (canonical_sync_config_for_sync_run, line ~329) as two fully independent
// calls, neither gated on the other succeeding, and an environment-stamped
// or as-yet-uncredentialed run must still get a selections/scope answer.
// resolveTeamCatalogIntegration (below) additionally resolves+validates a
// credential id and is for ResolveClient's use only.
func resolveTeamCatalogIntegrationID(ctx context.Context, pool *pgxpool.Pool, orgID, runID, provider string) (string, error) {
	if pool == nil || orgID == "" || runID == "" || provider == "" {
		return "", providersync.ErrInvalidConfiguration
	}
	var integrationID string
	queryErr := pool.QueryRow(ctx, `
SELECT integrations.id::text
FROM public.sync_runs
JOIN public.integrations ON integrations.id = sync_runs.integration_id
WHERE sync_runs.id = $1::uuid AND sync_runs.org_id = $2 AND integrations.org_id = $2
  AND lower(trim(integrations.provider)) = $3`,
		runID, orgID, provider).Scan(&integrationID)
	if errors.Is(queryErr, pgx.ErrNoRows) {
		return "", providersync.ErrInvalidConfiguration
	}
	if queryErr != nil {
		return "", queryErr
	}
	return integrationID, nil
}

func resolveTeamCatalogIntegration(
	ctx context.Context, pool *pgxpool.Pool, orgID, runID, provider string,
) (integrationID, credentialID, stampedFingerprint string, err error) {
	if pool == nil || orgID == "" || runID == "" || provider == "" {
		return "", "", "", providersync.ErrInvalidConfiguration
	}
	var authSource *string
	var stampedCredentialID, liveCredentialID, rawFingerprint string
	queryErr := pool.QueryRow(ctx, `
SELECT integrations.id::text, COALESCE(integrations.credential_id::text, ''),
       sync_runs.auth_source, COALESCE(sync_runs.credential_id::text, ''),
       COALESCE(sync_runs.credential_fingerprint, '')
FROM public.sync_runs
JOIN public.integrations ON integrations.id = sync_runs.integration_id
WHERE sync_runs.id = $1::uuid AND sync_runs.org_id = $2 AND integrations.org_id = $2
  AND lower(trim(integrations.provider)) = $3`,
		runID, orgID, provider).Scan(&integrationID, &liveCredentialID, &authSource, &stampedCredentialID, &rawFingerprint)
	if errors.Is(queryErr, pgx.ErrNoRows) {
		return "", "", "", providersync.ErrInvalidConfiguration
	}
	if queryErr != nil {
		return "", "", "", queryErr
	}
	// CHAOS-4431 codex review round 3, P1: auth_source non-NULL means STAMPED
	// regardless of whether credential_id is also NULL -- sync_bootstrap.py:
	// 184-185's `if auth_source is None:` is the ONLY branch point; a NULL
	// credential_id alongside a non-NULL auth_source is planner.py:490-523's
	// environment-auth stamp (Integration.credential_id was NULL at plan
	// time), not "unstamped". An earlier revision here used
	// `stampedCredentialID != ""` as a second, incorrect gate, which made an
	// environment-stamped run fall through to the mutable-credential branch
	// -- wrong account risk if that mutable value is later set, or a hard
	// failure if it never is.
	if authSource != nil && *authSource == authSourceEnvironment {
		// Go has no environment-credential resolution path (unlike
		// sync_bootstrap.py's _resolve_env_credentials) -- fail closed with a
		// distinct, honest error rather than silently using the wrong
		// credential or crashing on an empty one. Strict callers propagate
		// this (same as any other resolver error); the post-sync bridge
		// already degrades ANY resolver error to a non-fatal zero result.
		return "", "", "", fmt.Errorf("%w: integration=%s", errTeamCatalogEnvironmentAuthUnsupported, integrationID)
	}
	credentialID = liveCredentialID
	stamped := authSource != nil
	if stamped {
		credentialID = stampedCredentialID
		stampedFingerprint = rawFingerprint
	}
	if credentialID == "" {
		return "", "", "", providersync.ErrInvalidConfiguration
	}
	return integrationID, credentialID, stampedFingerprint, nil
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
	integrationID, credentialID, stampedFingerprint, err := resolveTeamCatalogIntegration(ctx, resolver.pool, orgID, runID, provider)
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
	// CHAOS-4431 codex review P1 (team-lead ruling, 2026-08-28: "not accepted
	// as a cut... it is the whole point of CHAOS-2755"): a stamped run's
	// frozen credential_id resolving to DIFFERENT secret content than what
	// was stamped at plan time -- an in-place secret edit, not a credential_
	// id repoint (that case is what the stamped credential_id itself already
	// defeats) -- must fail closed with its own distinct error, mirroring
	// _verify_stamped_fingerprint's strict branch (sync_bootstrap.py:123-160).
	// Deliberately always strict here (no SYNC_RUN_AUTH_STRICT warn-and-
	// continue escape hatch): this seam has no retry path that re-resolves a
	// fresh credential the way a claimed provider-unit's next attempt does,
	// so silently continuing on a mismatched witness has no later chance to
	// self-correct.
	if stampedFingerprint != "" {
		if computed := providerfoundation.CredentialFingerprint(credential, credentialID, integrationID); computed != stampedFingerprint {
			return providerfoundation.Credential{}, nil, "", fmt.Errorf("%w: integration=%s", errTeamCatalogCredentialFingerprintMismatch, integrationID)
		}
	}
	switch provider {
	case "linear":
		client, err := providerfoundation.NewLinearClient(credential, resolver.doer, resolver.retry, lease)
		return credential, client, integrationID, err
	case "github":
		client, err := providerfoundation.NewGitHubClient(credential, resolver.doer, resolver.retry, lease)
		return credential, client, integrationID, err
	case "gitlab":
		client, err := providerfoundation.NewGitLabClient(credential, resolver.doer, resolver.retry, lease)
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
	ctx context.Context, orgID, runID, provider string, strict bool,
) (providersync.TeamCatalogSelections, map[string]any, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	integrationID, err := resolveTeamCatalogIntegrationID(ctx, resolver.pool, orgID, runID, provider)
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
		// CHAOS-4431 codex review (4432's round, routed here as base owner):
		// no canonical row means sync_options itself carries nothing
		// trustworthy for CHAOS-4323 selections (handled below, strict vs
		// non-strict), but a collector may still need a provider-specific
		// SCOPE value beyond the resolved credential -- GitLab's group_path,
		// GitHub's org ("owner") fallback. reference_discovery.py:329-333
		// falls back to `dict(integration.config or {})` for EXACTLY that
		// reason; its own comment says this is kept ONLY for that fallback,
		// never trusted for auto_import_* category resolution (which is why
		// selections below stay governed purely by strict/non-strict, not by
		// this map's content). Integration.config is stale for selections
		// per chris -- never read auto_import_* out of it.
		fallbackConfig, configErr := resolveIntegrationConfigFallback(ctx, resolver.pool, integrationID)
		if configErr != nil {
			return providersync.TeamCatalogSelections{}, nil, configErr
		}
		if strict {
			// CHAOS-4431 codex review round 2, P2: no canonical
			// sync_configurations row at all is NOT the same case as an
			// existing row with every flag explicitly off. Strict reference
			// discovery must default to UNRESTRICTED here
			// (reference_discovery.py:329-354's sync_options_is_canonical
			// flag + team_autoimport.py:206-237, itself a prior CHAOS-4437
			// codex-review fix) so a legacy/no-config integration keeps
			// importing everything, matching pre-CHAOS-4323 behavior,
			// instead of silently going to "everything off".
			return providersync.TeamCatalogSelections{Teams: true, Projects: true, Members: true}, fallbackConfig, nil
		}
		// Non-strict (post-sync): every flag unconfigured -- the OR-gate at
		// native_post_sync.go:577 treats the identical case as "false" via
		// COALESCE; this mirrors that exactly.
		return providersync.TeamCatalogSelections{}, fallbackConfig, nil
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

// resolveIntegrationConfigFallback reads an integration's own `config`
// column (jsonb, NOT NULL DEFAULT '{}') -- used ONLY when no canonical
// sync_configurations row exists, mirroring reference_discovery.py:329-333's
// `dict(integration.config or {})` fallback. That Python comment is explicit
// that this value is trustworthy ONLY for a provider-specific scope
// fallback (GitHub's "owner", GitLab's group_path) -- never for CHAOS-4323
// category resolution, which is why the selections decision in
// ResolveSelections never reads this map at all.
func resolveIntegrationConfigFallback(ctx context.Context, pool *pgxpool.Pool, integrationID string) (map[string]any, error) {
	var configJSON []byte
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(config, '{}'::jsonb)
FROM public.integrations
WHERE id = $1::uuid`, integrationID).Scan(&configJSON); err != nil {
		return nil, err
	}
	var config map[string]any
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("decode integration config fallback: %w", err)
	}
	return config, nil
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
			selections, syncOptions, selectionsErr := bridge.selections.ResolveSelections(ctx, orgID, runID, provider, false)
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
			if result.Skipped {
				// CHAOS-4432 (team-lead ruling, 2026-08-28): a collector
				// that made NO writes and is reporting a clean, successful
				// zero result (Python parity for a non-strict walk
				// failure) must record a dedicated skipped outcome, not
				// "native" -- a zero-row success must never be silently
				// indistinguishable from a real, healthy zero-row run.
				// TeamCatalogOutcomeCollectorSkipped (codex review finding,
				// distinct from TeamCatalogOutcomeSkipped above, which
				// means "nothing selected, the collector never ran" --
				// this means the collector WAS called and chose to skip):
				// conflating the two would make dev_health_team_catalog_
				// dispatch_total unable to tell "nothing configured" apart
				// from "this provider's fetch is failing". result.
				// SkipReason (e.g. "group_projects_fetch_failed") is not
				// yet a metric label, but is warn-logged by the collector
				// itself at the point of failure.
				bridge.observeDispatch(provider, jobruntime.TeamCatalogOutcomeCollectorSkipped)
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
					{"sprints", result.SprintsWritten},
					{"projects_without_key", result.ProjectsWithoutKey},
					{"teams_skipped_policy", result.TeamsSkippedPolicy},
					{"team_memberships_skipped_manual_conflict", result.MembershipsSkippedManualConflict},
					{"teams_staged_for_review", result.TeamsStagedForReview},
					{"team_memberships_staged_for_review", result.MembershipsStagedForReview},
					{"team_drift_changes_superseded", result.DriftChangesSuperseded},
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
