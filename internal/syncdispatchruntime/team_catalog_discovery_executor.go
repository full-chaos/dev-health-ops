package syncdispatchruntime

import (
	"context"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// ProviderClientResolver resolves the credential + HTTP client pair a native
// team-catalog collector needs, the same way
// cmd/dev-health-worker/provider_sync.go already builds them for a claimed
// provider-unit -- minus the claim and lease, which this seam never has
// (CHAOS-4431 ruling, team-lead 2026-08-28, option (c)). runID is required
// (not just orgID+provider): an org can have more than one active
// integration for the same provider (observed locally for linear, org
// 70d529e0), so which credential governs is resolved from THIS sync run's
// own integration_id -- the same join resolveAuthoritativeProvider already
// uses (native_reference_discovery.go) -- never "the" active one for the pair.
// ResolveClient also returns the sync run's own integration_id (team-lead
// ruling, 2026-08-28) -- the same value resolveTeamCatalogIntegration
// already computes internally to pin the credential, returned here so the
// caller can populate TeamCatalogReference.IntegrationID without a second
// resolution.
type ProviderClientResolver interface {
	ResolveClient(ctx context.Context, orgID, runID, provider string) (credential providerfoundation.Credential, client *providerfoundation.HTTPClient, integrationID string, err error)
}

// TeamCatalogSelectionsResolver reads CHAOS-4323's three independent
// sync_configurations flags (auto_import_teams/auto_import_projects/
// auto_import_members) for this sync run's own integration, for the same
// multiple-active-integrations reason ProviderClientResolver's doc comment
// explains. Used by BOTH the post-sync team-autoimport dispatcher and
// TeamCatalogDiscoveryExecutor below.
//
// An earlier revision of this file exempted the reference-discovery path
// from selection gating, reasoning that Python's run_team_autoimport_strict
// never consults sync_options and reference discovery mirrors it. That
// reasoning was wrong: lane-4430 proved (CHAOS-4437, red test on
// team-item-selection-gate-proof) that run_team_autoimport_strict ignoring
// selections is itself a DEFECT -- chris's CHAOS-4323 rule is that each of
// the three items is independently selectable, unselected means not
// written, full stop. Parity with a known defect is not the spec. Both
// native paths gate on this resolver; CHAOS-4437 (a separate lane) is fixing
// the Python side to match.
// ResolveSelections also returns the run's raw sync_options map (team-lead
// ruling, 2026-08-28): a collector may need a provider-specific config value
// beyond the resolved credential (GitLab's group_path, GitHub's org
// fallback -- credential.Config carries only auth material, never scope).
// Both values come from the SAME canonical sync_configurations row, read in
// one round trip, never two separate queries for one Discover/TeamAutoImport
// call.
type TeamCatalogSelectionsResolver interface {
	ResolveSelections(ctx context.Context, orgID, runID, provider string) (selections providersync.TeamCatalogSelections, syncOptions map[string]any, err error)
}

// TeamCatalogDiscoveryExecutor dispatches reference discovery per provider:
// a provider with a registered native collector runs it directly (gated by
// CHAOS-4323 selections, same as the post-sync path) and skips the Python
// bridge entirely; every other provider falls through to Fallback, the
// existing BridgeDiscoveryExecutor. It implements the same DiscoveryExecutor
// seam VerifiedDiscoveryExecutor already wraps, so ClickHouse readback
// verification covers native and bridge providers alike.
//
// When every selection is off, this returns success with NO
// "reference_team_keys"/"reference_sprint_ids" keys at all (not empty
// slices under those keys -- their absence), per lane-4437's executed
// answer (native_dispatch_sync_run_service.go:242-253 gates unit dispatch
// only on the discovery ledger reaching status=success; that status is
// earned by ReferenceReadbackVerifier.Verify, clickhouse_readback.go:228-236,
// which is a no-op -- and therefore trivially succeeds -- exactly when the
// summary claims no keys at all). So an all-off native provider still lets
// dispatch proceed, with zero ClickHouse writes and zero bridge calls.
type TeamCatalogDiscoveryExecutor struct {
	Native     map[string]providersync.TeamCatalogCollector
	Fallback   DiscoveryExecutor
	Clients    ProviderClientResolver
	Selections TeamCatalogSelectionsResolver
	// Observer is optional: a nil Observer records nothing, the same
	// convention every other telemetry hook in this codebase uses.
	Observer jobruntime.TeamCatalogObserver
	Now      func() time.Time
}

func (executor *TeamCatalogDiscoveryExecutor) observeDispatch(provider string, outcome jobruntime.TeamCatalogOutcome) {
	if executor.Observer == nil {
		return
	}
	_ = executor.Observer.ObserveTeamCatalogDispatch(provider, jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome)
}

func (executor *TeamCatalogDiscoveryExecutor) now() time.Time {
	if executor.Now != nil {
		return executor.Now().UTC()
	}
	return time.Now().UTC()
}

func (executor *TeamCatalogDiscoveryExecutor) Discover(
	ctx context.Context, orgID, runID, provider string,
) (map[string]any, error) {
	if executor == nil || ctx == nil || orgID == "" || runID == "" || provider == "" {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	normalizedProvider := strings.ToLower(strings.TrimSpace(provider))
	collector, native := executor.Native[normalizedProvider]
	if !native {
		if executor.Fallback == nil {
			return nil, ErrReferenceDiscoveryUnavailable
		}
		executor.observeDispatch(normalizedProvider, jobruntime.TeamCatalogOutcomeBridge)
		return executor.Fallback.Discover(ctx, orgID, runID, provider)
	}
	if executor.Clients == nil || executor.Selections == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	selections, syncOptions, err := executor.Selections.ResolveSelections(ctx, orgID, runID, normalizedProvider)
	if err != nil {
		return nil, err
	}
	if !selections.Any() {
		// Nothing selected: no collector call, no bridge call, no CH writes.
		// Deliberately omit reference_team_keys/reference_sprint_ids (not an
		// empty slice under those keys -- their absence) so
		// ReferenceReadbackVerifier.Verify no-ops and the discovery ledger
		// still reaches status=success, letting unit dispatch proceed.
		executor.observeDispatch(normalizedProvider, jobruntime.TeamCatalogOutcomeSkipped)
		return map[string]any{"provider": normalizedProvider, "outcome": "skipped"}, nil
	}
	credential, client, integrationID, err := executor.Clients.ResolveClient(ctx, orgID, runID, normalizedProvider)
	if err != nil {
		return nil, err
	}
	result, err := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{
		OrgID: orgID, SyncRunID: runID, IntegrationID: integrationID,
		SyncOptions: syncOptions, Strict: true,
	}, credential, client, selections, executor.now())
	if err != nil {
		return nil, err
	}
	executor.observeDispatch(normalizedProvider, jobruntime.TeamCatalogOutcomeNative)
	if executor.Observer != nil {
		for _, row := range []struct {
			table string
			count int
		}{
			{"teams", result.TeamsWritten}, {"members", result.MembersWritten},
			{"team_memberships", result.MembershipsWritten}, {"projects", result.ProjectsWritten},
			{"team_project_ownership", result.OwnershipWritten},
		} {
			_ = executor.Observer.ObserveTeamCatalogRowsWritten(normalizedProvider, jobruntime.TeamCatalogTable(row.table), row.count)
		}
	}
	return map[string]any{
		"provider":            normalizedProvider,
		"outcome":             "native",
		"reference_team_keys": result.TeamKeys,
		"rows_written": map[string]int{
			"teams":                  result.TeamsWritten,
			"members":                result.MembersWritten,
			"team_memberships":       result.MembershipsWritten,
			"projects":               result.ProjectsWritten,
			"team_project_ownership": result.OwnershipWritten,
		},
	}, nil
}

var _ DiscoveryExecutor = &TeamCatalogDiscoveryExecutor{}
