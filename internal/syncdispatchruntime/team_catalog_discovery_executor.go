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
// strict distinguishes which Python early-exit default applies when NO
// canonical sync_configurations row exists for this integration at all
// (CHAOS-4431 codex review round 2, P2): reference_discovery.py:329-354 +
// team_autoimport.py:206-237's sync_options_is_canonical flag make STRICT
// discovery default to UNRESTRICTED (every category True) in that case --
// added by a prior CHAOS-4437 codex review specifically so a legacy/no-
// config integration keeps its pre-CHAOS-4323 "import everything" behavior
// instead of silently going to "everything off" -- while non-strict post-
// sync dispatch keeps its existing all-false default (matches
// native_post_sync.go:567-577's OR-gate).
type TeamCatalogSelectionsResolver interface {
	ResolveSelections(ctx context.Context, orgID, runID, provider string, strict bool) (selections providersync.TeamCatalogSelections, syncOptions map[string]any, err error)
}

// SourceExternalIDsResolver reads this sync run's own provider-source
// external id set (team-lead ruling, 2026-08-28), the same
// sync_run_units-JOIN-integration_sources join reference_discovery.py:
// 281-303 uses to build scope["source_external_ids"] -- including its
// fail-closed behavior when a run's source_id has no resolvable
// external_id. A collector that must scope its walk to the run's selected
// sources (e.g. GitLab's project catalog) reads
// TeamCatalogReference.SourceExternalIDs instead of querying sync_run_units
// itself; collectors stay DB-free.
type SourceExternalIDsResolver interface {
	ResolveSourceExternalIDs(ctx context.Context, orgID, runID string) ([]string, error)
}

// TeamCatalogDiscoveryExecutor dispatches reference discovery per provider:
// a provider with a registered native collector runs it directly (gated by
// CHAOS-4323 selections, same as the post-sync path) and skips the Python
// bridge entirely; every other provider falls through to Fallback, the
// existing BridgeDiscoveryExecutor. It implements the same DiscoveryExecutor
// seam VerifiedDiscoveryExecutor already wraps, so ClickHouse readback
// verification covers native and bridge providers alike.
//
// When every CHAOS-4323 selection is off, a native provider still dispatches
// (never blocks unit dispatch): teams/members/projects are not written, but
// -- corrected after codex review of this PR found a second-order gap in the
// original design here, independently converging with CHAOS-4437's Python
// fix -- sprint/cycle reference discovery is UNCONDITIONAL in strict mode
// (team_autoimport_linear.py:421's early exit only applies when NOT strict),
// so this executor always reaches the collector and "reference_sprint_ids"
// is populated from whatever cycles exist, even with every other category
// off. ReferenceReadbackVerifier.Verify (clickhouse_readback.go:228-253)
// checks each claimed key set independently by length, not by presence, so
// an empty "reference_team_keys" alongside a non-empty "reference_sprint_ids"
// still verifies correctly -- teams are vacuously satisfied, sprints are
// checked for real.
type TeamCatalogDiscoveryExecutor struct {
	Native     map[string]providersync.TeamCatalogCollector
	Fallback   DiscoveryExecutor
	Clients    ProviderClientResolver
	Selections TeamCatalogSelectionsResolver
	Sources    SourceExternalIDsResolver
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
	selections, syncOptions, err := executor.Selections.ResolveSelections(ctx, orgID, runID, normalizedProvider, true)
	if err != nil {
		return nil, err
	}
	// Unlike the non-strict post-sync bridge (teamCatalogAutoimportBridge),
	// this seam NEVER skips the collector call outright when every
	// CHAOS-4323 selection is off. Sprints/cycles are unconditional
	// reference data in Python's STRICT mode -- team_autoimport_linear.py:
	// 421's early exit explicitly does not apply when strict, specifically
	// so dispatch-blocking sprint keys are resolved even when an org
	// disabled every writable category (CHAOS-4437 P1; codex review of
	// this PR independently re-found the same gap in this executor before
	// that Python fix's rationale was even known here). Ref.Strict is
	// always true below, so the collector decides on its own whether to
	// skip -- see LinearTeamCatalogCollector.CollectTeamCatalog's doc
	// comment -- this function never special-cases "all off" itself.
	credential, client, integrationID, err := executor.Clients.ResolveClient(ctx, orgID, runID, normalizedProvider)
	if err != nil {
		return nil, err
	}
	// Sources is optional: a collector that does not need run-scoped source
	// ids (Linear today) is unaffected by its absence; one that does
	// (GitLab's project catalog) requires it wired in production.
	var sourceExternalIDs []string
	if executor.Sources != nil {
		sourceExternalIDs, err = executor.Sources.ResolveSourceExternalIDs(ctx, orgID, runID)
		if err != nil {
			return nil, err
		}
	}
	result, err := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{
		OrgID: orgID, SyncRunID: runID, IntegrationID: integrationID,
		SyncOptions: syncOptions, Strict: true, SourceExternalIDs: sourceExternalIDs,
	}, credential, client, selections, executor.now())
	if err != nil {
		return nil, err
	}
	if result.RosterPreservationFailed {
		// See providersync.TeamCatalogResult.RosterPreservationFailed's doc
		// comment: no collector sets this today, but the branch exists so a
		// future one's deliberate soft-fail is never silently indistinguishable
		// from a clean run.
		executor.observeDispatch(normalizedProvider, jobruntime.TeamCatalogOutcomeRosterPreservationFailed)
	} else {
		executor.observeDispatch(normalizedProvider, jobruntime.TeamCatalogOutcomeNative)
	}
	if executor.Observer != nil {
		for _, row := range []struct {
			table string
			count int
		}{
			{"teams", result.TeamsWritten}, {"members", result.MembersWritten},
			{"team_memberships", result.MembershipsWritten}, {"projects", result.ProjectsWritten},
			{"team_project_ownership", result.OwnershipWritten},
			{"team_repo_ownership", result.RepoOwnershipWritten},
			{"sprints", result.SprintsWritten},
			{"teams_skipped_policy", result.TeamsSkippedPolicy},
			{"team_memberships_skipped_manual_conflict", result.MembershipsSkippedManualConflict},
			{"teams_staged_for_review", result.TeamsStagedForReview},
			{"team_memberships_staged_for_review", result.MembershipsStagedForReview},
			{"team_drift_changes_superseded", result.DriftChangesSuperseded},
		} {
			_ = executor.Observer.ObserveTeamCatalogRowsWritten(normalizedProvider, jobruntime.TeamCatalogTable(row.table), row.count)
		}
	}
	return map[string]any{
		"provider":             normalizedProvider,
		"outcome":              "native",
		"reference_team_keys":  result.TeamKeys,
		"reference_sprint_ids": result.SprintIDs,
		"rows_written": map[string]int{
			"teams":                  result.TeamsWritten,
			"members":                result.MembersWritten,
			"team_memberships":       result.MembershipsWritten,
			"projects":               result.ProjectsWritten,
			"team_project_ownership": result.OwnershipWritten,
			"sprints":                result.SprintsWritten,
			"teams_skipped_policy":   result.TeamsSkippedPolicy,
			"team_memberships_skipped_manual_conflict": result.MembershipsSkippedManualConflict,
			"teams_staged_for_review":                  result.TeamsStagedForReview,
			"team_memberships_staged_for_review":       result.MembershipsStagedForReview,
			"team_drift_changes_superseded":            result.DriftChangesSuperseded,
		},
	}, nil
}

var _ DiscoveryExecutor = &TeamCatalogDiscoveryExecutor{}
