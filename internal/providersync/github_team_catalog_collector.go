package providersync

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// github_team_catalog_adapter.go adapts githubTeamCatalogCollect (the REST
// walk) and GitHubTeamCatalogClickHouseEffects (the write) to the shared,
// claim-free providersync.TeamCatalogCollector seam (CHAOS-4431/CHAOS-4434,
// team-lead ruling 2026-08-28, option (c)) -- the same shape
// LinearTeamCatalogCollector already uses. GitHub has no "Projects" import
// concept at all (auto_import_capabilities("github").projects is always
// False in Python): selections.Projects is read but never produces a
// ProjectsWritten row.
//
// team_repo_ownership rows are reported under TeamCatalogResult.
// RepoOwnershipWritten (CHAOS-4431 added this dedicated field, team-lead
// ruling 2026-08-28) -- a distinct destination table from Linear/GitLab's
// team_project_ownership (OwnershipWritten), so the two never share a
// telemetry label.
//
// Telemetry is generic, not per-provider: the caller (syncdispatchruntime.
// TeamCatalogDiscoveryExecutor, or internal/workerservice's
// nativeTeamAutoimportDispatcher) observes dispatch outcome and
// rows-written-per-table from this method's own return value via
// jobruntime.TeamCatalogObserver -- no bespoke GitHub-specific Observer
// field needed here.
type GitHubTeamCatalogCollector struct {
	Client GitHubTeamCatalogRouteHandler
	Sink   GitHubTeamCatalogClickHouseEffects
	// ScopeCensus counts the org's other active GitHub integrations. Without
	// it no provider_access row is closed (decideOwnershipClose).
	ScopeCensus OwnershipScopeCensus
}

// githubOrgNameConfigKeys mirrors team_autoimport_github.py's _github_org
// fallback order (credentials["org"|"organization"|"org_name"|"owner"], then
// scope.sync_options[same]) exactly: the credential's own fields (both its
// unencrypted Config -- the web form's non-secret "Organization / Owner"
// field, ProviderForms.tsx GitHubForm -- and its encrypted fields, since it
// is not yet established which column this deployment actually persisted
// "org" into) are checked FIRST, then ref.SyncOptions (this run's own
// sync_configurations.sync_options, CHAOS-4431's SyncOptions addition) as
// the fallback -- matching Python's own credentials-before-scope order.
var githubOrgNameConfigKeys = []string{"org", "organization", "org_name", "owner"}

func githubOrgNameFromCredential(credential providerfoundation.Credential) string {
	for _, key := range githubOrgNameConfigKeys {
		if value := strings.TrimSpace(credential.Config[key]); value != "" {
			return value
		}
	}
	for _, key := range githubOrgNameConfigKeys {
		if value, ok := credential.Secret(key); ok && value.Configured() {
			if trimmed := strings.TrimSpace(value.Reveal()); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func githubOrgNameFromSyncOptions(syncOptions map[string]any) string {
	for _, key := range githubOrgNameConfigKeys {
		if raw, ok := syncOptions[key]; ok {
			if value, ok := raw.(string); ok {
				if trimmed := strings.TrimSpace(value); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func (adapter GitHubTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if ctx == nil {
		return TeamCatalogResult{}, nil
	}
	// Mirrors LinearTeamCatalogCollector's gate exactly (CHAOS-4431 codex
	// review round 2): a non-strict caller with nothing selected is a clean
	// skip, never an error -- only strict (reference discovery, which the
	// selections resolver now defaults to "everything on" when no canonical
	// config row exists) is expected to always have something selected.
	if !ref.Strict && !selections.Any() {
		return TeamCatalogResult{}, nil
	}
	// codex review round 1, P1 (team-lead ruling 2026-08-28): GitHub has no
	// unconditional reference surface and no "Projects" import concept at
	// all -- with neither Teams nor Members selected (e.g. a strict call
	// where only Projects ended up selected), there is nothing this
	// collector can do. This must be checked BEFORE resolving credentials,
	// the org name, or touching ClickHouse -- an earlier revision reached
	// org-name resolution first and, under strict, hard-errored on a
	// legitimately-disabled catalog instead of skipping it cleanly, exactly
	// like Python's no_categories_selected early return
	// (team_autoimport_github.py:81-82) does before ever touching
	// credentials.
	if !selections.Teams && !selections.Members {
		return TeamCatalogResult{}, nil
	}
	if ref.validate() != nil || credential.Provider != "github" || client == nil || client.Provider != "github" {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	if adapter.Sink.Conn == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	orgName := githubOrgNameFromCredential(credential)
	if orgName == "" {
		orgName = githubOrgNameFromSyncOptions(ref.SyncOptions)
	}
	if orgName == "" {
		if ref.Strict {
			// Matches Python's _populate_async under strict_reference_discovery:
			// "raise ValueError(missing GitHub credentials or org for strict
			// reference discovery)" -- a strict caller (reference discovery)
			// must see this as a real failure, never a silent zero result.
			return TeamCatalogResult{}, ErrInvalidConfiguration
		}
		// Non-strict (post-sync): a missing org is a skip (zero summary),
		// never a hard error -- an org can have GitHub connected with no org
		// name set yet.
		return TeamCatalogResult{}, nil
	}

	collector := adapter.Client
	collector.Client = client
	collector.OrgName = orgName
	collector.Strict = ref.Strict
	if collector.Now == nil {
		collector.Now = func() time.Time { return normalizedAt }
	}
	rows, _, err := collector.Collect(ctx, ref.OrgID, selections.Teams, selections.Members)
	if err != nil {
		return TeamCatalogResult{}, err
	}

	// The membership-conflict guard runs before the memberships write below
	// (a membership it rejects is never written). Computed once, up front.
	var keptMemberships []githubMembershipRow
	var membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded int
	if selections.Members {
		// CHAOS-4444 / codex review round 1, P2: rows.Teams is only ever
		// populated `if wantTeams` (githubTeamCatalogRows.Teams's own doc
		// comment), so deriving observed scopes from it undercounts to
		// EMPTY whenever a run selects Members without Teams -- a Members-
		// only run would then never resolve any stale pending identity
		// change, since no scope would ever be "observed". Collect uses
		// ObservedMembershipTeamIDs instead, populated whenever a team's
		// member fetch succeeds, independent of wantTeams.
		var err error
		keptMemberships, membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded, err = applyGitHubTeamMembershipConflictGuard(
			ctx, adapter.Sink.Conn, ref.OrgID, rows.Memberships, rows.ObservedMembershipTeamIDs, normalizedAt,
		)
		if err != nil {
			return TeamCatalogResult{}, err
		}
	}

	result := TeamCatalogResult{}
	if selections.Teams && len(rows.Teams) > 0 {
		// CHAOS-4431 codex review findings #3/#6, team-lead ruling
		// 2026-08-28: fail-safe guard ahead of the full CHAOS-2622
		// drift-aware projector -- a team whose sync_policy is not the
		// auto-apply default (0) is left completely untouched, not
		// overwritten with this call's observed values.
		keptTeams, skippedTeamIDs, teamsStagedForReview, teamsDriftSuperseded, err := applyGitHubTeamSyncPolicyGuard(ctx, adapter.Sink.Conn, ref.OrgID, rows.Teams, normalizedAt)
		if err != nil {
			return result, err
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
		result.TeamsStagedForReview = teamsStagedForReview
		driftChangesSuperseded += teamsDriftSuperseded
		if len(keptTeams) > 0 {
			if err := adapter.Sink.WriteTeams(ctx, ref.OrgID, keptTeams); err != nil {
				return result, err
			}
			result.TeamsWritten = len(keptTeams)
			result.TeamKeys = make([]string, 0, len(keptTeams))
			for _, team := range keptTeams {
				if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
					result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
				}
			}
		}
	}
	// CHAOS-4434 correction: team_repo_ownership is gated ONLY on
	// selections.Teams, matching _populate_async exactly -- Python writes it
	// even on a roster_write_safe=false run (the roster gate protects only
	// the `teams` row's members field, never this table). Independent of the
	// sync_policy guard above too -- that guard is scoped to the `teams`
	// table only, matching Linear's own applyTeamSyncPolicyGuard doc comment.
	if selections.Teams && (len(rows.RepoOwnership) > 0 || len(rows.RepoListedTeamIDs) > 0) {
		decision := decideOwnershipClose(ctx, adapter.ScopeCensus, ownershipCloseRequest{
			ref: ref, provider: githubTeamCatalogProvider,
			listed: rows.RepoListedTeamIDs, unproven: rows.RepoUnprovenTeamIDs, responses: rows.RepoListingResponses,
		})
		// A grant that a listing of more than one response does not hold is
		// closed only on GitHub's own answer for that grant.
		lookups := NewOwnershipAbsenceLookups(ctx, githubRepoGrantAbsence{client: client, org: orgName})
		written, plan, err := adapter.Sink.SnapshotTeamRepoOwnership(
			ctx, ref.OrgID, orgName, rows.RepoOwnership, decision.read, decision.snapshot(GitHubTeamRepoGrantKind, githubTeamRepositoriesWalk, lookups.Answer), normalizedAt,
		)
		if err != nil {
			return result, err
		}
		closed := len(plan.Retract)
		ReportSnapshotPlan(ctx, githubTeamCatalogProvider, ref.OrgID, plan)
		result.RepoOwnershipWritten = written
		result.DegradedLegs = append(result.DegradedLegs, decision.legs...)
		result.DegradedLegs = append(result.DegradedLegs, SnapshotAbsenceLegs(plan)...)
		slog.Default().InfoContext(ctx, "github_team_catalog_repo_ownership_snapshot",
			"org_id", ref.OrgID, "teams_listed", len(rows.RepoListedTeamIDs),
			"teams_closable", len(decision.closable), "rows_written", written, "rows_closed", closed)
	}
	if selections.Members {
		result.MembershipsSkippedManualConflict = membershipsSkippedManualConflict
		result.MembershipsStagedForReview = membershipsStagedForReview
		// CHAOS-9007 / CHAOS-9079: a membership keeps the valid_from it was first
		// seen with, and a member absent from the COMPLETE read of its team is
		// closed (through the one snapshot rule). Absence is judged against the
		// members the provider returned, not the part the conflict guard keeps.
		decision := decideOwnershipClose(ctx, adapter.ScopeCensus, ownershipCloseRequest{
			ref: ref, provider: githubTeamCatalogProvider, listed: rows.ObservedMembershipTeamIDs,
			unproven: rows.UnprovenMembershipTeamIDs, dataset: "team_memberships", leg: membershipCloseLeg,
		})
		membershipRows, membershipOutcome, snapshotErr := githubMembershipWriter.Snapshot(
			ctx, adapter.Sink.Conn, ref.OrgID, rows.Memberships, keptMemberships, normalizedAt.UTC().Truncate(time.Millisecond),
			rows.MembershipAbsence, decision.membershipSnapshot(GitHubTeamMembershipKind))
		if snapshotErr != nil {
			return result, snapshotErr
		}
		ReportSnapshotPlan(ctx, githubTeamCatalogProvider, ref.OrgID, membershipOutcome.Plan)
		result.DegradedLegs = append(result.DegradedLegs, decision.legs...)
		result.MembershipsClosed = membershipOutcome.Closed
		if len(membershipRows) > 0 {
			if err := adapter.Sink.WriteMemberships(ctx, ref.OrgID, membershipRows); err != nil {
				return result, err
			}
			result.MembershipsWritten = len(keptMemberships)
			// MembersWritten stays 0 (codex round 2, P2): Linear's collector
			// sets it to its own `members` table row count
			// (linear_team_catalog_collector.go: batch.Result.Members ==
			// len(rows.Members)) -- GitHub has no `members` table writer at
			// all, only `team_memberships`, so reporting a distinct-identity
			// count under this field would claim rows were written to a
			// table this producer never touches.
		}
	}
	result.DriftChangesSuperseded = driftChangesSuperseded
	return result, nil
}

var _ TeamCatalogCollector = GitHubTeamCatalogCollector{}
