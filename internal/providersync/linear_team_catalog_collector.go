package providersync

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// LinearTeamCatalogCollector adapts LinearReferenceCatalogRouteHandler
// (the collection walk) and LinearReferenceCatalogClickHouseEffects (the
// write) to the shared, claim-free TeamCatalogCollector seam (CHAOS-4431,
// team-lead ruling 2026-08-28, option (c)). It is the first native
// implementation; CHAOS-4434 (GitHub) and CHAOS-4432 (GitLab) follow the
// same shape against the same interface.
type LinearTeamCatalogCollector struct {
	Handler LinearReferenceCatalogRouteHandler
	Sink    LinearReferenceCatalogClickHouseEffects
	// ScopeCensus counts the org's other active Linear integrations. Without
	// it no ownership row is closed (ProveSoleScope).
	ScopeCensus OwnershipScopeCensus
}

// CollectTeamCatalog walks Linear's teams/members/projects once and writes
// only the destinations this org has selected (CHAOS-4323). Teams are always
// collected from the wire in one shot with members and projects (Linear's
// GraphQL shape nests members under teams), but a selection still gates the
// WRITE: a deselected surface is never persisted, matching Python's
// per-flag behavior in run_team_autoimport_strict/run_post_sync_team_autoimport.
//
// Sprints/cycles are the one exception (CHAOS-4431 codex review P1): Python
// only skips its ENTIRE call, sprints included, when non-strict AND every
// selection is off (team_autoimport_linear.py:421). In strict mode -- this
// seam's ONLY caller today, TeamCatalogDiscoveryExecutor -- that early exit
// never applies, so sprints are collected and written even with every
// selection off. A future non-strict caller that reaches this function with
// every selection off should not call it at all (mirrors Python's early
// return); this function does not re-derive that decision itself.
func (collector LinearTeamCatalogCollector) CollectTeamCatalog(
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
	if !ref.Strict && !selections.Any() {
		return TeamCatalogResult{}, nil
	}
	if collector.Sink.Conn == nil || collector.Sink.Lease == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	batch, err := collector.Handler.CollectReferenceCatalog(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		return TeamCatalogResult{}, err
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: "linear"}}
	result := TeamCatalogResult{}

	// CHAOS-4431 codex review finding #6, ROUND 2 correction (P1): the
	// membership-conflict guard must run BEFORE the team roster is written,
	// not after -- team_autoimport_linear.py builds team_rows[...]['members']
	// via _apply_roster(team_rows, memberships) using memberships that
	// ALREADY went through split_memberships_for_review. Running the guard
	// after the Teams write (as an earlier revision of this file did) lets a
	// membership the guard rejects still show up in `teams.members`, a live
	// attribution fallback source, silently reintroducing the exact
	// contradiction the guard exists to prevent. So this is computed once,
	// up front, and both blocks below read from its result.
	var keptMemberships []linearReferenceMembershipRow
	var membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded int
	if selections.Members {
		observedTeamIDs := make([]string, 0, len(batch.Rows.Teams))
		for _, team := range batch.Rows.Teams {
			observedTeamIDs = append(observedTeamIDs, team.ID)
		}
		var err error
		keptMemberships, membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded, err = applyTeamMembershipConflictGuard(
			ctx, collector.Sink.Conn, ref.OrgID, "linear", batch.Rows.Memberships, observedTeamIDs, normalizedAt,
		)
		if err != nil {
			return result, err
		}
	}

	if selections.Teams {
		teamRows := batch.Rows.Teams
		if selections.Members {
			// Rebuild each team's roster from the CONFLICT-FILTERED
			// memberships, not the raw provider-observed roster
			// CollectReferenceCatalog baked into the row -- see the doc
			// comment above.
			teamRows = append([]linearReferenceTeamRow(nil), teamRows...)
			for index := range teamRows {
				teamRows[index].Members = linearReferenceTeamRosterFromMemberships(teamRows[index].ID, keptMemberships)
			}
		} else if len(teamRows) > 0 {
			// CHAOS-4431 codex review P1: a teams-only run (members
			// deselected) must not overwrite `teams.members` with the
			// page-1-only placeholder CollectReferenceCatalog left on the
			// row -- preserve whatever roster is already persisted, exactly
			// like Python's _existing_team_members path
			// (team_autoimport_linear.py:685-707).
			teamIDs := make([]string, 0, len(teamRows))
			for _, team := range teamRows {
				teamIDs = append(teamIDs, team.ID)
			}
			existingRoster, err := PreserveExistingTeamMembersRoster(ctx, collector.Sink.Conn, ref.OrgID, teamIDs)
			if err != nil {
				return result, err
			}
			teamRows = append([]linearReferenceTeamRow(nil), teamRows...)
			for index := range teamRows {
				teamRows[index].Members = existingRoster[teamRows[index].ID]
			}
		}
		// CHAOS-4431 codex review findings #3/#6, team-lead ruling
		// 2026-08-28: fail-safe guard ahead of the full CHAOS-2622
		// drift-aware projector -- a team whose sync_policy is not the
		// auto-apply default (0) is left completely untouched, not
		// overwritten with this call's observed values.
		keptTeams, skippedTeamIDs, teamsStagedForReview, teamsDriftSuperseded, err := applyTeamSyncPolicyGuard(ctx, collector.Sink.Conn, ref.OrgID, teamRows, normalizedAt)
		if err != nil {
			return result, err
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
		result.TeamsStagedForReview = teamsStagedForReview
		driftChangesSuperseded += teamsDriftSuperseded
		teamsEffect, err := effectBatchFromValues(linearReferenceCatalogTeamsDestination, EffectReadbackRequired, keptTeams)
		if err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, teamsEffect); err != nil {
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
	if selections.Members {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Members); err != nil {
			return result, err
		}
		// CHAOS-4431 codex review finding #6, team-lead ruling 2026-08-28:
		// fail-safe guard ahead of the full CHAOS-2622/CHAOS-4444 drift-aware
		// projector -- skip any membership row whose member has an active
		// manual membership to a DIFFERENT team, or whose member identity
		// has an active member-scoped manual_attribution_fallbacks row.
		// Independent of the #3 sync_policy guard above: this gate applies
		// even to policy-0 teams (team-attribution.md:793-797). Computed
		// above, before the Teams block, so the roster and the memberships
		// table never disagree about which assignments are safe.
		result.MembershipsSkippedManualConflict = membershipsSkippedManualConflict
		result.MembershipsStagedForReview = membershipsStagedForReview
		// CHAOS-9007 / CHAOS-9079: a membership keeps the valid_from it was first
		// seen with, and a member absent from the COMPLETE read of its team is
		// closed (through the one snapshot rule). Linear's member walk fails the
		// whole run when a team's member list does not end, so a run that gets
		// here has read every team it lists to its end (evidence.MembersComplete).
		teamIDs := make([]string, 0, len(batch.Rows.Teams))
		for _, team := range batch.Rows.Teams {
			teamIDs = append(teamIDs, team.ID)
		}
		// A team whose member list held a node the collector cannot use is not
		// complete either: nothing of it closes.
		unprovenTeamIDs := batch.Rows.UnusableMemberTeamIDs
		if !batch.Evidence.MembersComplete {
			unprovenTeamIDs = teamIDs
		}
		// A team that holds open memberships and that the workspace's team walk
		// no longer returns is a dropped team (CHAOS-9102): its memberships close
		// when the team walk reached its end (the cursor walk's statement, as for
		// the team-key ownership kind).
		teamListing := TeamListingEvidence{
			TeamIDPrefix: teamid.Prefix("linear"), Listed: teamIDs,
			ProvesEnd: batch.Evidence.TeamsComplete, Cursor: true,
		}
		droppedTeams, err := openMembershipTeamIDsNotListed(
			ctx, collector.Sink.Conn, ref.OrgID, linearMembershipWriter.Provider, linearMembershipWriter.Source,
			teamListing.TeamIDPrefix, teamListing.Listed)
		if err != nil {
			return result, err
		}
		decision := decideOwnershipClose(ctx, collector.ScopeCensus, ownershipCloseRequest{
			ref: ref, provider: "linear", listed: teamIDs, unproven: unprovenTeamIDs,
			dataset: "team_memberships", leg: membershipCloseLeg,
			dropped: droppedTeams, teamListing: teamListing,
		})
		membershipRows, membershipOutcome, err := linearMembershipWriter.Snapshot(
			ctx, collector.Sink.Conn, ref.OrgID, batch.Rows.Memberships, keptMemberships, normalizedAt.UTC().Truncate(time.Millisecond),
			decision.membershipProver(linearInactiveAbsence{inactive: batch.Rows.InactiveMemberKeys}), decision.membershipSnapshot(LinearTeamMembershipKind))
		if err != nil {
			return result, err
		}
		ReportSnapshotPlan(ctx, "linear", ref.OrgID, membershipOutcome.Plan)
		result.DegradedLegs = append(result.DegradedLegs, decision.legs...)
		result.MembershipsClosed = membershipOutcome.Closed
		membershipsEffect, err := effectBatchFromValues(linearReferenceCatalogMembershipsDestination, EffectReadbackRequired, membershipRows)
		if err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, membershipsEffect); err != nil {
			return result, err
		}
		result.MembersWritten = batch.Result.Members
		result.MembershipsWritten = len(keptMemberships)
	}
	result.DriftChangesSuperseded = driftChangesSuperseded
	// CHAOS-4530 (corrected, post-CF finding): this collector never writes
	// ANY row -- active or tombstone -- for the team-key-shaped pseudo-
	// project identity any more (see CollectReferenceCatalog's doc comment).
	// batch.Rows.Projects/Effects.Projects therefore hold only REAL native
	// Linear projects, gated by selections.Projects exactly like Ownership
	// always was -- no separate unconditional write path needed.
	if selections.Projects {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Projects); err != nil {
			return result, err
		}
		// Snapshot rule, the one every ownership writer shares
		// (PlanOwnershipSnapshot): a row the run still holds keeps the
		// valid_from it was first seen with, and an open row of this writer
		// that the run no longer holds is closed in this write only when the
		// walk of ITS kind proved its end and found at least one row of the
		// kind. A new stamp at each sync would add one more open row for the
		// same fact.
		ownershipRows, plan, err := collector.Sink.SnapshotOwnership(
			ctx, ref.OrgID, batch.Rows.Ownership, normalizedAt.UTC().Truncate(time.Millisecond),
			linearOwnershipKindSnapshots(ref.OrgID, ProveSoleScope(ctx, collector.ScopeCensus, ref.OrgID, "linear", ref.IntegrationID),
				batch.Evidence, batch.Result)...)
		if err != nil {
			return result, err
		}
		retracted := len(plan.Retract)
		incomplete := ReportSnapshotPlan(ctx, "linear", ref.OrgID, plan)
		if incomplete {
			slog.Default().WarnContext(ctx, "linear_reference_catalog_ownership_snapshot_incomplete",
				"org_id", ref.OrgID, "teams_complete", batch.Evidence.TeamsComplete,
				"projects_complete", batch.Evidence.ProjectsComplete,
				"teams_without_key", batch.Result.OwnershipTeamsWithoutKey, "reasons", strings.Join(plan.SnapshotReasons(), ","))
			// The project-walk causes are counted where the walk gives up; the
			// ones below are decided here.
			for _, reason := range plan.SnapshotReasons() {
				switch reason {
				case linearSnapshotTeamsNotRead, linearSnapshotKeylessLink, SnapshotEmptyAnswer,
					OwnershipCloseSkippedScopeShared, OwnershipCloseSkippedCensusUnavailable, OwnershipCloseSkippedCensusFailed, ScopeNotProven:
					recordLinearOwnershipSnapshotIncomplete(ctx, reason)
				}
			}
		}
		if retracted > 0 {
			slog.Default().InfoContext(ctx, "linear_reference_catalog_ownership_retracted",
				"org_id", ref.OrgID, "rows", retracted)
		}
		ownershipEffect, err := effectBatchFromValues(linearReferenceCatalogOwnershipDestination, EffectReadbackRequired, ownershipRows)
		if err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, ownershipEffect); err != nil {
			return result, err
		}
		result.OwnershipRetracted = retracted
		result.OwnershipSnapshotIncomplete = incomplete
		result.ProjectsWritten = batch.Result.Projects
		result.OwnershipWritten = batch.Result.Ownership
		result.ProjectsWithoutKey = batch.Result.ProjectsWithoutKey
	}
	// Unconditional reference data -- see the function doc comment above.
	if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Sprints); err != nil {
		return result, err
	}
	result.SprintsWritten = len(batch.Rows.Sprints)
	result.SprintIDs = make([]string, 0, len(batch.Rows.Sprints))
	for _, sprint := range batch.Rows.Sprints {
		result.SprintIDs = append(result.SprintIDs, sprint.SprintID)
	}
	return result, nil
}

var _ TeamCatalogCollector = LinearTeamCatalogCollector{}

// The reasons of the Linear snapshot terms. The project-walk causes
// (project_pages_not_read_to_the_end, project_teams_page_end_not_stated, ...)
// are named where the walk gives up; linearSnapshotProjectsNotRead is the
// term that carries them.
const (
	linearSnapshotTeamsNotRead    = "teams_not_read_to_the_end"
	linearSnapshotProjectsNotRead = "projects_not_read_to_the_end"
	linearSnapshotKeylessLink     = "project_team_link_without_key"
)

// linearOwnershipKindSnapshots is the proof of each fact kind of one Linear
// catalog run. scope is the scope gate's answer for the run (the open rows
// read are every Linear row of the organization, whatever workspace wrote
// them). Each kind's proof holds only terms of its own walk: the team walk for
// the team-key rows; every project page and node, and every project-team link
// with a key, for the project rows (a dropped link is a fact the run did not
// see).
func linearOwnershipKindSnapshots(orgID string, scope ScopeProof, evidence LinearReferenceCatalogEvidence, result LinearReferenceCatalogResult) []KindSnapshot[OwnershipSnapshotRow] {
	return []KindSnapshot[OwnershipSnapshotRow]{
		LinearProjectOwnershipKind(orgID).Snapshot(scope, ProveSnapshot(
			SnapshotTerm{Holds: evidence.ProjectsComplete, Reason: linearSnapshotProjectsNotRead},
			SnapshotTerm{Holds: result.OwnershipTeamsWithoutKey == 0, Reason: linearSnapshotKeylessLink},
		), AbsenceByWalk[OwnershipSnapshotRow](AbsenceWalkByCursor)),
		LinearTeamKeyOwnershipKind(orgID).Snapshot(scope, ProveSnapshot(
			SnapshotTerm{Holds: evidence.TeamsComplete, Reason: linearSnapshotTeamsNotRead},
		), AbsenceByWalk[OwnershipSnapshotRow](AbsenceWalkByCursor)),
	}
}
