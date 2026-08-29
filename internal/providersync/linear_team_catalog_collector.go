package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
	if selections.Teams {
		teamRows := batch.Rows.Teams
		// CHAOS-4431 codex review P1: a teams-only run (members deselected)
		// must not overwrite `teams.members` with the page-1-only
		// placeholder CollectReferenceCatalog left on the row -- preserve
		// whatever roster is already persisted, exactly like Python's
		// _existing_team_members path (team_autoimport_linear.py:685-707).
		if !selections.Members && len(teamRows) > 0 {
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
		keptTeams, skippedTeamIDs, err := applyTeamSyncPolicyGuard(ctx, collector.Sink.Conn, ref.OrgID, teamRows)
		if err != nil {
			return result, err
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
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
		// projector -- skip any membership row whose (member_id, team_id)
		// already has an active manual membership, or whose member identity
		// has an active member-scoped manual_attribution_fallbacks row.
		// Independent of the #3 sync_policy guard above: this gate applies
		// even to policy-0 teams (team-attribution.md:793-797).
		keptMemberships, skippedMemberships, err := applyTeamMembershipConflictGuard(
			ctx, collector.Sink.Conn, ref.OrgID, "linear", batch.Rows.Memberships,
		)
		if err != nil {
			return result, err
		}
		result.MembershipsSkippedManualConflict = skippedMemberships
		membershipsEffect, err := effectBatchFromValues(linearReferenceCatalogMembershipsDestination, EffectReadbackRequired, keptMemberships)
		if err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, membershipsEffect); err != nil {
			return result, err
		}
		result.MembersWritten = batch.Result.Members
		result.MembershipsWritten = len(keptMemberships)
	}
	if selections.Projects {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Projects); err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Ownership); err != nil {
			return result, err
		}
		result.ProjectsWritten = batch.Result.Projects
		result.OwnershipWritten = batch.Result.Ownership
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
