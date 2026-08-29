package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GitLab's own thin wrappers around the shared, provider-agnostic guard
// primitives (team_sync_policy_guard.go, team_membership_conflict_guard.go)
// -- CHAOS-4431 codex review findings #3 (drift projector bypass) and #6
// (membership conflict review bypass), team-lead ruling 2026-08-28, applied
// to GitLab the same way LinearTeamCatalogCollector already wires them
// (linear_team_catalog_collector.go). Same shape, GitLab-typed rows.

// applyGitLabTeamSyncPolicyGuard mirrors applyTeamSyncPolicyGuard exactly,
// against gitlabTeamCatalogTeamRow instead of linearReferenceTeamRow: a
// team whose sync_policy is not the auto-apply default (0) is left
// completely untouched by this write.
func applyGitLabTeamSyncPolicyGuard(
	ctx context.Context, conn driver.Conn, orgID string, teams []gitlabTeamCatalogTeamRow, now time.Time,
) ([]gitlabTeamCatalogTeamRow, []string, int, int, error) {
	if len(teams) == 0 {
		return teams, nil, 0, 0, nil
	}
	views := make([]teamDriftTeamView, len(teams))
	for index, team := range teams {
		views[index] = gitlabTeamRowToDriftView(team)
	}
	keptIdx, skipped, staged, superseded, err := reviewTeamRowsForDrift(ctx, conn, orgID, views, now)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	kept := make([]gitlabTeamCatalogTeamRow, 0, len(keptIdx))
	for _, index := range keptIdx {
		kept = append(kept, teams[index])
	}
	return kept, skipped, staged, superseded, nil
}

// gitlabMembershipConflictsWithManualState mirrors membershipConflictsWithManualState
// exactly (CORRECTED, codex review round 3: fallback rows are now team-
// scoped too, and the rule is "any active manual/fallback row names a team
// OTHER than the incoming one", not "the incoming team is absent from the
// set" -- a member/identity with an active row for BOTH the incoming team
// and another team is still a conflict, since Python's loop checks every
// row rather than stopping at the first match): a manual-membership or
// fallback row to the EXACT SAME team as this native row is a CONFIRMATION;
// a row to ANY OTHER team is the conflict.
func gitlabMembershipConflictsWithManualState(
	row gitlabTeamCatalogMembershipRow,
	manualTeamsByMember map[string]map[string]struct{},
	fallbackTeamsByIdentity map[string]map[string]struct{},
) bool {
	if manualTeams, hasManual := manualTeamsByMember[row.MemberID]; hasManual {
		if anyTeamDiffersFrom(manualTeams, row.TeamID) {
			return true
		}
	}
	if len(fallbackTeamsByIdentity) == 0 {
		return false
	}
	candidates := make([]string, 0, len(row.IdentityFacets)+2)
	if row.RawProviderUserID != nil {
		candidates = append(candidates, *row.RawProviderUserID)
	}
	if row.RawEmail != nil {
		candidates = append(candidates, *row.RawEmail)
	}
	candidates = append(candidates, row.IdentityFacets...)
	for _, candidate := range candidates {
		normalized := normalizeMembershipIdentity(candidate)
		if normalized == "" {
			continue
		}
		if fallbackTeams, hasFallback := fallbackTeamsByIdentity[normalized]; hasFallback {
			if anyTeamDiffersFrom(fallbackTeams, row.TeamID) {
				return true
			}
		}
	}
	return false
}

// applyGitLabTeamMembershipConflictGuard mirrors applyTeamMembershipConflictGuard
// exactly, against gitlabTeamCatalogMembershipRow: filters a batch of native
// membership rows against both active-conflict sources, returning the rows
// safe to write and a count of how many were skipped (reported under
// TeamCatalogResult.MembershipsSkippedManualConflict, never folded into
// MembershipsWritten).
// CHAOS-4444: every skipped (conflicting) row is now ALSO staged as a
// team_drift_changes row via the shared reviewMembershipsForDrift engine
// (identity_drift_review.go), and a stale pending row for a member this run
// observed but no longer sees conflicting (or sees at all) is
// resolved/superseded. observedTeamIDs names the teams whose member fetch
// succeeded this run (rows with MembersAuthoritative=true).
func applyGitLabTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID, provider string, rows []gitlabTeamCatalogMembershipRow, observedTeamIDs []string, now time.Time,
) ([]gitlabTeamCatalogMembershipRow, int, int, int, error) {
	if len(rows) == 0 && len(observedTeamIDs) == 0 {
		return rows, 0, 0, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	kept := make([]gitlabTeamCatalogMembershipRow, 0, len(rows))
	skipped := 0
	conflictedIdx := make(map[int]struct{})
	views := make([]teamDriftMembershipView, len(rows))
	for index, row := range rows {
		views[index] = gitlabMembershipRowToDriftView(row)
		if gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
			skipped++
			conflictedIdx[index] = struct{}{}
			continue
		}
		kept = append(kept, row)
	}
	staged, superseded, err := reviewMembershipsForDrift(ctx, conn, orgID, provider, views, conflictedIdx, observedTeamIDs, now)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return kept, skipped, staged, superseded, nil
}
