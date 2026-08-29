package providersync

import (
	"context"

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
	ctx context.Context, conn driver.Conn, orgID string, teams []gitlabTeamCatalogTeamRow,
) ([]gitlabTeamCatalogTeamRow, []string, error) {
	if len(teams) == 0 {
		return teams, nil, nil
	}
	teamIDs := make([]string, 0, len(teams))
	for _, team := range teams {
		teamIDs = append(teamIDs, team.ID)
	}
	policies, err := resolveTeamSyncPolicies(ctx, conn, orgID, teamIDs)
	if err != nil {
		return nil, nil, err
	}
	kept := make([]gitlabTeamCatalogTeamRow, 0, len(teams))
	skipped := make([]string, 0)
	for _, team := range teams {
		if policies[team.ID] != teamAutoApplySyncPolicy {
			skipped = append(skipped, team.ID)
			continue
		}
		kept = append(kept, team)
	}
	return kept, skipped, nil
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
func applyGitLabTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID, provider string, rows []gitlabTeamCatalogMembershipRow,
) ([]gitlabTeamCatalogMembershipRow, int, error) {
	if len(rows) == 0 {
		return rows, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, err
	}
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, err
	}
	kept := make([]gitlabTeamCatalogMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
			skipped++
			continue
		}
		kept = append(kept, row)
	}
	return kept, skipped, nil
}
