package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// github_team_catalog_guards.go is GitHub's own thin wrapper over the two
// shared, provider-agnostic fail-safe guards CHAOS-4431 built for Linear
// (team_membership_conflict_guard.go finding #6, team_sync_policy_guard.go
// finding #3) -- team-lead ruling, 2026-08-28: every native team-catalog
// collector needs both, not just Linear's. Deliberately typed to
// githubMembershipRow/githubTeamRow rather than generic, mirroring
// linear_team_catalog_collector.go's own pattern -- the underlying
// resolve* functions (resolveActiveManualMembershipTeams,
// resolveActiveMemberAttributionFallbackIdentities, resolveTeamSyncPolicies)
// are org_id/team_id/member_id string-keyed and already shared.

// githubMembershipConflictsWithManualState reports whether a native
// membership row must be skipped. CORRECTED (round 2) semantics, mirroring
// membershipConflictsWithManualState exactly: member-scoped, not
// pair-scoped -- if this member has ANY active manual membership at all, and
// NONE of those manual team_ids is this row's own team_id, skip it (a
// manual membership to the SAME team is a CONFIRMATION, not a conflict).
// The second source, member-scoped manual_attribution_fallbacks, is
// team-agnostic: an active match blocks the write regardless of team.
func githubMembershipConflictsWithManualState(
	row githubMembershipRow,
	manualTeamsByMember map[string]map[string]struct{},
	fallbackIdentities map[string]struct{},
) bool {
	if manualTeams, hasManual := manualTeamsByMember[row.MemberID]; hasManual {
		if _, confirmed := manualTeams[row.TeamID]; !confirmed {
			return true
		}
	}
	if len(fallbackIdentities) == 0 {
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
		if normalized := normalizeMembershipIdentity(candidate); normalized != "" {
			if _, conflict := fallbackIdentities[normalized]; conflict {
				return true
			}
		}
	}
	return false
}

// applyGitHubTeamMembershipConflictGuard filters a batch of native
// membership rows against both active-conflict sources, returning the rows
// safe to write and a count of how many were skipped (never folded into
// MembershipsWritten, so "skipped for a real conflict" stays distinguishable
// from "nothing to write this run" in telemetry -- mirrors
// applyTeamMembershipConflictGuard exactly).
func applyGitHubTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID string, rows []githubMembershipRow,
) ([]githubMembershipRow, int, error) {
	if len(rows) == 0 {
		return rows, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, githubTeamCatalogProvider)
	if err != nil {
		return nil, 0, err
	}
	fallbackIdentities, err := resolveActiveMemberAttributionFallbackIdentities(ctx, conn, orgID, githubTeamCatalogProvider)
	if err != nil {
		return nil, 0, err
	}
	kept := make([]githubMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if githubMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackIdentities) {
			skipped++
			continue
		}
		kept = append(kept, row)
	}
	return kept, skipped, nil
}

// applyGitHubTeamSyncPolicyGuard is GitHub's wrapper over the shared
// resolveTeamSyncPolicies (CHAOS-2622/CHAOS-4444-class fail-safe guard,
// codex review finding #3): a team whose sync_policy is not the auto-apply
// default (0) is left completely untouched by this write. Mirrors
// applyTeamSyncPolicyGuard exactly, typed to githubTeamRow.
func applyGitHubTeamSyncPolicyGuard(
	ctx context.Context, conn driver.Conn, orgID string, teams []githubTeamRow,
) ([]githubTeamRow, []string, error) {
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
	kept := make([]githubTeamRow, 0, len(teams))
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
