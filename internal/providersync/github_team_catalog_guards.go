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
// membership row must be skipped. Mirrors membershipConflictsWithManualState
// exactly (round 3 corrected shape): both sources are member/identity-scoped
// sets of team_ids, and BOTH share the same same-team-confirms /
// different-team-conflicts rule (anyTeamDiffersFrom) -- a manual membership
// or member-scoped manual_attribution_fallbacks row for the member's OWN
// team is a CONFIRMATION; one naming any OTHER team is a conflict, even if
// a same-team row also exists (every candidate row is checked, matching
// clickhouse_identity_drift.py's _conflict_for loop exactly).
func githubMembershipConflictsWithManualState(
	row githubMembershipRow,
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
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, githubTeamCatalogProvider)
	if err != nil {
		return nil, 0, err
	}
	kept := make([]githubMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if githubMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
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
