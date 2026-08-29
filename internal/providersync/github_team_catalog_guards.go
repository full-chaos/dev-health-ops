package providersync

import (
	"context"
	"time"

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
// applyTeamMembershipConflictGuard exactly). CHAOS-4444: every skipped
// (conflicting) row is now ALSO staged as a team_drift_changes row via the
// shared reviewMembershipsForDrift engine (identity_drift_review.go), and a
// stale pending row for a member this run observed but no longer sees
// conflicting (or sees at all) is resolved/superseded. observedTeamIDs
// names the teams whose member fetch succeeded this run (see
// GitHubTeamCatalogCollector's rows.FailedMemberFetchTeamIDs).
func applyGitHubTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID string, rows []githubMembershipRow, observedTeamIDs []string, now time.Time,
) ([]githubMembershipRow, int, int, int, error) {
	if len(rows) == 0 && len(observedTeamIDs) == 0 {
		return rows, 0, 0, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, githubTeamCatalogProvider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, githubTeamCatalogProvider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	kept := make([]githubMembershipRow, 0, len(rows))
	skipped := 0
	conflictedIdx := make(map[int]struct{})
	views := make([]teamDriftMembershipView, len(rows))
	for index, row := range rows {
		views[index] = githubMembershipRowToDriftView(row)
		if githubMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
			skipped++
			conflictedIdx[index] = struct{}{}
			continue
		}
		kept = append(kept, row)
	}
	staged, superseded, err := reviewMembershipsForDrift(ctx, conn, orgID, githubTeamCatalogProvider, views, conflictedIdx, observedTeamIDs, now)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return kept, skipped, staged, superseded, nil
}

// applyGitHubTeamSyncPolicyGuard is GitHub's wrapper over the shared
// reviewTeamRowsForDrift engine (team_drift_review.go, CHAOS-4444): a team
// whose sync_policy is not the auto-apply default (0) is left completely
// untouched by this write, and its diff against the currently-persisted row
// is staged as a team_drift_changes row for review. Mirrors
// applyTeamSyncPolicyGuard exactly, typed to githubTeamRow.
func applyGitHubTeamSyncPolicyGuard(
	ctx context.Context, conn driver.Conn, orgID string, teams []githubTeamRow, now time.Time,
) ([]githubTeamRow, []string, int, int, error) {
	if len(teams) == 0 {
		return teams, nil, 0, 0, nil
	}
	views := make([]teamDriftTeamView, len(teams))
	for index, team := range teams {
		views[index] = githubTeamRowToDriftView(team)
	}
	keptIdx, skipped, staged, superseded, err := reviewTeamRowsForDrift(ctx, conn, orgID, views, now)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	kept := make([]githubTeamRow, 0, len(keptIdx))
	for _, index := range keptIdx {
		kept = append(kept, teams[index])
	}
	return kept, skipped, staged, superseded, nil
}
