package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const activeManualMembershipsQuery = `
SELECT member_id, team_id
FROM team_memberships FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND source = 'manual' AND (valid_to IS NULL OR valid_to > now())`

// resolveActiveManualMembershipTeams batch-reads every active manual
// team_memberships row for this org+provider, grouped by member_id into the
// SET of team_ids that member has an active manual pin to, mirroring
// clickhouse_identity_drift.py's _manual_memberships query (scoped to
// source='manual', not-yet-expired). CHAOS-4431 codex review finding #6
// (team-lead ruling 2026-08-28): an interim guard ahead of the full
// CHAOS-2622/CHAOS-4444 drift-aware projector, not a replacement for it.
//
// Grouped by member (not a flat (member,team) set) because the conflict
// check ITSELF is member-scoped, not pair-scoped -- see
// membershipConflictsWithManualState's doc comment for why (codex review
// round 2, P1: an earlier revision of this file got this backwards).
func resolveActiveManualMembershipTeams(
	ctx context.Context, conn driver.Conn, orgID, provider string,
) (map[string]map[string]struct{}, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(provider) == "" {
		return nil, ErrInvalidConfiguration
	}
	rows, err := conn.Query(ctx, activeManualMembershipsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byMember := make(map[string]map[string]struct{})
	for rows.Next() {
		var memberID, teamID string
		if err := rows.Scan(&memberID, &teamID); err != nil {
			return nil, err
		}
		teams, ok := byMember[memberID]
		if !ok {
			teams = make(map[string]struct{})
			byMember[memberID] = teams
		}
		teams[teamID] = struct{}{}
	}
	return byMember, rows.Err()
}

const activeMemberAttributionFallbacksQuery = `
SELECT scope_id, team_id
FROM manual_attribution_fallbacks FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND scope_type = 'member' AND (valid_to IS NULL OR valid_to > now())`

// resolveActiveMemberAttributionFallbackTeams batch-reads every active
// member-scoped manual_attribution_fallbacks row for this org+provider,
// mirroring clickhouse_identity_drift.py's _member_fallbacks query, grouped
// by normalized identity (the SAME normalization _normalize_identity
// applies) into the SET of team_ids that identity has an active fallback
// pin to.
//
// CHAOS-4431 codex review round 3, P2: an earlier revision of this query
// dropped team_id entirely, treating every matching fallback row as a
// conflict. clickhouse_identity_drift.py's _conflict_for fallback branch
// (lines 263-271) does compare team_id, with the SAME same-team-is-a-
// confirmation / different-team-is-a-conflict shape the manual-membership
// branch uses (line 269: `if fallback.team_id == team_id: continue`) -- it
// is NOT team-agnostic the way this file's doc comments previously claimed.
func resolveActiveMemberAttributionFallbackTeams(
	ctx context.Context, conn driver.Conn, orgID, provider string,
) (map[string]map[string]struct{}, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(provider) == "" {
		return nil, ErrInvalidConfiguration
	}
	rows, err := conn.Query(ctx, activeMemberAttributionFallbacksQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byIdentity := make(map[string]map[string]struct{})
	for rows.Next() {
		var scopeID, teamID string
		if err := rows.Scan(&scopeID, &teamID); err != nil {
			return nil, err
		}
		normalized := normalizeMembershipIdentity(scopeID)
		if normalized == "" {
			continue
		}
		teams, ok := byIdentity[normalized]
		if !ok {
			teams = make(map[string]struct{})
			byIdentity[normalized] = teams
		}
		teams[teamID] = struct{}{}
	}
	return byIdentity, rows.Err()
}

// normalizeMembershipIdentity ports _normalize_identity verbatim: trimmed,
// lowercased. The comparisons this guard does are exact-match only, never
// fuzzy -- matching the simpler (not the full projector's) contract
// team-lead's ruling specified for this interim guard.
func normalizeMembershipIdentity(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// anyTeamDiffersFrom reports whether teams contains at least one entry other
// than exclude. This is the exact shape clickhouse_identity_drift.py's
// _conflict_for loop produces (CHAOS-4431 codex review round 3, P2): it
// iterates EVERY candidate row for a member/identity and returns a conflict
// on the FIRST one whose team_id differs, rather than stopping once it finds
// ONE row that matches. A member/identity with manual rows for BOTH the
// incoming team and another team is therefore still a conflict -- "the
// incoming team is confirmed somewhere in the set" is not sufficient on its
// own (an earlier revision of this file only checked set membership, missing
// this case).
func anyTeamDiffersFrom(teams map[string]struct{}, exclude string) bool {
	for teamID := range teams {
		if teamID != exclude {
			return true
		}
	}
	return false
}

// membershipConflictsWithManualState reports whether a native membership row
// must be skipped: its member has an active manual membership to a team
// OTHER than the incoming row's team (manual-membership source), or any
// identity the row resolves to (provider id, raw email, or a facet) has an
// active member-scoped manual_attribution_fallbacks row to a team OTHER than
// the incoming row's team (fallback source). Both sources share the same
// same-team-confirms / different-team-conflicts shape as
// clickhouse_identity_drift.py's _conflict_for.
func membershipConflictsWithManualState(
	row linearReferenceMembershipRow,
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

// applyTeamMembershipConflictGuard filters a batch of native membership rows
// against both active-conflict sources above, returning the rows safe to
// write and a count of how many were skipped (team-lead ruling, 2026-08-28:
// reported under a distinct "skipped_manual_conflict" reason, never folded
// into MembershipsWritten so "skipped for a real conflict" is never
// indistinguishable from "nothing to write this run"). CHAOS-4444: every
// skipped (conflicting) row is now ALSO staged as a team_drift_changes row
// via the shared reviewMembershipsForDrift engine
// (identity_drift_review.go), and a stale pending row for a member this run
// observed but no longer sees conflicting (or sees at all) is
// resolved/superseded. observedTeamIDs names the teams whose member fetch
// was attempted this run (Linear always collects members alongside teams
// in one walk -- no per-team partial-failure model -- so this is simply
// every team id CollectReferenceCatalog returned when Members was
// selected).
func applyTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID, provider string, rows []linearReferenceMembershipRow, observedTeamIDs []string, now time.Time,
) ([]linearReferenceMembershipRow, int, int, int, error) {
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
	kept := make([]linearReferenceMembershipRow, 0, len(rows))
	skipped := 0
	conflictedIdx := make(map[int]struct{})
	views := make([]teamDriftMembershipView, len(rows))
	for index, row := range rows {
		views[index] = linearMembershipRowToDriftView(row)
		if membershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
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
