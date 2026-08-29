package providersync

import (
	"context"
	"strings"

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
SELECT scope_id
FROM manual_attribution_fallbacks FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND scope_type = 'member' AND (valid_to IS NULL OR valid_to > now())`

// resolveActiveMemberAttributionFallbackIdentities batch-reads every active
// member-scoped manual_attribution_fallbacks row for this org+provider,
// mirroring clickhouse_identity_drift.py's _member_fallbacks query. Returned
// as normalized (trimmed, lowercased) identity strings -- the SAME
// normalization _normalize_identity applies -- to compare against a
// membership row's raw_provider_user_id/raw_email/identity_facets.
func resolveActiveMemberAttributionFallbackIdentities(
	ctx context.Context, conn driver.Conn, orgID, provider string,
) (map[string]struct{}, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(provider) == "" {
		return nil, ErrInvalidConfiguration
	}
	rows, err := conn.Query(ctx, activeMemberAttributionFallbacksQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	identities := make(map[string]struct{})
	for rows.Next() {
		var scopeID string
		if err := rows.Scan(&scopeID); err != nil {
			return nil, err
		}
		normalized := normalizeMembershipIdentity(scopeID)
		if normalized != "" {
			identities[normalized] = struct{}{}
		}
	}
	return identities, rows.Err()
}

// normalizeMembershipIdentity ports _normalize_identity verbatim: trimmed,
// lowercased. The comparisons this guard does are exact-match only, never
// fuzzy -- matching the simpler (not the full projector's) contract
// team-lead's ruling specified for this interim guard.
func normalizeMembershipIdentity(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// membershipConflictsWithManualState reports whether a native membership row
// must be skipped.
//
// CHAOS-4431 codex review round 2, P1: an earlier revision of this function
// treated an exact (member_id, team_id) match against an active manual
// membership as the conflict to skip. That is backwards --
// clickhouse_identity_drift.py's _conflict_for treats a manual membership
// for the SAME team as a CONFIRMATION (line 259: `if manual.team_id ==
// team_id: continue` -- not a conflict at all) and only flags a manual
// membership to a DIFFERENT team as the conflict: Linear says this member
// is now on team B, but an active manual pin says team A, so writing B's
// native row would contradict the pin. The correct check is therefore
// member-scoped, not pair-scoped: if this member has ANY active manual
// membership at all, and NONE of those manual team_ids is this row's own
// team_id, skip it.
//
// The second source, member-scoped manual_attribution_fallbacks, is
// unaffected by this correction: it was already team-agnostic (matches
// clickhouse_identity_drift.py's own fallback branch, which never compares
// team_id at all).
func membershipConflictsWithManualState(
	row linearReferenceMembershipRow,
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

// applyTeamMembershipConflictGuard filters a batch of native membership rows
// against both active-conflict sources above, returning the rows safe to
// write and a count of how many were skipped (team-lead ruling, 2026-08-28:
// reported under a distinct "skipped_manual_conflict" reason, never folded
// into MembershipsWritten so "skipped for a real conflict" is never
// indistinguishable from "nothing to write this run").
func applyTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID, provider string, rows []linearReferenceMembershipRow,
) ([]linearReferenceMembershipRow, int, error) {
	if len(rows) == 0 {
		return rows, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, err
	}
	fallbackIdentities, err := resolveActiveMemberAttributionFallbackIdentities(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, err
	}
	kept := make([]linearReferenceMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if membershipConflictsWithManualState(row, manualTeamsByMember, fallbackIdentities) {
			skipped++
			continue
		}
		kept = append(kept, row)
	}
	return kept, skipped, nil
}
