package providersync

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// membershipConflictPair identifies one (member_id, team_id) attribution
// edge -- the same key space team-attribution.md's drift projector and
// clickhouse_identity_drift.py's _member_key use.
type membershipConflictPair struct {
	MemberID string
	TeamID   string
}

const activeManualMembershipsQuery = `
SELECT member_id, team_id
FROM team_memberships FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND source = 'manual' AND (valid_to IS NULL OR valid_to > now())`

// resolveActiveManualMembershipPairs batch-reads every active manual
// team_memberships row for this org+provider, mirroring
// clickhouse_identity_drift.py's _manual_memberships query (scoped to
// source='manual', not-yet-expired). CHAOS-4431 codex review finding #6
// (team-lead ruling 2026-08-28): an interim guard ahead of the full
// CHAOS-2622/CHAOS-4444 drift-aware projector, not a replacement for it.
func resolveActiveManualMembershipPairs(
	ctx context.Context, conn driver.Conn, orgID, provider string,
) (map[membershipConflictPair]struct{}, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(provider) == "" {
		return nil, ErrInvalidConfiguration
	}
	rows, err := conn.Query(ctx, activeManualMembershipsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pairs := make(map[membershipConflictPair]struct{})
	for rows.Next() {
		var memberID, teamID string
		if err := rows.Scan(&memberID, &teamID); err != nil {
			return nil, err
		}
		pairs[membershipConflictPair{MemberID: memberID, TeamID: teamID}] = struct{}{}
	}
	return pairs, rows.Err()
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
// must be skipped: either its exact (member_id, team_id) pair already has an
// active MANUAL membership (writing the native row would not override a
// manual pin -- the projector's job, not this guard's -- so skip it instead
// of racing it), or the member's own identity (provider id, raw email, or
// any resolved facet) has an active member-scoped manual_attribution_
// fallbacks row, regardless of which team that fallback names (fallbacks are
// member-scoped, not team-scoped, same as clickhouse_identity_drift.py's own
// _conflict_for fallback branch).
func membershipConflictsWithManualState(
	row linearReferenceMembershipRow,
	manualPairs map[membershipConflictPair]struct{},
	fallbackIdentities map[string]struct{},
) bool {
	if _, conflict := manualPairs[membershipConflictPair{MemberID: row.MemberID, TeamID: row.TeamID}]; conflict {
		return true
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
	manualPairs, err := resolveActiveManualMembershipPairs(ctx, conn, orgID, provider)
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
		if membershipConflictsWithManualState(row, manualPairs, fallbackIdentities) {
			skipped++
			continue
		}
		kept = append(kept, row)
	}
	return kept, skipped, nil
}
