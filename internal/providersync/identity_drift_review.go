package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// identity_drift_review.go is CHAOS-4444's shared engine for
// clickhouse_identity_drift.py's split_memberships_for_review: instead of
// the interim CHAOS-4431 membership-conflict guard's plain skip (which
// already correctly EXCLUDES a provider_access row that fights an active
// manual team_memberships row or manual_attribution_fallbacks row for the
// same member -- see team_membership_conflict_guard.go, unchanged by this
// file), a conflicting row is now ALSO staged as a STATUS_PENDING
// team_drift_changes row (entity_type='identity') an admin can approve,
// and a previously-staged pending row for a member no longer observed (or
// no longer conflicting) this run is resolved/superseded.
//
// The exclusion decision itself -- which rows are unsafe to write --
// stays exactly what team_membership_conflict_guard.go already computes
// (membershipConflictsWithManualState against
// resolveActiveManualMembershipTeams/resolveActiveMemberAttributionFallbackTeams):
// this file only adds the staging/stale-resolution side effects on top,
// deliberately not touching the already-shipped, already-tested exclusion
// logic across three merged PRs.

const (
	identityDriftEntityType          = "identity"
	identityDriftMembershipChangedT  = "membership_changed"
	identityDriftFieldTeamMembership = "team_memberships"
	identityDriftFieldMemberFallback = "manual_attribution_fallbacks.member"
)

// teamDriftMembershipView is the generic, provider-agnostic projection of a
// native membership row the identity-drift engine operates on -- mirrors
// linearReferenceMembershipRow/githubMembershipRow/gitlabTeamCatalogMembershipRow,
// which are already structurally identical across all three providers.
type teamDriftMembershipView struct {
	Provider          string
	TeamID            string
	MemberID          string
	RawProviderUserID *string
	RawEmail          *string
	IdentityFacets    []string
	Source            string
	IsPrimary         uint8
	Specificity       uint16
	Priority          int32
	ValidFrom         time.Time
	ValidTo           *time.Time
	UpdatedAt         time.Time
}

func linearMembershipRowToDriftView(row linearReferenceMembershipRow) teamDriftMembershipView {
	return teamDriftMembershipView{
		Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID,
		RawProviderUserID: row.RawProviderUserID, RawEmail: row.RawEmail, IdentityFacets: row.IdentityFacets,
		Source: row.Source, IsPrimary: row.IsPrimary, Specificity: row.Specificity, Priority: row.Priority,
		ValidFrom: row.ValidFrom, ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt,
	}
}

func githubMembershipRowToDriftView(row githubMembershipRow) teamDriftMembershipView {
	return teamDriftMembershipView{
		Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID,
		RawProviderUserID: row.RawProviderUserID, RawEmail: row.RawEmail, IdentityFacets: row.IdentityFacets,
		Source: row.Source, IsPrimary: row.IsPrimary, Specificity: row.Specificity, Priority: row.Priority,
		ValidFrom: row.ValidFrom, ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt,
	}
}

func gitlabMembershipRowToDriftView(row gitlabTeamCatalogMembershipRow) teamDriftMembershipView {
	return teamDriftMembershipView{
		Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID,
		RawProviderUserID: row.RawProviderUserID, RawEmail: row.RawEmail, IdentityFacets: row.IdentityFacets,
		Source: row.Source, IsPrimary: row.IsPrimary, Specificity: row.Specificity, Priority: row.Priority,
		ValidFrom: row.ValidFrom, ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt,
	}
}

// membershipRowJSON mirrors _row_dict's OBJECT branch (our incoming rows are
// always struct-typed, never dict-typed): org_id, provider, team_id,
// member_id, raw_provider_user_id, raw_email, source, is_primary,
// specificity, priority, valid_from, valid_to, updated_at -- deliberately
// NOT including identity_facets, which _row_dict's attribute allowlist
// omits for a non-dict row.
func membershipRowJSON(orgID string, row teamDriftMembershipView) map[string]any {
	return map[string]any{
		"org_id": orgID, "provider": row.Provider, "team_id": row.TeamID, "member_id": row.MemberID,
		"raw_provider_user_id": row.RawProviderUserID, "raw_email": row.RawEmail, "source": row.Source,
		"is_primary": row.IsPrimary, "specificity": row.Specificity, "priority": row.Priority,
		"valid_from": row.ValidFrom, "valid_to": row.ValidTo, "updated_at": row.UpdatedAt,
	}
}

// manualMembershipRow is the full row shape _manual_memberships reads back
// (team_memberships FINAL, source='manual', not yet expired) -- column
// order matches that query exactly.
type manualMembershipRow struct {
	OrgID             string
	Provider          string
	TeamID            string
	MemberID          string
	RawProviderUserID *string
	RawEmail          *string
	Source            string
	IsPrimary         uint8
	Specificity       uint16
	Priority          int32
	ValidFrom         time.Time
	ValidTo           *time.Time
	UpdatedAt         time.Time
}

func (row manualMembershipRow) json() map[string]any {
	return map[string]any{
		"org_id": row.OrgID, "provider": row.Provider, "team_id": row.TeamID, "member_id": row.MemberID,
		"raw_provider_user_id": row.RawProviderUserID, "raw_email": row.RawEmail, "source": row.Source,
		"is_primary": row.IsPrimary, "specificity": row.Specificity, "priority": row.Priority,
		"valid_from": row.ValidFrom, "valid_to": row.ValidTo, "updated_at": row.UpdatedAt,
	}
}

const manualMembershipRowsQuery = `
SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at
FROM team_memberships FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND source = 'manual' AND (valid_to IS NULL OR valid_to > now())
ORDER BY member_id, team_id`

func fetchManualMembershipRows(ctx context.Context, conn driver.Conn, orgID, provider string) ([]manualMembershipRow, error) {
	rows, err := conn.Query(ctx, manualMembershipRowsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []manualMembershipRow
	for rows.Next() {
		var row manualMembershipRow
		if err := rows.Scan(&row.OrgID, &row.Provider, &row.TeamID, &row.MemberID, &row.RawProviderUserID,
			&row.RawEmail, &row.Source, &row.IsPrimary, &row.Specificity, &row.Priority,
			&row.ValidFrom, &row.ValidTo, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// memberFallbackRow is the full row shape _member_fallbacks reads back
// (manual_attribution_fallbacks FINAL, scope_type='member', not yet
// expired) -- column order matches that query exactly.
type memberFallbackRow struct {
	OrgID     string
	Provider  string
	ScopeType string
	ScopeID   string
	TeamID    string
	TeamName  string
	Reason    string
	Priority  int32
	ValidFrom time.Time
	ValidTo   *time.Time
	CreatedBy *string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (row memberFallbackRow) json() map[string]any {
	return map[string]any{
		"org_id": row.OrgID, "provider": row.Provider, "scope_type": row.ScopeType, "scope_id": row.ScopeID,
		"team_id": row.TeamID, "team_name": row.TeamName, "reason": row.Reason, "priority": row.Priority,
		"valid_from": row.ValidFrom, "valid_to": row.ValidTo, "created_by": row.CreatedBy,
		"created_at": row.CreatedAt, "updated_at": row.UpdatedAt,
	}
}

const memberFallbackRowsQuery = `
SELECT org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, created_by, created_at, updated_at
FROM manual_attribution_fallbacks FINAL
WHERE org_id = {org_id:String} AND provider = {provider:String}
  AND scope_type = 'member' AND (valid_to IS NULL OR valid_to > now())
ORDER BY scope_id, team_id`

func fetchMemberFallbackRows(ctx context.Context, conn driver.Conn, orgID, provider string) ([]memberFallbackRow, error) {
	rows, err := conn.Query(ctx, memberFallbackRowsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", provider))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []memberFallbackRow
	for rows.Next() {
		var row memberFallbackRow
		if err := rows.Scan(&row.OrgID, &row.Provider, &row.ScopeType, &row.ScopeID, &row.TeamID, &row.TeamName,
			&row.Reason, &row.Priority, &row.ValidFrom, &row.ValidTo, &row.CreatedBy, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// membershipConflictDetail is the specific conflicting record a staged
// team_drift_changes row's old_value_json describes -- mirrors
// _conflict_for's return dict shape ({"field": ..., "manual_membership":
// ...} or {"field": ..., "manual_fallback": ...}).
type membershipConflictDetail struct {
	Field string
	Value map[string]any
}

// conflictDetailForMembership finds the SPECIFIC conflicting row for a
// membership already known to conflict (team_membership_conflict_guard.go's
// membershipConflictsWithManualState said true), so the staged change's
// old_value_json can carry it. Mirrors _conflict_for's own iteration
// exactly: every manual-membership candidate for this member is checked
// (same-team is a confirmation, skip to the next candidate; the FIRST
// different-team row found is returned), falling back to the
// fallback-table check using the SAME candidate identity set
// membershipConflictsWithManualState already used (raw_provider_user_id,
// raw_email, every identity_facets entry -- team-lead ruling 2026-08-28
// extended this beyond _conflict_for's narrower 3-facet Python set for the
// EXCLUSION decision; the staged detail simply reflects whichever candidate
// the (already shipped, already tested) exclusion decision actually used,
// rather than silently narrowing it back down and risking "excluded but no
// detail found").
func conflictDetailForMembership(
	row teamDriftMembershipView, manualRows []manualMembershipRow, fallbackRows []memberFallbackRow,
) *membershipConflictDetail {
	for _, manual := range manualRows {
		if manual.Provider != row.Provider || manual.MemberID != row.MemberID {
			continue
		}
		if manual.TeamID == row.TeamID {
			continue
		}
		return &membershipConflictDetail{Field: identityDriftFieldTeamMembership, Value: manual.json()}
	}

	candidates := make([]string, 0, len(row.IdentityFacets)+2)
	if row.RawProviderUserID != nil {
		candidates = append(candidates, *row.RawProviderUserID)
	}
	if row.RawEmail != nil {
		candidates = append(candidates, *row.RawEmail)
	}
	candidates = append(candidates, row.IdentityFacets...)
	candidateSet := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if normalized := normalizeMembershipIdentity(candidate); normalized != "" {
			candidateSet[normalized] = struct{}{}
		}
	}
	for _, fallback := range fallbackRows {
		if fallback.Provider != row.Provider {
			continue
		}
		if _, ok := candidateSet[normalizeMembershipIdentity(fallback.ScopeID)]; !ok {
			continue
		}
		if fallback.TeamID == row.TeamID {
			continue
		}
		return &membershipConflictDetail{Field: identityDriftFieldMemberFallback, Value: fallback.json()}
	}
	return nil
}

func changeIDForIdentityMembership(orgID, teamID, provider, memberID, field, oldValueJSON, newValueJSON string) string {
	payload := map[string]any{
		"org_id": orgID, "entity_type": identityDriftEntityType, "entity_id": teamID,
		"provider": provider, "member_id": memberID, "change_type": identityDriftMembershipChangedT,
		"field": field, "old_value_json": oldValueJSON, "new_value_json": newValueJSON,
	}
	return sha256Hex(pyCanonicalJSON(payload))
}

const identityDriftChangesQuery = `
SELECT change_id, entity_id, provider, native_team_key, field, old_value_json, new_value_json, status, first_seen_at
FROM team_drift_changes FINAL
WHERE org_id = {org_id:String} AND entity_type = 'identity' AND change_type = 'membership_changed'`

// fetchIdentityDriftChanges mirrors _identity_changes: ORG-scoped, not
// filtered by provider (a provider's own review pass only ever acts on the
// rows whose embedded new_value_json resolves to ITS OWN provider -- see
// reviewMembershipsForDrift's observed-scope check -- so reading the whole
// org's identity changes here is safe, matching Python exactly).
func fetchIdentityDriftChanges(ctx context.Context, conn driver.Conn, orgID string) ([]teamDriftChangeRow, error) {
	rows, err := conn.Query(ctx, identityDriftChangesQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []teamDriftChangeRow
	for rows.Next() {
		var row teamDriftChangeRow
		var field *string
		if err := rows.Scan(&row.ChangeID, &row.EntityID, &row.Provider, &row.NativeTeamKey, &field,
			&row.OldValueJSON, &row.NewValueJSON, &row.Status, &row.FirstSeenAt); err != nil {
			return nil, err
		}
		row.OrgID = orgID
		row.EntityType = identityDriftEntityType
		row.ChangeType = identityDriftMembershipChangedT
		row.Field = field
		result = append(result, row)
	}
	return result, rows.Err()
}

// changeMemberKey mirrors _change_member_key: the (provider, team_id,
// member_id) key of a PERSISTED team_drift_changes row, recovered by
// decoding its own new_value_json (the row shape membershipRowJSON wrote --
// see that function's doc comment for why entity_id/provider on the change
// row itself are not enough: entity_id is the TEAM id, and member_id lives
// only inside the JSON payload).
func changeMemberKey(row teamDriftChangeRow) (provider, teamID, memberID string) {
	var decoded struct {
		Provider string `json:"provider"`
		TeamID   string `json:"team_id"`
		MemberID string `json:"member_id"`
	}
	if json.Unmarshal([]byte(row.NewValueJSON), &decoded) == nil && decoded.MemberID != "" {
		return decoded.Provider, decoded.TeamID, decoded.MemberID
	}
	return row.Provider, row.EntityID, ""
}

type membershipKey struct {
	Provider string
	TeamID   string
	MemberID string
}

type membershipScope struct {
	Provider string
	TeamID   string
}

// reviewMembershipsForDrift is the shared engine every provider's
// applyXTeamMembershipConflictGuard wrapper calls after computing its own
// (unchanged) manual/fallback conflict exclusion. rows is the FULL incoming
// membership batch (safe and conflicting alike -- mirrors
// split_memberships_for_review's own `rows` parameter); conflictedIdx names
// which of those indices membershipConflictsWithManualState already
// excluded; observedTeamIDs is this run's own team ids whose member fetch
// succeeded (mirrors Python's observed_team_ids -- used only to scope
// stale-pending resolution, see fetchIdentityDriftChanges's doc comment).
//
// Every conflicting row not already decided (approved/dismissed) stages a
// fresh (or last_seen_at-refreshed) STATUS_PENDING team_drift_changes row.
// Every existing pending row for a (provider, team, member) this run
// observed but that either (a) no longer appears in rows at all (member
// removed from the provider) or (b) is no longer conflicting is RESOLVED;
// one that is STILL conflicting but whose specific diff changed (a
// different change_id) is SUPERSEDED.
func reviewMembershipsForDrift(
	ctx context.Context, conn driver.Conn, orgID, provider string,
	rows []teamDriftMembershipView, conflictedIdx map[int]struct{}, observedTeamIDs []string, now time.Time,
) (staged int, superseded int, err error) {
	if len(rows) == 0 && len(observedTeamIDs) == 0 {
		return 0, 0, nil
	}
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return 0, 0, ErrInvalidConfiguration
	}

	manualRows, err := fetchManualMembershipRows(ctx, conn, orgID, provider)
	if err != nil {
		return 0, 0, err
	}
	fallbackRows, err := fetchMemberFallbackRows(ctx, conn, orgID, provider)
	if err != nil {
		return 0, 0, err
	}
	existingChanges, err := fetchIdentityDriftChanges(ctx, conn, orgID)
	if err != nil {
		return 0, 0, err
	}
	changesByID := make(map[string]teamDriftChangeRow, len(existingChanges))
	for _, change := range existingChanges {
		changesByID[change.ChangeID] = change
	}

	incomingKeys := make(map[membershipKey]struct{}, len(rows))
	observedScopes := make(map[membershipScope]struct{}, len(rows)+len(observedTeamIDs))
	for _, row := range rows {
		incomingKeys[membershipKey{Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID}] = struct{}{}
		if row.Provider != "" && row.TeamID != "" {
			observedScopes[membershipScope{Provider: row.Provider, TeamID: row.TeamID}] = struct{}{}
		}
	}
	for _, teamID := range observedTeamIDs {
		if teamID != "" {
			observedScopes[membershipScope{Provider: provider, TeamID: teamID}] = struct{}{}
		}
	}

	conflictedKeys := make(map[membershipKey]struct{}, len(conflictedIdx))
	refreshedChangeIDs := make(map[string]struct{})
	var toInsert []teamDriftChangeRow

	for index, row := range rows {
		if _, isConflicted := conflictedIdx[index]; !isConflicted {
			continue
		}
		key := membershipKey{Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID}
		conflictedKeys[key] = struct{}{}

		detail := conflictDetailForMembership(row, manualRows, fallbackRows)
		if detail == nil {
			// The exclusion decision found a conflict via a broader
			// candidate set than this detail lookup could reconstruct
			// (should not happen given both draw from the same fetch --
			// defensive only). Nothing to stage; the row stays excluded
			// from the write by the caller regardless.
			continue
		}
		oldJSON := pyCanonicalJSON(detail.Value)
		newJSON := pyCanonicalJSON(membershipRowJSON(orgID, row))
		changeID := changeIDForIdentityMembership(orgID, row.TeamID, row.Provider, row.MemberID, detail.Field, oldJSON, newJSON)

		existing, hasExisting := changesByID[changeID]
		if hasExisting && isTeamDriftDecidedStatus(existing.Status) {
			continue
		}
		firstSeenAt := now
		if hasExisting && existing.Status == teamDriftStatusPending && !existing.FirstSeenAt.IsZero() {
			firstSeenAt = existing.FirstSeenAt
		}
		refreshedChangeIDs[changeID] = struct{}{}
		nativeTeamKey := row.TeamID
		field := detail.Field
		toInsert = append(toInsert, teamDriftChangeRow{
			OrgID: orgID, ChangeID: changeID, EntityType: identityDriftEntityType, EntityID: row.TeamID,
			Provider: row.Provider, NativeTeamKey: &nativeTeamKey,
			ChangeType: identityDriftMembershipChangedT, Field: &field,
			OldValueJSON: oldJSON, NewValueJSON: newJSON,
			Status: teamDriftStatusPending, FirstSeenAt: firstSeenAt, LastSeenAt: now, UpdatedAt: now,
		})
		staged++
	}

	for _, change := range existingChanges {
		if change.Status != teamDriftStatusPending {
			continue
		}
		if _, refreshed := refreshedChangeIDs[change.ChangeID]; refreshed {
			continue
		}
		changeProvider, changeTeamID, changeMemberID := changeMemberKey(change)
		scope := membershipScope{Provider: changeProvider, TeamID: changeTeamID}
		if _, observed := observedScopes[scope]; !observed {
			continue
		}
		key := membershipKey{Provider: changeProvider, TeamID: changeTeamID, MemberID: changeMemberID}
		_, stillIncoming := incomingKeys[key]
		_, stillConflicted := conflictedKeys[key]
		status := teamDriftStatusResolved
		if stillIncoming && stillConflicted {
			status = teamDriftStatusSuperseded
			superseded++
		}
		toInsert = append(toInsert, teamDriftStatusRow(change, status, now))
	}

	if err := insertTeamDriftChanges(ctx, conn, toInsert); err != nil {
		return 0, 0, err
	}
	return staged, superseded, nil
}
