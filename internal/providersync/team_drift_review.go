package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// team_drift_review.go is the shared, provider-agnostic engine CHAOS-4444
// builds to replace the interim CHAOS-4431 sync_policy guard's plain
// skip-the-write behavior with clickhouse_team_drift_projector.py's actual
// review-staging behavior: a team whose org has sync_policy
// FLAG_FOR_REVIEW(1) or MANUAL(2) is not just left untouched -- the diff
// against the currently-persisted row is computed and staged as a
// team_drift_changes row an admin can approve/dismiss (via the existing
// Python-owned admin API, api/admin/routers/teams.py, unaffected by this
// change: it reads/writes the SAME ClickHouse tables regardless of which
// language staged a row).
//
// Every native team-catalog collector (Linear, GitHub, GitLab) delegates to
// reviewTeamRowsForDrift through its own thin, provider-typed wrapper (see
// team_sync_policy_guard.go's applyTeamSyncPolicyGuard and its GitHub/
// GitLab mirrors), the same shape CHAOS-4431's original guard already used.

const (
	teamDriftAutoApplyPolicy     = 0
	teamDriftFlagForReviewPolicy = 1
	teamDriftManualPolicy        = 2

	teamDriftEntityTypeTeam   = "team"
	teamDriftFieldChangedType = "field_changed"
	teamDriftStatusPending    = "pending"
	teamDriftStatusApproved   = "approved"
	teamDriftStatusDismissed  = "dismissed"
	teamDriftStatusResolved   = "resolved"
	teamDriftStatusSuperseded = "superseded"
)

// teamDriftManagedFields mirrors clickhouse_team_drift_projector.py's
// DEFAULT_MANAGED_FIELDS exactly, in the same order (the loop order is not
// behaviorally significant -- every field is always visited -- but keeping
// it identical makes a side-by-side diff against the Python source trivial).
var teamDriftManagedFields = []string{"name", "description", "members", "project_keys", "repo_patterns"}

func isTeamDriftDecidedStatus(status string) bool {
	return status == teamDriftStatusApproved || status == teamDriftStatusDismissed
}

// teamDriftTeamView is the generic, provider-agnostic projection of a team
// row's managed fields the review engine operates on -- mirrors Python's
// team_row dict shape (_observed_row reads exactly these keys off it).
// Every native collector's own row type (linearReferenceTeamRow,
// githubTeamRow, gitlabTeamCatalogTeamRow) converts to this via a small,
// provider-specific adapter; the engine itself never imports a
// provider-typed row.
type teamDriftTeamView struct {
	ID            string
	Provider      string
	NativeTeamKey string
	Name          *string
	Description   *string
	Members       []string
	ProjectKeys   []string
	RepoPatterns  []string
	IsActive      bool
	ParentTeamID  *string
}

func linearTeamRowToDriftView(row linearReferenceTeamRow) teamDriftTeamView {
	name := row.Name
	return teamDriftTeamView{
		ID: row.ID, Provider: row.Provider, NativeTeamKey: teamDriftNativeKey(row.NativeTeamKey, row.ID),
		Name: &name, Description: row.Description,
		Members: row.Members, ProjectKeys: row.ProjectKeys, RepoPatterns: row.RepoPatterns,
		IsActive: row.IsActive != 0, ParentTeamID: row.ParentTeamID,
	}
}

func githubTeamRowToDriftView(row githubTeamRow) teamDriftTeamView {
	name := row.Name
	return teamDriftTeamView{
		ID: row.ID, Provider: row.Provider, NativeTeamKey: teamDriftNativeKey(row.NativeTeamKey, row.ID),
		Name: &name, Description: row.Description,
		Members: row.Members, ProjectKeys: row.ProjectKeys, RepoPatterns: row.RepoPatterns,
		IsActive: row.IsActive != 0, ParentTeamID: nil,
	}
}

func gitlabTeamRowToDriftView(row gitlabTeamCatalogTeamRow) teamDriftTeamView {
	name := row.Name
	return teamDriftTeamView{
		ID: row.ID, Provider: row.Provider, NativeTeamKey: teamDriftNativeKey(row.NativeTeamKey, row.ID),
		Name: &name, Description: row.Description,
		Members: row.Members, ProjectKeys: row.ProjectKeys, RepoPatterns: row.RepoPatterns,
		IsActive: row.IsActive != 0, ParentTeamID: row.ParentTeamID,
	}
}

// teamDriftNativeKey mirrors _observed_row's
// `team_row.get("native_team_key") or team_row.get("id") or ""` fallback.
func teamDriftNativeKey(nativeTeamKey *string, id string) string {
	if nativeTeamKey != nil && *nativeTeamKey != "" {
		return *nativeTeamKey
	}
	return id
}

// teamDriftChangeRow mirrors every column of the team_drift_changes table
// (migration 058) -- shared by both the team-level and identity-level
// review engines (team_drift_review.go / identity_drift_review.go).
type teamDriftChangeRow struct {
	OrgID         string
	ChangeID      string
	EntityType    string
	EntityID      string
	Provider      string
	NativeTeamKey *string
	ChangeType    string
	Field         *string
	OldValueJSON  string
	NewValueJSON  string
	Status        string
	FirstSeenAt   time.Time
	LastSeenAt    time.Time
	DecidedAt     *time.Time
	DecidedBy     *string
	UpdatedAt     time.Time
}

const teamDriftChangesInsert = "INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, decided_at, decided_by, updated_at)"

// insertTeamDriftChanges batch-inserts one or more team_drift_changes row
// versions (ReplacingMergeTree keyed on (org_id, change_id) -- every insert
// here, including a bare status transition, is a full new row version,
// mirroring storage/clickhouse.py's insert_team_drift_changes).
func insertTeamDriftChanges(ctx context.Context, conn driver.Conn, rows []teamDriftChangeRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, teamDriftChangesInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.ChangeID, row.EntityType, row.EntityID, row.Provider, row.NativeTeamKey,
			row.ChangeType, row.Field, row.OldValueJSON, row.NewValueJSON, row.Status,
			row.FirstSeenAt, row.LastSeenAt, row.DecidedAt, row.DecidedBy, row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

const teamProviderObservationsInsert = "INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at)"

// insertTeamProviderObservations batch-inserts one team_provider_observations
// row per observed team (migration 057), unconditionally -- mirrors
// project_team's _insert_observation call, which happens for EVERY team
// regardless of sync_policy (the observation table is a raw audit trail of
// what the provider reported, independent of what got applied or staged).
func insertTeamProviderObservations(ctx context.Context, conn driver.Conn, orgID string, views []teamDriftTeamView, now time.Time) error {
	if len(views) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, teamProviderObservationsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, view := range views {
		isActive := uint8(0)
		if view.IsActive {
			isActive = 1
		}
		if err := batch.Append(
			orgID, view.Provider, view.NativeTeamKey, view.ID, view.Name, view.Description,
			pyCanonicalJSON(pyComparisonListField(view.Members)),
			pyCanonicalJSON(pyComparisonListField(view.ProjectKeys)),
			pyCanonicalJSON(pyComparisonListField(view.RepoPatterns)),
			isActive, view.ParentTeamID, now, now,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// teamDriftExistingRow is the subset of a persisted `teams` row's managed
// fields the review engine diffs an observation against.
type teamDriftExistingRow struct {
	Name         *string
	Description  *string
	Members      []string
	ProjectKeys  []string
	RepoPatterns []string
}

const teamDriftExistingRowsQuery = "SELECT id, name, description, members, project_keys, repo_patterns FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}"

func fetchTeamDriftExistingRows(ctx context.Context, conn driver.Conn, orgID string, teamIDs []string) (map[string]teamDriftExistingRow, error) {
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamDriftExistingRowsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := make(map[string]teamDriftExistingRow, len(teamIDs))
	for rows.Next() {
		var id string
		var name, description *string
		var members, projectKeys, repoPatterns []string
		if err := rows.Scan(&id, &name, &description, &members, &projectKeys, &repoPatterns); err != nil {
			return nil, err
		}
		existing[id] = teamDriftExistingRow{
			Name: name, Description: description,
			Members: members, ProjectKeys: projectKeys, RepoPatterns: repoPatterns,
		}
	}
	return existing, rows.Err()
}

const teamSyncPolicyManagedFieldsQuery = "SELECT team_id, sync_policy, managed_fields FROM team_sync_policies FINAL WHERE org_id = {org_id:String} AND team_id IN {team_ids:Array(String)}"

type teamDriftPolicy struct {
	Policy        int
	ManagedFields []string
}

func fetchTeamDriftPolicies(ctx context.Context, conn driver.Conn, orgID string, teamIDs []string) (map[string]teamDriftPolicy, error) {
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamSyncPolicyManagedFieldsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policies := make(map[string]teamDriftPolicy, len(teamIDs))
	for rows.Next() {
		var teamID string
		var policy uint8
		var managedFields []string
		if err := rows.Scan(&teamID, &policy, &managedFields); err != nil {
			return nil, err
		}
		policies[teamID] = teamDriftPolicy{Policy: int(policy), ManagedFields: normalizeManagedFields(managedFields)}
	}
	return policies, rows.Err()
}

// normalizeManagedFields mirrors _managed_fields: an empty or entirely-
// unrecognized managed_fields column falls back to the full default set;
// otherwise it is filtered down to only the recognized field names, order
// preserved from teamDriftManagedFields (not from the stored column) so a
// diff always visits fields in the same, predictable order.
func normalizeManagedFields(stored []string) []string {
	if len(stored) == 0 {
		return teamDriftManagedFields
	}
	allowed := make(map[string]struct{}, len(stored))
	for _, field := range stored {
		allowed[field] = struct{}{}
	}
	out := make([]string, 0, len(teamDriftManagedFields))
	for _, field := range teamDriftManagedFields {
		if _, ok := allowed[field]; ok {
			out = append(out, field)
		}
	}
	if len(out) == 0 {
		return teamDriftManagedFields
	}
	return out
}

const teamDriftTeamChangesQuery = `
SELECT change_id, entity_id, provider, native_team_key, field, old_value_json, new_value_json, status, first_seen_at
FROM team_drift_changes FINAL
WHERE org_id = {org_id:String} AND entity_type = 'team' AND change_type = 'field_changed' AND entity_id IN {team_ids:Array(String)}`

func fetchTeamDriftTeamChanges(ctx context.Context, conn driver.Conn, orgID string, teamIDs []string) (map[string][]teamDriftChangeRow, error) {
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamDriftTeamChangesQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byTeam := make(map[string][]teamDriftChangeRow)
	for rows.Next() {
		var row teamDriftChangeRow
		var field *string
		if err := rows.Scan(&row.ChangeID, &row.EntityID, &row.Provider, &row.NativeTeamKey, &field,
			&row.OldValueJSON, &row.NewValueJSON, &row.Status, &row.FirstSeenAt); err != nil {
			return nil, err
		}
		row.OrgID = orgID
		row.EntityType = teamDriftEntityTypeTeam
		row.ChangeType = teamDriftFieldChangedType
		row.Field = field
		byTeam[row.EntityID] = append(byTeam[row.EntityID], row)
	}
	return byTeam, rows.Err()
}

func teamFieldValue(field string) func(teamDriftTeamView) any {
	switch field {
	case "name":
		return func(v teamDriftTeamView) any { return v.Name }
	case "description":
		return func(v teamDriftTeamView) any { return v.Description }
	case "members":
		return func(v teamDriftTeamView) any { return pyComparisonListField(v.Members) }
	case "project_keys":
		return func(v teamDriftTeamView) any { return pyComparisonListField(v.ProjectKeys) }
	case "repo_patterns":
		return func(v teamDriftTeamView) any { return pyComparisonListField(v.RepoPatterns) }
	default:
		return func(teamDriftTeamView) any { return nil }
	}
}

func existingFieldValue(existing *teamDriftExistingRow, field string) any {
	if existing == nil {
		switch field {
		case "members", "project_keys", "repo_patterns":
			return []string{}
		default:
			return nil
		}
	}
	switch field {
	case "name":
		return existing.Name
	case "description":
		return existing.Description
	case "members":
		return pyComparisonListField(existing.Members)
	case "project_keys":
		return pyComparisonListField(existing.ProjectKeys)
	case "repo_patterns":
		return pyComparisonListField(existing.RepoPatterns)
	default:
		return nil
	}
}

func changeIDForTeamField(orgID, teamID, field, oldValueJSON, newValueJSON string) string {
	payload := map[string]any{
		"org_id": orgID, "entity_type": teamDriftEntityTypeTeam, "entity_id": teamID,
		"change_type": teamDriftFieldChangedType, "field": field,
		"old_value_json": oldValueJSON, "new_value_json": newValueJSON,
	}
	return sha256Hex(pyCanonicalJSON(payload))
}

// orNull mirrors Python's `row.get("old_value_json") or "null"` -- an empty
// stored value (should not occur against a schema where the column is a
// non-nullable String, but the fallback costs nothing) reads back as the
// JSON literal null rather than an empty string.
func orNull(value string) string {
	if value == "" {
		return "null"
	}
	return value
}

// teamDriftStatusRow mirrors _status_rows: carries an existing change row
// forward under a new status, preserving its stored old/new JSON and
// first_seen_at (or defaulting first_seen_at to now if it was never set).
// This engine only ever transitions to RESOLVED or SUPERSEDED, never a
// DECIDED status (that is the admin-approval action, still Python-owned),
// so decided_at/decided_by are always cleared.
func teamDriftStatusRow(existing teamDriftChangeRow, status string, now time.Time) teamDriftChangeRow {
	row := existing
	row.Status = status
	row.OldValueJSON = orNull(row.OldValueJSON)
	row.NewValueJSON = orNull(row.NewValueJSON)
	if row.FirstSeenAt.IsZero() {
		row.FirstSeenAt = now
	}
	row.LastSeenAt = now
	row.UpdatedAt = now
	row.DecidedAt = nil
	row.DecidedBy = nil
	return row
}

// reviewTeamRowsForDrift is the shared engine every provider's
// applyXTeamSyncPolicyGuard wrapper delegates to. For every team, it always
// records a team_provider_observations row (project_team's unconditional
// observation insert), then per-team:
//
//   - AUTO_APPLY_POLICY (0, the default): the row is safe to write through
//     as before (returned via keptIdx) and any PENDING team_drift_changes
//     rows for this team are marked RESOLVED (mirrors project_team's
//     `_mark_pending(..., status=STATUS_RESOLVED)` on the auto-apply path --
//     a team switched back to auto-apply, or whose drift resolved itself,
//     has nothing left pending for review).
//   - FLAG_FOR_REVIEW_POLICY (1): the row is EXCLUDED from the write
//     (native_team_key returned in skippedTeamIDs, same contract the
//     interim guard already had), and each of the team's managed_fields is
//     diffed against the currently-persisted row -- a changed field stages
//     a new PENDING team_drift_changes row (or refreshes last_seen_at on an
//     already-pending one with the identical diff), superseding any stale
//     pending row for that field with a DIFFERENT diff, and resolving any
//     pending row for a field that no longer differs. A field whose
//     existing change is already approved/dismissed is left alone (the
//     admin's decision stands until the next actual diff).
//   - MANUAL_POLICY (2) (or any other non-0/1 value): mirrors
//     project_team's own `if policy != FLAG_FOR_REVIEW_POLICY or not
//     detect_drift: return observed` exactly -- the row is EXCLUDED from
//     the write, same as FLAG_FOR_REVIEW_POLICY, but NOTHING is staged,
//     superseded, or resolved. The team is admin-owned end-to-end; this
//     call's only effect on it is the unconditional observation insert
//     every team gets regardless of policy (codex review round 1, P1, PR
//     #2002 -- an earlier revision of this function incorrectly ran the
//     diff/staging path for policy 2 too).
//
// Returns the indices (into teams, in order) safe to write through,
// the native_team_key of every skipped team, how many DISTINCT teams got at
// least one freshly staged/refreshed pending row this call, and how many
// team_drift_changes rows (any entity type -- identity_drift_review.go adds
// to the same running total) were superseded.
func reviewTeamRowsForDrift(
	ctx context.Context, conn driver.Conn, orgID string, teams []teamDriftTeamView, now time.Time,
) (keptIdx []int, skippedTeamIDs []string, stagedTeams int, superseded int, err error) {
	if len(teams) == 0 {
		return nil, nil, 0, 0, nil
	}
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, nil, 0, 0, ErrInvalidConfiguration
	}

	if err := insertTeamProviderObservations(ctx, conn, orgID, teams, now); err != nil {
		return nil, nil, 0, 0, err
	}

	teamIDs := make([]string, 0, len(teams))
	for _, team := range teams {
		teamIDs = append(teamIDs, team.ID)
	}
	policies, err := fetchTeamDriftPolicies(ctx, conn, orgID, teamIDs)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	existingChanges, err := fetchTeamDriftTeamChanges(ctx, conn, orgID, teamIDs)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	// reviewTeamIDs is scoped to FLAG_FOR_REVIEW_POLICY(1) teams ONLY --
	// mirrors project_team's `if policy != FLAG_FOR_REVIEW_POLICY or not
	// detect_drift: return observed`: MANUAL_POLICY(2) returns immediately,
	// before ever reading the persisted row or diffing anything. A team
	// whose policy is neither 0 nor 1 must never reach the existing-row
	// read below.
	reviewTeamIDs := make([]string, 0, len(teams))
	for _, team := range teams {
		policy, ok := policies[team.ID]
		if !ok || policy.Policy != teamDriftFlagForReviewPolicy {
			continue
		}
		reviewTeamIDs = append(reviewTeamIDs, team.ID)
	}
	existingRows, err := fetchTeamDriftExistingRows(ctx, conn, orgID, reviewTeamIDs)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	var toInsert []teamDriftChangeRow
	for index, team := range teams {
		policy := teamDriftPolicy{Policy: teamDriftAutoApplyPolicy, ManagedFields: teamDriftManagedFields}
		if resolved, ok := policies[team.ID]; ok {
			policy = resolved
		}

		if policy.Policy == teamDriftAutoApplyPolicy {
			keptIdx = append(keptIdx, index)
			for _, existingChange := range existingChanges[team.ID] {
				if existingChange.Status != teamDriftStatusPending {
					continue
				}
				toInsert = append(toInsert, teamDriftStatusRow(existingChange, teamDriftStatusResolved, now))
			}
			continue
		}

		skippedTeamIDs = append(skippedTeamIDs, team.ID)

		if policy.Policy != teamDriftFlagForReviewPolicy {
			// MANUAL_POLICY(2) (or any future non-0/1 value): mirrors
			// project_team's own `policy != FLAG_FOR_REVIEW_POLICY` early
			// return exactly -- no diff, no staging, no resolve/supersede
			// of any existing pending change. The team is admin-owned
			// end-to-end; this call's only effect on it was the
			// unconditional observation insert above.
			continue
		}

		existingRow, hasExisting := existingRows[team.ID]
		var existingRowPtr *teamDriftExistingRow
		if hasExisting {
			existingRowPtr = &existingRow
		}

		pendingByField := make(map[string][]teamDriftChangeRow)
		changesByID := make(map[string]teamDriftChangeRow)
		for _, change := range existingChanges[team.ID] {
			changesByID[change.ChangeID] = change
			if change.Status == teamDriftStatusPending && change.Field != nil {
				pendingByField[*change.Field] = append(pendingByField[*change.Field], change)
			}
		}

		teamStaged := false
		for _, field := range policy.ManagedFields {
			oldValue := existingFieldValue(existingRowPtr, field)
			newValue := teamFieldValue(field)(team)
			oldJSON := pyCanonicalJSON(oldValue)
			newJSON := pyCanonicalJSON(newValue)
			fieldPending := pendingByField[field]

			if oldJSON == newJSON {
				for _, pending := range fieldPending {
					toInsert = append(toInsert, teamDriftStatusRow(pending, teamDriftStatusResolved, now))
				}
				continue
			}

			changeID := changeIDForTeamField(orgID, team.ID, field, oldJSON, newJSON)
			for _, pending := range fieldPending {
				if pending.ChangeID == changeID {
					continue
				}
				toInsert = append(toInsert, teamDriftStatusRow(pending, teamDriftStatusSuperseded, now))
				superseded++
			}

			existingChange, hasExistingChange := changesByID[changeID]
			if hasExistingChange && isTeamDriftDecidedStatus(existingChange.Status) {
				continue
			}
			firstSeenAt := now
			if hasExistingChange && existingChange.Status == teamDriftStatusPending && !existingChange.FirstSeenAt.IsZero() {
				firstSeenAt = existingChange.FirstSeenAt
			}
			fieldCopy := field
			nativeTeamKey := team.NativeTeamKey
			toInsert = append(toInsert, teamDriftChangeRow{
				OrgID: orgID, ChangeID: changeID, EntityType: teamDriftEntityTypeTeam, EntityID: team.ID,
				Provider: team.Provider, NativeTeamKey: &nativeTeamKey,
				ChangeType: teamDriftFieldChangedType, Field: &fieldCopy,
				OldValueJSON: oldJSON, NewValueJSON: newJSON,
				Status: teamDriftStatusPending, FirstSeenAt: firstSeenAt, LastSeenAt: now, UpdatedAt: now,
			})
			teamStaged = true
		}
		if teamStaged {
			stagedTeams++
		}
	}

	if err := insertTeamDriftChanges(ctx, conn, toInsert); err != nil {
		return nil, nil, 0, 0, err
	}
	return keptIdx, skippedTeamIDs, stagedTeams, superseded, nil
}
