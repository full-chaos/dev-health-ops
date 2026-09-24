package teamsidentity

import (
	"context"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Team drift review (CHAOS-6312): ports ClickHouseTeamDriftService
// (clickhouse_team_drift.py) and the three routes teams.py:337-390 serves
// from it -- GET /teams/pending-changes and POST /teams/{id}/approve-changes
// and /dismiss-changes -- over the team_drift_changes / team_provider_
// observations rows POST /teams/import (drift.go) writes.

// reviewChangeRow is one pending team_drift_changes row joined with its
// team's name (_pending_rows' SELECT).
type reviewChangeRow struct {
	ChangeID      string
	EntityType    string
	TeamID        string // entity_id
	TeamName      string
	Provider      string
	NativeTeamKey *string
	ChangeType    string
	Field         *string
	OldValueJSON  string
	NewValueJSON  string
	FirstSeenAt   time.Time
	LastSeenAt    time.Time
}

// pendingReviewRows is ClickHouseTeamDriftService._pending_rows: every
// pending team/identity change (optionally one team's), newest first.
func (s Store) pendingReviewRows(ctx context.Context, orgID string, teamID *string) ([]reviewChangeRow, error) {
	conditions := `c.org_id = {org_id:String} AND c.entity_type IN ('team', 'identity') AND c.status = 'pending'`
	args := []any{clickhouse.Named("org_id", orgID)}
	if teamID != nil {
		conditions += ` AND c.entity_id = {team_id:String}`
		args = append(args, clickhouse.Named("team_id", *teamID))
	}
	rows, err := s.Conn.Query(ctx, `
		SELECT c.change_id, c.entity_type, c.entity_id AS team_id, coalesce(t.name, c.entity_id) AS team_name,
		       c.provider, c.native_team_key, c.change_type, c.field, c.old_value_json, c.new_value_json,
		       c.first_seen_at, c.last_seen_at
		FROM team_drift_changes AS c FINAL
		LEFT JOIN teams AS t FINAL ON t.org_id = c.org_id AND t.id = c.entity_id
		WHERE `+conditions+`
		ORDER BY c.first_seen_at DESC, c.change_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reviewChangeRow
	for rows.Next() {
		var row reviewChangeRow
		var teamName *string
		if err := rows.Scan(&row.ChangeID, &row.EntityType, &row.TeamID, &teamName, &row.Provider, &row.NativeTeamKey,
			&row.ChangeType, &row.Field, &row.OldValueJSON, &row.NewValueJSON, &row.FirstSeenAt, &row.LastSeenAt); err != nil {
			return nil, err
		}
		if teamName != nil {
			row.TeamName = *teamName
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// selectChangesForDecision is _select_changes_for_decision: nothing unless
// decideAll or explicit ids; decideAll takes every pending row for the team,
// otherwise the rows whose change_id was requested (in row order).
func (s Store) selectChangesForDecision(ctx context.Context, orgID, teamID string, changeIDs []string, decideAll bool) ([]reviewChangeRow, error) {
	if !decideAll && len(changeIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pendingReviewRows(ctx, orgID, &teamID)
	if err != nil {
		return nil, err
	}
	if decideAll {
		return rows, nil
	}
	requested := toSet(changeIDs)
	var out []reviewChangeRow
	for _, row := range rows {
		if requested[row.ChangeID] {
			out = append(out, row)
		}
	}
	return out, nil
}

// loadsJSONValue is _loads_json: None/"" -> None, valid JSON -> its value,
// invalid JSON -> the raw string.
func loadsJSONValue(raw string) pyjson.Value {
	if raw == "" {
		return nil
	}
	decoded, err := pyjson.DecodeString(raw)
	if err != nil {
		return raw
	}
	return decoded
}

func flaggedChangeJSON(row reviewChangeRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("change_id", row.ChangeID)
	out.Set("team_id", row.TeamID)
	teamName := row.TeamName
	if teamName == "" {
		teamName = row.TeamID
	}
	out.Set("team_name", teamName)
	out.Set("change_type", row.ChangeType)
	if row.Field != nil {
		out.Set("field", *row.Field)
	} else {
		out.Set("field", nil)
	}
	out.Set("old_value", loadsJSONValue(row.OldValueJSON))
	out.Set("new_value", loadsJSONValue(row.NewValueJSON))
	out.Set("discovered_at", naiveISOTime(row.FirstSeenAt))
	return out
}

// pendingChanges is GET /teams/pending-changes, dispatched from getTeam (the
// literal collides with the {team_id} wildcard exactly as discover does).
//
// The endpoint lists every pending change in the org: like the Python route,
// it takes no filter, so a team_id query parameter is ignored.
func (h handlers) pendingChanges(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.pendingReviewRows(r.Context(), orgIDOf(r.Context()), nil)
	if err != nil {
		h.internal(w, r, "list pending team changes", err)
		return
	}
	changes := make([]pyjson.Value, len(rows))
	for i, row := range rows {
		changes[i] = flaggedChangeJSON(row)
	}
	out := pyjson.NewObject()
	out.Set("changes", changes)
	out.Set("total", int64(len(rows)))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h handlers) approveChanges(w http.ResponseWriter, r *http.Request) { h.decideChanges(w, r, true) }
func (h handlers) dismissChanges(w http.ResponseWriter, r *http.Request) {
	h.decideChanges(w, r, false)
}

// decideChanges is approve_team_changes / dismiss_team_changes: the body is
// {change_ids: list[str] = [], approve_all|dismiss_all: bool = False}; the
// decision's decided_by is the caller's org id (get_admin_org_id), as in
// Python.
func (h handlers) decideChanges(w http.ResponseWriter, r *http.Request, approve bool) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	changeIDs, _ := problems.DefaultedStringList(object, "change_ids")
	allKey := "dismiss_all"
	if approve {
		allKey = "approve_all"
	}
	decideAll, _ := problems.DefaultedBool(object, allKey)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}

	ctx := r.Context()
	orgID := orgIDOf(ctx)
	teamID := pathParam(r, "team_id")
	rows, err := h.store.selectChangesForDecision(ctx, orgID, teamID, changeIDs, decideAll)
	if err != nil {
		h.internal(w, r, "select team changes for decision", err)
		return
	}
	status, countKey := statusDismissed, "dismissed"
	if approve {
		status, countKey = statusApproved, "approved"
		for _, row := range rows {
			if err := h.store.applyChange(ctx, orgID, row); err != nil {
				h.internal(w, r, "apply team change", err)
				return
			}
		}
	}
	if err := h.store.insertDecisionRows(ctx, orgID, rows, status, orgID); err != nil {
		h.internal(w, r, "record team change decision", err)
		return
	}
	ids := make([]pyjson.Value, len(rows))
	for i, row := range rows {
		ids[i] = row.ChangeID
	}
	out := pyjson.NewObject()
	out.Set(countKey, int64(len(rows)))
	out.Set("change_ids", ids)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// insertDecisionRows is _insert_status_rows: one status row per decided
// change, first/last_seen_at carried over unchanged, decided_at/updated_at
// now, decided_by the caller.
func (s Store) insertDecisionRows(ctx context.Context, orgID string, rows []reviewChangeRow, status, decidedBy string) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now().UTC()
	out := make([]teamDriftChangeRow, len(rows))
	for i, row := range rows {
		oldJSON, newJSON := row.OldValueJSON, row.NewValueJSON
		if oldJSON == "" {
			oldJSON = "null"
		}
		if newJSON == "" {
			newJSON = "null"
		}
		entityType := row.EntityType
		if entityType == "" {
			entityType = "team"
		}
		decided := decidedBy
		out[i] = teamDriftChangeRow{
			OrgID: orgID, ChangeID: row.ChangeID, EntityType: entityType, EntityID: row.TeamID,
			Provider: row.Provider, NativeTeamKey: row.NativeTeamKey, ChangeType: row.ChangeType, Field: row.Field,
			OldValueJSON: oldJSON, NewValueJSON: newJSON, Status: status,
			FirstSeenAt: row.FirstSeenAt, LastSeenAt: row.LastSeenAt, DecidedAt: &now, DecidedBy: &decided, UpdatedAt: now,
		}
	}
	return s.insertChanges(ctx, out)
}

// naiveISOTime renders a ClickHouse DateTime64 read (timezone-naive in the
// Python client) the way datetime.isoformat() does: no offset, and the six
// fractional digits only when the microsecond part is non-zero.
func naiveISOTime(value time.Time) string {
	value = value.UTC()
	if value.Nanosecond()/1000 == 0 {
		return value.Format("2006-01-02T15:04:05")
	}
	return value.Format("2006-01-02T15:04:05.000000")
}
