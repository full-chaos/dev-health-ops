package teamsidentity

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// applyChange is ClickHouseTeamDriftService._apply_change: an identity change
// applies a membership, a team change writes the observed value of its field
// through the team upsert. Any refusal is Python's ValueError -> a 500.
func (s Store) applyChange(ctx context.Context, orgID string, row reviewChangeRow) error {
	if row.EntityType == "identity" {
		return s.applyIdentityMembershipChange(ctx, orgID, row, time.Now().UTC())
	}
	if row.Field == nil || *row.Field == "" {
		return nil
	}
	field := *row.Field
	observation, err := s.observationFor(ctx, orgID, row)
	if err != nil {
		return err
	}
	if len(observation) == 0 {
		return fmt.Errorf("cannot approve drift change without a provider observation")
	}
	column := field
	switch field {
	case "members":
		column = "members_json"
	case "project_keys":
		column = "project_keys_json"
	case "repo_patterns":
		column = "repo_patterns_json"
	}
	rawObserved, ok := observation[column]
	if !ok {
		return fmt.Errorf("cannot approve drift change without observed field %s", field)
	}
	observed := rawObserved
	if strings.HasSuffix(column, "_json") {
		text, _ := rawObserved.(string)
		observed = loadsJSONValue(text)
	}

	existing, err := s.GetTeam(ctx, orgID, row.TeamID)
	if err != nil {
		return err
	}
	name := row.TeamName
	if name == "" {
		name = row.TeamID
	}
	var description *string
	members, projectKeys, repoPatterns := []string{}, []string{}, []string{}
	if existing != nil {
		name, description = existing.Name, existing.Description
		members, projectKeys, repoPatterns = existing.Members, existing.ProjectKeys, existing.RepoPatterns
	}
	switch field {
	case "name":
		if observed != nil {
			name = pythonStr(observed)
		}
	case "description":
		if observed == nil {
			description = nil
		} else {
			text := pythonStr(observed)
			description = &text
		}
	case "members":
		members = jsonListValue(observed)
	case "project_keys":
		projectKeys = jsonListValue(observed)
	case "repo_patterns":
		repoPatterns = jsonListValue(observed)
	default:
		return nil
	}
	_, err = s.CreateOrUpdateTeam(ctx, orgID, TeamWrite{
		TeamID: row.TeamID, Name: name, Description: description,
		Members: &members, ProjectKeys: &projectKeys, RepoPatterns: &repoPatterns,
	})
	return err
}

// jsonListValue is _json_list over an already-decoded value: a string is
// decoded again, a list becomes str() of each non-null element, anything
// else is empty.
func jsonListValue(value pyjson.Value) []string {
	if text, ok := value.(string); ok {
		value = loadsJSONValue(text)
	}
	list, ok := value.([]pyjson.Value)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if item != nil {
			out = append(out, pythonStr(item))
		}
	}
	return out
}

// observationFor is _observation_for: the newest provider observation of the
// changed team, matched on provider + native_team_key when the change has
// one, else provider + team_id. Values are keyed by column name.
func (s Store) observationFor(ctx context.Context, orgID string, row reviewChangeRow) (map[string]pyjson.Value, error) {
	conditions := `org_id = {org_id:String} AND provider = {provider:String}`
	args := []any{clickhouse.Named("org_id", orgID), clickhouse.Named("provider", row.Provider)}
	if row.NativeTeamKey != nil && *row.NativeTeamKey != "" {
		conditions += ` AND native_team_key = {native_team_key:String}`
		args = append(args, clickhouse.Named("native_team_key", *row.NativeTeamKey))
	} else {
		conditions += ` AND team_id = {team_id:String}`
		args = append(args, clickhouse.Named("team_id", row.TeamID))
	}
	rows, err := s.Conn.Query(ctx, `
		SELECT team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id
		FROM team_provider_observations FINAL WHERE `+conditions+` ORDER BY updated_at DESC LIMIT 1`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return map[string]pyjson.Value{}, rows.Err()
	}
	var (
		teamID, membersJSON, projectKeysJSON, repoPatternsJSON string
		name, description, parentTeamID                        *string
		isActive                                               uint8
	)
	if err := rows.Scan(&teamID, &name, &description, &membersJSON, &projectKeysJSON, &repoPatternsJSON, &isActive, &parentTeamID); err != nil {
		return nil, err
	}
	optional := func(value *string) pyjson.Value {
		if value == nil {
			return nil
		}
		return *value
	}
	return map[string]pyjson.Value{
		"team_id": teamID, "name": optional(name), "description": optional(description),
		"members_json": membersJSON, "project_keys_json": projectKeysJSON, "repo_patterns_json": repoPatternsJSON,
		"is_active": int64(isActive), "parent_team_id": optional(parentTeamID),
	}, nil
}

// --- identity membership changes (clickhouse_identity_drift.py) --------------

// applyIdentityMembershipChange is apply_identity_membership_change: insert
// the change's membership, expire the manual membership / member fallback it
// conflicted with, then add the member's facets to the team.
func (s Store) applyIdentityMembershipChange(ctx context.Context, orgID string, row reviewChangeRow, now time.Time) error {
	newValue, ok := loadsJSONValue(row.NewValueJSON).(*pyjson.Object)
	if !ok {
		return fmt.Errorf("cannot approve identity drift change without membership payload")
	}
	membership := cloneObject(newValue)
	membership.Set("org_id", orgID)
	membership.Set("updated_at", now)
	if err := s.insertTeamMembership(ctx, orgID, membership); err != nil {
		return err
	}
	if oldValue, isObject := loadsJSONValue(row.OldValueJSON).(*pyjson.Object); isObject {
		if err := s.expireConflict(ctx, orgID, oldValue, now); err != nil {
			return err
		}
	}
	teamID := firstTruthyString(objectValue(newValue, "team_id"), row.TeamID)
	facets := membershipFacets(newValue)
	if len(facets) > 0 {
		if _, err := s.AddMembers(ctx, orgID, teamID, sortedKeysOfSet(facets)); err != nil {
			return err
		}
	}
	return nil
}

func objectValue(object *pyjson.Object, key string) pyjson.Value {
	value, _ := object.Get(key)
	return value
}

func cloneObject(source *pyjson.Object) *pyjson.Object {
	out := pyjson.NewObject()
	for _, key := range source.Keys() {
		value, _ := source.Get(key)
		out.Set(key, value)
	}
	return out
}

// truthy is Python truthiness for a decoded JSON value.
func truthy(value pyjson.Value) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case pyjson.Int:
		return v.Sign() != 0
	case pyjson.Float:
		return float64(v) != 0
	case []pyjson.Value:
		return len(v) > 0
	case *pyjson.Object:
		return v.Len() > 0
	}
	return true
}

// firstTruthyString is `str(a or b)`.
func firstTruthyString(first pyjson.Value, fallback string) string {
	if truthy(first) {
		return pythonStr(first)
	}
	return fallback
}

// strOrEmpty is `str(value or "")`.
func strOrEmpty(value pyjson.Value) string {
	if !truthy(value) {
		return ""
	}
	return pythonStr(value)
}

// membershipFacets is _membership_facets: the truthy member_id,
// raw_provider_user_id and raw_email values as strings.
func membershipFacets(row *pyjson.Object) map[string]bool {
	out := map[string]bool{}
	for _, key := range []string{"member_id", "raw_provider_user_id", "raw_email"} {
		if value := objectValue(row, key); truthy(value) {
			out[pythonStr(value)] = true
		}
	}
	return out
}

func sortedKeysOfSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// expireConflict is _expire_conflict: write the conflicting manual
// membership (or member fallback) back with valid_to = now.
func (s Store) expireConflict(ctx context.Context, orgID string, conflict *pyjson.Object, now time.Time) error {
	switch pythonStr(objectValue(conflict, "field")) {
	case "team_memberships":
		manual, ok := objectValue(conflict, "manual_membership").(*pyjson.Object)
		if !ok || manual.Len() == 0 {
			return nil
		}
		row := cloneObject(manual)
		row.Set("org_id", orgID)
		row.Set("valid_to", now)
		row.Set("updated_at", now)
		return s.insertTeamMembership(ctx, orgID, row)
	case "manual_attribution_fallbacks.member":
		fallback, ok := objectValue(conflict, "manual_fallback").(*pyjson.Object)
		if !ok || fallback.Len() == 0 {
			return nil
		}
		row := cloneObject(fallback)
		row.Set("org_id", orgID)
		row.Set("valid_to", now)
		row.Set("updated_at", now)
		return s.insertManualFallback(ctx, orgID, row, now)
	}
	return nil
}

// pythonIntOrZero is `int(value or 0)`.
func pythonIntOrZero(value pyjson.Value) (int64, error) {
	if !truthy(value) {
		return 0, nil
	}
	switch v := value.(type) {
	case bool:
		return 1, nil
	case pyjson.Int:
		if !v.IsInt64() {
			return 0, fmt.Errorf("integer out of range")
		}
		return v.Int64(), nil
	case pyjson.Float:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return 0, fmt.Errorf("cannot convert %v to int", float64(v))
		}
		return int64(float64(v)), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid int %q", v)
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("cannot convert %T to int", value)
}

// datetimeColumn turns a payload value into what the DateTime64 column
// takes: a time.Time as is, a Python str(datetime) / ISO string parsed
// (canonical JSON renders datetimes with default=str), nil as NULL.
func datetimeColumn(value pyjson.Value) (*time.Time, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case time.Time:
		utc := v.UTC()
		return &utc, nil
	case string:
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02T15:04:05.999999999", "2006-01-02",
		} {
			if parsed, err := time.Parse(layout, v); err == nil {
				utc := parsed.UTC()
				return &utc, nil
			}
		}
		return nil, fmt.Errorf("unparseable datetime %q", v)
	}
	return nil, fmt.Errorf("cannot store %T as a datetime", value)
}

const teamMembershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

// insertTeamMembership is ClickHouseStore.insert_team_memberships for one
// row (_insert_team_edge_rows' per-column normalization).
func (s Store) insertTeamMembership(ctx context.Context, orgID string, row *pyjson.Object) error {
	optionalText := func(key string) any {
		value := objectValue(row, key)
		if value == nil {
			return nil
		}
		text := pythonStr(value)
		return &text
	}
	facets, err := arrayStringValue("identity_facets", listOrEmpty(objectValue(row, "identity_facets")))
	if err != nil {
		return err
	}
	isPrimary, err := pythonIntOrZero(objectValue(row, "is_primary"))
	if err != nil {
		return err
	}
	specificity, err := pythonIntOrZero(objectValue(row, "specificity"))
	if err != nil {
		return err
	}
	priority, err := pythonIntOrZero(objectValue(row, "priority"))
	if err != nil {
		return err
	}
	validFrom, err := datetimeColumn(objectValue(row, "valid_from"))
	if err != nil {
		return err
	}
	if validFrom == nil {
		return fmt.Errorf("valid_from is required")
	}
	validTo, err := datetimeColumn(objectValue(row, "valid_to"))
	if err != nil {
		return err
	}
	updatedAt, err := datetimeColumn(objectValue(row, "updated_at"))
	if err != nil {
		return err
	}
	if updatedAt == nil {
		return fmt.Errorf("updated_at is required")
	}
	rowOrg := strOrEmpty(objectValue(row, "org_id"))
	if rowOrg == "" {
		rowOrg = orgID
	}
	batch, err := s.Conn.PrepareBatch(ctx, teamMembershipsInsert)
	if err != nil {
		return err
	}
	if err := batch.Append(rowOrg, strOrEmpty(objectValue(row, "provider")), strOrEmpty(objectValue(row, "team_id")),
		strOrEmpty(objectValue(row, "member_id")), optionalText("raw_provider_user_id"), optionalText("raw_email"), facets,
		strOrEmpty(objectValue(row, "source")), uint8(isPrimary), uint16(specificity), int32(priority),
		*validFrom, validTo, *updatedAt); err != nil {
		return err
	}
	return batch.Send()
}

// listOrEmpty is `list(value or [])`.
func listOrEmpty(value pyjson.Value) pyjson.Value {
	if !truthy(value) {
		return []pyjson.Value{}
	}
	return value
}

const manualFallbacksInsert = `INSERT INTO manual_attribution_fallbacks (org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, created_by, created_at, updated_at)`

// insertManualFallback is ClickHouseStore.insert_manual_attribution_fallbacks
// for one row: a missing/None priority defaults to 100, non-nullable
// datetimes fall back to now.
func (s Store) insertManualFallback(ctx context.Context, orgID string, row *pyjson.Object, now time.Time) error {
	priority := int64(100)
	if raw := objectValue(row, "priority"); raw != nil {
		var err error
		priority, err = pythonIntOrZero(raw)
		if err != nil {
			return err
		}
	}
	orNow := func(key string) (time.Time, error) {
		value, err := datetimeColumn(objectValue(row, key))
		if err != nil {
			return time.Time{}, err
		}
		if value == nil {
			return now, nil
		}
		return *value, nil
	}
	validFrom, err := orNow("valid_from")
	if err != nil {
		return err
	}
	validTo, err := datetimeColumn(objectValue(row, "valid_to"))
	if err != nil {
		return err
	}
	createdAt, err := orNow("created_at")
	if err != nil {
		return err
	}
	updatedAt, err := orNow("updated_at")
	if err != nil {
		return err
	}
	var createdBy any
	if value := objectValue(row, "created_by"); value != nil {
		text := pythonStr(value)
		createdBy = &text
	}
	rowOrg := strOrEmpty(objectValue(row, "org_id"))
	if rowOrg == "" {
		rowOrg = orgID
	}
	batch, err := s.Conn.PrepareBatch(ctx, manualFallbacksInsert)
	if err != nil {
		return err
	}
	if err := batch.Append(rowOrg, strOrEmpty(objectValue(row, "provider")), strOrEmpty(objectValue(row, "scope_type")),
		strOrEmpty(objectValue(row, "scope_id")), strOrEmpty(objectValue(row, "team_id")), strOrEmpty(objectValue(row, "team_name")),
		strOrEmpty(objectValue(row, "reason")), int32(priority), validFrom, validTo, createdBy, createdAt, updatedAt); err != nil {
		return err
	}
	return batch.Send()
}
