package teamsidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Team-drift policy constants, transcribed from
// clickhouse_team_drift_projector.py:14-16.
const (
	autoApplyPolicy     = 0
	flagForReviewPolicy = 1
	manualPolicy        = 2
)

const (
	statusPending    = "pending"
	statusApproved   = "approved"
	statusDismissed  = "dismissed"
	statusResolved   = "resolved"
	statusSuperseded = "superseded"
	entityTypeTeam   = "team"
	changeTypeField  = "field_changed"
)

// defaultManagedFields mirrors DEFAULT_MANAGED_FIELDS
// (clickhouse_team_drift_projector.py:27-33).
var defaultManagedFields = []string{"name", "description", "members", "project_keys", "repo_patterns"}

// jsonDiffFields mirrors JSON_FIELDS (clickhouse_team_drift_projector.py:
// 34): these three fields compare as a SORTED, DEDUPED list, not the raw
// field value.
var jsonDiffFields = map[string]bool{"members": true, "project_keys": true, "repo_patterns": true}

// teamProviderObservationRow mirrors team_provider_observations' columns
// (057_team_provider_observations.sql) after _observed_row's own shaping
// (clickhouse_team_drift_projector.py:378-397): the three list fields are
// JSON-TEXT columns (members_json/project_keys_json/repo_patterns_json),
// encoded the same bare `json.dumps(list, default=str)` shape
// pythonProviderIdentitiesJSON already uses via pyjson.Dumps (storage/
// clickhouse.py's own `_json_column`, storage/clickhouse.py:189-194).
type teamProviderObservationRow struct {
	OrgID            string
	Provider         string
	NativeTeamKey    string
	TeamID           string
	Name             *string
	Description      *string
	MembersJSON      string
	ProjectKeysJSON  string
	RepoPatternsJSON string
	IsActive         uint8
	ParentTeamID     *string
	DiscoveredAt     time.Time
	UpdatedAt        time.Time
}

// teamDriftChangeRow mirrors team_drift_changes' columns
// (058_team_drift_changes.sql) after _status_rows'/_project_field_changes'
// own shaping.
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

// observedTeamRow is what project_team/record_observation build from a
// discovered team before ANY policy branch (clickhouse_team_drift_
// projector.py:_observed_row, 378-397) -- the shape both the observation
// insert and the field-diff comparison read from.
type observedTeamRow struct {
	OrgID         string
	Provider      string
	NativeTeamKey string
	TeamID        string
	Name          *string
	Description   *string
	Members       []string
	ProjectKeys   []string
	RepoPatterns  []string
	IsActive      bool
	ParentTeamID  *string
	DiscoveredAt  time.Time
	UpdatedAt     time.Time
}

// jsonListColumn renders a []string list the SAME bare json.dumps(list,
// default=str) shape storage/clickhouse.py's own _json_column gives a
// list value -- ensure_ascii=True, allow_nan=True, spaced separators,
// unsorted (insertion order, here always the slice's own order). Reuses
// pyjson.Dumps, the same shared encoder pythonProviderIdentitiesJSON
// already proved against a live Python oracle (CHAOS-6310).
func jsonListColumn(values []string) (string, error) {
	if values == nil {
		values = []string{}
	}
	return pyjson.Dumps(values)
}

func observedRowFromDiscovered(orgID string, team discoveredTeam, discoveredAt, updatedAt time.Time) observedTeamRow {
	repoPatterns, _ := stringListAssociation(team.Associations, "repo_patterns")
	projectKeys, _ := stringListAssociation(team.Associations, "project_keys")
	name := team.Name
	return observedTeamRow{
		OrgID:         orgID,
		Provider:      team.ProviderType,
		NativeTeamKey: team.ProviderTeamID,
		TeamID:        importedTeamID(team.ProviderType, team.ProviderTeamID),
		Name:          &name,
		Description:   team.Description,
		Members:       []string{},
		ProjectKeys:   projectKeys,
		RepoPatterns:  repoPatterns,
		IsActive:      true,
		ParentTeamID:  nil,
		DiscoveredAt:  discoveredAt,
		UpdatedAt:     updatedAt,
	}
}

func stringListAssociation(associations *pyjson.Object, key string) ([]string, bool) {
	if associations == nil {
		return nil, false
	}
	value, ok := associations.Get(key)
	if !ok {
		return nil, false
	}
	return pythonListField(value), true
}

// pythonListField ports _list_field
// (clickhouse_team_drift_projector.py:497-504): None -> [], a string -> [it],
// a list/tuple/set -> [str(item) for item in it if item is not None], anything
// else -> []. Python's `str()` of each element is what reaches ClickHouse, so
// a numeric key 7 is stored as "7" -- CHAOS-6311's review executed exactly
// that input against the Go route and found it stored ["ENG"] where Python
// stores ["7", "ENG"]. A JSON-decoded body carries arrays as
// []pyjson.Value; hand-built values (tests, discovery output) are []string.
func pythonListField(value pyjson.Value) []string {
	switch list := value.(type) {
	case nil:
		return []string{}
	case string:
		return []string{list}
	case []string:
		return append([]string{}, list...)
	case []pyjson.Value:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if item != nil {
				out = append(out, pythonStr(item))
			}
		}
		return out
	}
	return []string{}
}

// pythonStr is Python's str() of a JSON-decoded value: a string is itself,
// ints are decimal, floats use repr, bools are True/False, None is "None",
// and a list or dict renders as its Python repr (strings inside a container
// use repr's quoting).
func pythonStr(value pyjson.Value) string {
	switch v := value.(type) {
	case nil:
		return "None"
	case string:
		return v
	case bool:
		if v {
			return "True"
		}
		return "False"
	case pyjson.Int:
		return v.String()
	case pyjson.Float:
		return pythonparity.Repr(float64(v))
	case []pyjson.Value:
		parts := make([]string, len(v))
		for i, item := range v {
			parts[i] = pythonRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *pyjson.Object:
		parts := make([]string, 0, v.Len())
		for _, key := range v.Keys() {
			item, _ := v.Get(key)
			parts = append(parts, pythonparity.StrRepr(key)+": "+pythonRepr(item))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(value)
}

// pythonRepr is repr() of a JSON-decoded value (only strings differ from str).
func pythonRepr(value pyjson.Value) string {
	if text, ok := value.(string); ok {
		return pythonparity.StrRepr(text)
	}
	return pythonStr(value)
}

// importedTeamID mirrors import_teams' own team_id derivation
// (clickhouse_team_admin.py:397-407): "gh:"/"gl:"/"ms-teams:" prefixes
// for those three providers, the bare provider_team_id for every other
// provider (jira, linear -- their provider_team_id IS already the team_id
// shape those routes use elsewhere).
func importedTeamID(providerType, providerTeamID string) string {
	switch providerType {
	case "github":
		return "gh:" + providerTeamID
	case "gitlab":
		return "gl:" + providerTeamID
	case "ms-teams":
		return "ms-teams:" + providerTeamID
	default:
		return providerTeamID
	}
}

// changeIDForTeamField mirrors change_id_for_team_field
// (clickhouse_team_drift_projector.py:76-89): sha256 of the canonical
// (sorted-key, compact-separator) JSON encoding of the change's identity
// fields. Built by inserting pyjson.Object keys in ALREADY-SORTED order
// and calling pyjson.Marshal (compact, unsorted-preserving separators --
// sorting happens at INSERTION time here, not inside the encoder), which
// gives the exact same {",":":"}-separated, sorted-key bytes Python's
// `json.dumps(payload, sort_keys=True, separators=(",", ":"))` does.
func changeIDForTeamField(orgID, teamID, field, oldValueJSON, newValueJSON, changeType string) (string, error) {
	// Field order matches Python's dict literal alphabetically sorted by
	// sort_keys=True: change_type, entity_id, entity_type, field, new_value_json,
	// old_value_json, org_id.
	payload := pyjson.NewObject()
	payload.Set("change_type", changeType)
	payload.Set("entity_id", teamID)
	payload.Set("entity_type", entityTypeTeam)
	payload.Set("field", field)
	payload.Set("new_value_json", newValueJSON)
	payload.Set("old_value_json", oldValueJSON)
	payload.Set("org_id", orgID)
	encoded, err := pyjson.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalFieldJSON mirrors _field_json/_canonical_json
// (clickhouse_team_drift_projector.py:325-333): a JSON_FIELDS field
// compares as `_comparison_list_field` (sorted+deduped) first; every
// field then goes through `json.dumps(value, sort_keys=True,
// separators=(",", ":"), default=str)` -- a scalar's "sort_keys" is a
// no-op (nothing to sort), so this is just pyjson.Marshal on the bare
// value either way.
func canonicalFieldJSON(field string, row *observedTeamRow) (string, error) {
	if row == nil {
		return marshalString(nil)
	}
	switch field {
	case "members":
		return marshalString(sortedDedupedStrings(row.Members))
	case "project_keys":
		return marshalString(sortedDedupedStrings(row.ProjectKeys))
	case "repo_patterns":
		return marshalString(sortedDedupedStrings(row.RepoPatterns))
	case "name":
		return marshalString(stringPtrValue(row.Name))
	case "description":
		return marshalString(stringPtrValue(row.Description))
	default:
		return marshalString(nil)
	}
}

// marshalString is pyjson.Marshal, converted to string -- every canonical-
// JSON comparison in this file compares strings (matching Python's own
// json.dumps(...) -> str), never raw bytes.
func marshalString(value pyjson.Value) (string, error) {
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func stringPtrValue(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

func sortedDedupedStrings(values []string) []string {
	set := map[string]bool{}
	for _, value := range values {
		set[value] = true
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// --- ClickHouse reads/writes -------------------------------------------

func (s Store) syncPolicy(ctx context.Context, orgID, teamID string) (int, []string, error) {
	rows, err := s.Conn.Query(ctx,
		`SELECT sync_policy, managed_fields FROM team_sync_policies FINAL WHERE org_id = {org_id:String} AND team_id = {team_id:String} LIMIT 1`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_id", teamID))
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return autoApplyPolicy, defaultManagedFields, rows.Err()
	}
	var (
		policy  uint8
		managed []string
	)
	if err := rows.Scan(&policy, &managed); err != nil {
		return 0, nil, err
	}
	filtered := filterManagedFields(managed)
	if len(filtered) == 0 {
		filtered = defaultManagedFields
	}
	return int(policy), filtered, nil
}

func filterManagedFields(fields []string) []string {
	allowed := map[string]bool{}
	for _, field := range defaultManagedFields {
		allowed[field] = true
	}
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if allowed[field] {
			out = append(out, field)
		}
	}
	return out
}

func (s Store) pendingChangeRows(ctx context.Context, orgID, teamID string) ([]teamDriftChangeRow, error) {
	rows, err := s.Conn.Query(ctx, `
		SELECT org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field,
		       old_value_json, new_value_json, status, first_seen_at, last_seen_at, decided_at, decided_by, updated_at
		FROM team_drift_changes FINAL
		WHERE org_id = {org_id:String} AND entity_type = 'team' AND entity_id = {team_id:String} AND change_type = 'field_changed'`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_id", teamID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []teamDriftChangeRow
	for rows.Next() {
		var row teamDriftChangeRow
		if err := rows.Scan(&row.OrgID, &row.ChangeID, &row.EntityType, &row.EntityID, &row.Provider, &row.NativeTeamKey,
			&row.ChangeType, &row.Field, &row.OldValueJSON, &row.NewValueJSON, &row.Status,
			&row.FirstSeenAt, &row.LastSeenAt, &row.DecidedAt, &row.DecidedBy, &row.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s Store) insertObservation(ctx context.Context, row teamProviderObservationRow) error {
	batch, err := s.Conn.PrepareBatch(ctx,
		"INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at)")
	if err != nil {
		return err
	}
	if err := batch.Append(row.OrgID, row.Provider, row.NativeTeamKey, row.TeamID, row.Name, row.Description,
		row.MembersJSON, row.ProjectKeysJSON, row.RepoPatternsJSON, row.IsActive, row.ParentTeamID, row.DiscoveredAt, row.UpdatedAt); err != nil {
		return err
	}
	return batch.Send()
}

// projectTeamResult mirrors import_teams' own per-team accounting
// (clickhouse_team_admin.py:397-472): whether the team was imported
// (new), merged (existing, updated or observed), or skipped (existing,
// on_conflict=skip).
type projectTeamResult struct {
	TeamID         string
	ProviderTeamID string
	Action         string // "imported" | "merged" | "skipped"
}

// projectTeam mirrors ClickHouseTeamDriftProjector.project_team
// (clickhouse_team_drift_projector.py:189-233) as import_teams
// (clickhouse_team_admin.py:380-472) calls it: always inserts an
// observation; then, by policy: AUTO_APPLY writes the teams catalog row
// (manual_members carried forward from any existing row, CHAOS-4321) and
// resolves any pending drift changes; FLAG_FOR_REVIEW diffs each managed
// field against the existing row and writes drift-change rows; MANUAL
// does neither (observation only). skipOnConflict mirrors import_teams'
// own on_conflict="skip" short-circuit: an ALREADY-EXISTING team records
// only an observation, regardless of policy, and never reaches the
// catalog/diff branches below (clickhouse_team_admin.py:427-438).
func (s Store) projectTeam(ctx context.Context, orgID string, team discoveredTeam, onConflict string) (projectTeamResult, error) {
	now := time.Now().UTC()
	observed := observedRowFromDiscovered(orgID, team, now, now)
	existing, err := s.GetTeam(ctx, orgID, observed.TeamID)
	if err != nil {
		return projectTeamResult{}, err
	}

	membersJSON, err := jsonListColumn(observed.Members)
	if err != nil {
		return projectTeamResult{}, err
	}
	projectKeysJSON, err := jsonListColumn(observed.ProjectKeys)
	if err != nil {
		return projectTeamResult{}, err
	}
	repoPatternsJSON, err := jsonListColumn(observed.RepoPatterns)
	if err != nil {
		return projectTeamResult{}, err
	}
	isActive := uint8(0)
	if observed.IsActive {
		isActive = 1
	}
	if err := s.insertObservation(ctx, teamProviderObservationRow{
		OrgID: observed.OrgID, Provider: observed.Provider, NativeTeamKey: observed.NativeTeamKey,
		TeamID: observed.TeamID, Name: observed.Name, Description: observed.Description,
		MembersJSON: membersJSON, ProjectKeysJSON: projectKeysJSON, RepoPatternsJSON: repoPatternsJSON,
		IsActive: isActive, ParentTeamID: observed.ParentTeamID, DiscoveredAt: observed.DiscoveredAt, UpdatedAt: observed.UpdatedAt,
	}); err != nil {
		return projectTeamResult{}, err
	}

	if existing != nil && onConflict == "skip" {
		return projectTeamResult{TeamID: observed.TeamID, ProviderTeamID: team.ProviderTeamID, Action: "skipped"}, nil
	}

	policy, managedFields, err := s.syncPolicy(ctx, orgID, observed.TeamID)
	if err != nil {
		return projectTeamResult{}, err
	}
	pending, err := s.pendingChangeRows(ctx, orgID, observed.TeamID)
	if err != nil {
		return projectTeamResult{}, err
	}

	if policy == autoApplyPolicy {
		manualMembers := []string{}
		members := []string{}
		teamUUIDValue := teamUUID(orgID, observed.TeamID)
		if existing != nil {
			manualMembers = existing.ManualMembers
			members = existing.Members
			teamUUIDValue = existing.TeamUUID
		}
		// import_teams' own catalog_row (clickhouse_team_admin.py:413-425)
		// deliberately writes provider="" and native_team_key=None on the
		// TEAMS TABLE row -- NOT the discovered team's real provider/
		// native_team_key (those ARE carried on the observation row
		// above). An admin-imported team must never be silently reclaimed
		// or overwritten by a later real provider sync matching on
		// provider+native_team_key; only the observation history remembers
		// where it came from.
		// The catalog row takes the RAW association values (Python passes
		// `associations.get("project_keys", [])` straight into the insert,
		// only the observation goes through _list_field), so what
		// ClickHouse accepts decides -- see catalogStringList.
		catalogProjectKeys, err := catalogStringList(team.Associations, "project_keys")
		if err != nil {
			return projectTeamResult{}, err
		}
		catalogRepoPatterns, err := catalogStringList(team.Associations, "repo_patterns")
		if err != nil {
			return projectTeamResult{}, err
		}
		if err := s.insertTeamRow(ctx, teamInsertRow{
			ID: observed.TeamID, TeamUUID: teamUUIDValue, Name: stringPtrOr(observed.Name, observed.TeamID),
			Description: observed.Description, Members: members, ManualMembers: manualMembers,
			ProjectKeys: catalogProjectKeys, RepoPatterns: catalogRepoPatterns, IsActive: true,
			OrgID: orgID, Provider: "", NativeTeamKey: nil, ParentTeamID: observed.ParentTeamID,
			UpdatedAt: now,
		}); err != nil {
			return projectTeamResult{}, err
		}
		if err := s.markPending(ctx, pending, statusResolved, now); err != nil {
			return projectTeamResult{}, err
		}
		action := "merged"
		if existing == nil {
			action = "imported"
		}
		return projectTeamResult{TeamID: observed.TeamID, ProviderTeamID: team.ProviderTeamID, Action: action}, nil
	}

	if policy != flagForReviewPolicy {
		// MANUAL: observation only, same accounting as AUTO_APPLY for the
		// caller's imported/merged bookkeeping (import_teams itself does
		// not special-case MANUAL differently -- it only ever branches on
		// existing==nil).
		action := "merged"
		if existing == nil {
			action = "imported"
		}
		return projectTeamResult{TeamID: observed.TeamID, ProviderTeamID: team.ProviderTeamID, Action: action}, nil
	}

	if err := s.projectFieldChanges(ctx, orgID, observed, existing, managedFields, pending, now); err != nil {
		return projectTeamResult{}, err
	}
	action := "merged"
	if existing == nil {
		action = "imported"
	}
	return projectTeamResult{TeamID: observed.TeamID, ProviderTeamID: team.ProviderTeamID, Action: action}, nil
}

func stringPtrOr(value *string, fallback string) string {
	if value == nil {
		return fallback
	}
	return *value
}

func (s Store) markPending(ctx context.Context, rows []teamDriftChangeRow, status string, now time.Time) error {
	var toWrite []teamDriftChangeRow
	for _, row := range rows {
		if row.Status != statusPending {
			continue
		}
		toWrite = append(toWrite, statusRow(row, status, now))
	}
	return s.insertChanges(ctx, toWrite)
}

func statusRow(row teamDriftChangeRow, status string, now time.Time) teamDriftChangeRow {
	out := row
	out.Status = status
	out.LastSeenAt = now
	out.UpdatedAt = now
	if status == statusApproved || status == statusDismissed {
		out.DecidedAt = &now
	} else {
		out.DecidedAt = nil
		out.DecidedBy = nil
	}
	return out
}

// projectFieldChanges mirrors _project_field_changes
// (clickhouse_team_drift_projector.py:279-323): for each managed field,
// compare the observed value's canonical JSON against the existing row's;
// unchanged fields resolve their own pending rows; a changed field's
// content-addressed change_id supersedes any OTHER pending row for the
// same field, and is itself skipped (not re-inserted) if already decided.
func (s Store) projectFieldChanges(ctx context.Context, orgID string, observed observedTeamRow, existing *Team, managedFields []string, pending []teamDriftChangeRow, now time.Time) error {
	pendingByField := map[string][]teamDriftChangeRow{}
	byChangeID := map[string]teamDriftChangeRow{}
	for _, row := range pending {
		byChangeID[row.ChangeID] = row
		if row.Status == statusPending && row.Field != nil {
			pendingByField[*row.Field] = append(pendingByField[*row.Field], row)
		}
	}

	var toWrite []teamDriftChangeRow
	for _, field := range managedFields {
		oldJSON, err := existingFieldJSON(field, existing)
		if err != nil {
			return err
		}
		newJSON, err := canonicalFieldJSON(field, &observed)
		if err != nil {
			return err
		}
		fieldPending := pendingByField[field]

		if oldJSON == newJSON {
			for _, row := range fieldPending {
				toWrite = append(toWrite, statusRow(row, statusResolved, now))
			}
			continue
		}

		changeID, err := changeIDForTeamField(orgID, observed.TeamID, field, oldJSON, newJSON, changeTypeField)
		if err != nil {
			return err
		}
		for _, row := range fieldPending {
			if row.ChangeID == changeID {
				continue
			}
			toWrite = append(toWrite, statusRow(row, statusSuperseded, now))
		}

		if existingChange, ok := byChangeID[changeID]; ok && (existingChange.Status == statusApproved || existingChange.Status == statusDismissed) {
			continue
		}

		firstSeenAt := now
		if existingChange, ok := byChangeID[changeID]; ok && existingChange.Status == statusPending {
			firstSeenAt = existingChange.FirstSeenAt
		}
		fieldCopy := field
		toWrite = append(toWrite, teamDriftChangeRow{
			OrgID: orgID, ChangeID: changeID, EntityType: entityTypeTeam, EntityID: observed.TeamID,
			Provider: observed.Provider, NativeTeamKey: &observed.NativeTeamKey, ChangeType: changeTypeField,
			Field: &fieldCopy, OldValueJSON: oldJSON, NewValueJSON: newJSON, Status: statusPending,
			FirstSeenAt: firstSeenAt, LastSeenAt: now, UpdatedAt: now,
		})
	}
	return s.insertChanges(ctx, toWrite)
}

// existingFieldJSON mirrors _field_json applied to the EXISTING teams row
// (nil when there is none, matching Python's `existing.get(field)` on a
// None row -- every field then canonical-JSON-encodes to "null").
func existingFieldJSON(field string, existing *Team) (string, error) {
	if existing == nil {
		return marshalString(nil)
	}
	switch field {
	case "members":
		return marshalString(sortedDedupedStrings(existing.Members))
	case "project_keys":
		return marshalString(sortedDedupedStrings(existing.ProjectKeys))
	case "repo_patterns":
		return marshalString(sortedDedupedStrings(existing.RepoPatterns))
	case "name":
		return marshalString(existing.Name)
	case "description":
		return marshalString(stringPtrValue(existing.Description))
	default:
		return marshalString(nil)
	}
}

func (s Store) insertChanges(ctx context.Context, rows []teamDriftChangeRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx,
		"INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, decided_at, decided_by, updated_at)")
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.ChangeID, row.EntityType, row.EntityID, row.Provider, row.NativeTeamKey,
			row.ChangeType, row.Field, row.OldValueJSON, row.NewValueJSON, row.Status,
			row.FirstSeenAt, row.LastSeenAt, row.DecidedAt, row.DecidedBy, row.UpdatedAt); err != nil {
			return err
		}
	}
	return batch.Send()
}

// catalogStringList is what an Array(String) catalog insert makes of an
// association value Python hands over untouched, established by running
// import requests through the real Python api and the Go api side by side
// (the protected-routes venue oracle): absent or an explicit null -> []; a list is inserted
// element by element and every element must be a string (an int, bool,
// float, null or nested list makes the insert fail, so the request 500s
// after the observation row was already written); a string is iterated
// into its characters and a dict into its keys (both accepted); any other
// scalar (number, bool) fails the insert. An error is a 500, as in Python.
func catalogStringList(associations *pyjson.Object, key string) ([]string, error) {
	if associations == nil {
		return []string{}, nil
	}
	value, ok := associations.Get(key)
	if !ok {
		return []string{}, nil
	}
	switch v := value.(type) {
	case nil:
		// An explicit null is accepted and stored as an empty array.
		return []string{}, nil
	case string:
		out := []string{}
		for _, r := range v {
			out = append(out, string(r))
		}
		return out, nil
	case *pyjson.Object:
		return v.Keys(), nil
	case []string:
		return append([]string{}, v...), nil
	case []pyjson.Value:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, isString := item.(string)
			if !isString {
				return nil, fmt.Errorf("%s: a catalog Array(String) insert cannot take a %T element", key, item)
			}
			out = append(out, text)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%s: a catalog Array(String) insert cannot take a %T value", key, value)
}
