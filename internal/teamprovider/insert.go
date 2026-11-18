package teamprovider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// Gone says what a version does with gone_at.
type Gone uint8

const (
	// GoneKept keeps the newest stored version's gone_at: the writer does not
	// list the team.
	GoneKept Gone = iota
	// GoneCleared writes NULL: the writer lists the team, so it is not gone.
	GoneCleared
	// GoneStated writes Write.GoneAt: the version moves a team, with its own
	// state, to a new id.
	GoneStated
)

// Roster says what a version does with manual_members, the admin roster.
type Roster uint8

const (
	// RosterStated writes the value the writer gives: an admin roster edit.
	RosterStated Roster = iota
	// RosterKept writes the roster of teams FINAL, read inside the insert
	// statement: a writer that does not edit the roster. The read narrows the
	// window in which a roster edit can be lost to one statement; it is not a
	// lock (two INSERT..SELECT statements are not isolated).
	RosterKept
	// RosterDelta writes the roster of teams FINAL, read inside the insert
	// statement, with Write.RosterAdd added and Write.RosterRemove taken out
	// (sorted, distinct, no empty member): an admin edit of some members.
	RosterDelta
	// RosterSource writes the roster of the team Write.RosterSource when a
	// version of it newer than Write.RosterSourceRead (by (updated_at,
	// last_synced)) is stored, read inside the insert statement ([] when that
	// version is an admin delete); else the value the writer gives. A team
	// moved to a new id writes the roster it read from its old id, and an
	// edit of the old id committed after that read is the roster it takes.
	RosterSource
)

// Kept is a set of the columns an admin write does not edit: a version keeps
// them from the team's teams FINAL row, read inside the insert statement.
type Kept uint32

const (
	KeptName Kept = 1 << iota
	KeptDescription
	KeptProjectKeys
	KeptRepoPatterns
)

// keptColumns are the columns of Kept, in bit order.
var keptColumns = []struct {
	bit    Kept
	column string
}{{KeptName, "name"}, {KeptDescription, "description"}, {KeptProjectKeys, "project_keys"}, {KeptRepoPatterns, "repo_patterns"}}

const keptAll = KeptName | KeptDescription | KeptProjectKeys | KeptRepoPatterns

// Version is a stored version of a team: its (updated_at, last_synced).
type Version struct {
	UpdatedAt, LastSynced time.Time
}

// Write is one version of a team for Insert. Values are the writer's own
// columns, in Insert's column order. A non-empty ProviderTeamID or
// ProviderName is the run's own value; an empty one keeps the newest
// non-empty stored value. With Roster RosterKept or RosterDelta the
// manual_members value is not written; with RosterSource it is written only
// when the source team has no newer version.
type Write struct {
	Values         []any
	ProviderTeamID string
	ProviderName   string
	Gone           Gone
	GoneAt         *time.Time
	Roster         Roster
	RosterAdd      []string
	RosterRemove   []string
	// RosterSource is the team a RosterSource row takes its roster from, and
	// RosterSourceRead the newest version of it the writer read.
	RosterSource     string
	RosterSourceRead Version
	// Kept are the columns this version takes from the team's teams FINAL
	// row when the team has one; the writer's value otherwise.
	Kept Kept
	// VersionAfterStored moves updated_at to just after the newest stored
	// version of the team (every version, deleted too) when it is not after
	// it already, read inside the insert statement.
	VersionAfterStored bool
	// SkipDeleted writes nothing when the team has no teams FINAL row or its
	// newest version is an admin delete, read inside the insert statement: an
	// admin edit of a team deleted meanwhile (the delete wins).
	SkipDeleted bool
}

// Execer is the one connection method Insert needs.
type Execer interface {
	Exec(ctx context.Context, query string, args ...any) error
}

// ErrInvalidWrite is a column Insert does not know, a provider identity
// column in the writer's list, no id column, a row of the wrong width or a
// row of another organization.
var ErrInvalidWrite = errors.New("teamprovider: invalid teams write")

// columnTypes is the teams schema (ClickHouse migrations through 137) for
// every column a writer states; a time column is read as UTC text.
var columnTypes = map[string]string{
	"id":              "String",
	"team_uuid":       "UUID",
	"name":            "String",
	"description":     "Nullable(String)",
	"manual_members":  "Array(String)",
	"project_keys":    "Array(String)",
	"repo_patterns":   "Array(String)",
	"is_active":       "UInt8",
	"updated_at":      "DateTime64(6, 'UTC')",
	"last_synced":     "DateTime64(6, 'UTC')",
	"org_id":          "String",
	"provider":        "String",
	"native_team_key": "Nullable(String)",
	"parent_team_id":  "Nullable(String)",
	"source_id":       "Nullable(UUID)",
	"created_at":      "Nullable(DateTime64(6, 'UTC'))",
	"deleted_at":      "Nullable(DateTime64(6, 'UTC'))",
}

// ColumnTypes returns the column types Insert sends, for the schema census.
func ColumnTypes() map[string]string {
	out := make(map[string]string, len(columnTypes))
	for column, columnType := range columnTypes {
		out[column] = columnType
	}
	return out
}

// Insert writes rows into teams in ONE INSERT..SELECT. provider_team_id,
// provider_name and gone_at are read from the stored versions of each team
// inside that statement (the Query rule: the newest non-empty id and name,
// the newest version's gone_at), and so is manual_members of a RosterKept
// or RosterDelta row (teams FINAL) and of a RosterSource row (its source
// team's newest version), so no write of another writer can land
// between the read and the insert of a separate statement and be erased.
// The SELECT still reads a snapshot taken when the statement starts, so a
// write that commits while the statement runs is not seen: ClickHouse has no
// transaction across statements or inside one. Rows are inserted in the
// order given (the last inserted row wins an equal version).
func Insert(ctx context.Context, conn Execer, orgID string, columns []string, rows []Write) error {
	if isNilExecer(conn) || strings.TrimSpace(orgID) == "" {
		return ErrInvalidArguments
	}
	if len(rows) == 0 {
		return nil
	}
	keep, source, admin := false, false, false
	for r, row := range rows {
		if row.Kept&^keptAll != 0 {
			return fmt.Errorf("%w: row %d keeps unknown columns %d", ErrInvalidWrite, r, row.Kept)
		}
		admin = admin || row.Kept != 0 || row.VersionAfterStored || row.SkipDeleted
		if row.Roster > RosterSource {
			return fmt.Errorf("%w: row %d has roster rule %d", ErrInvalidWrite, r, row.Roster)
		}
		if row.Roster == RosterSource && strings.TrimSpace(row.RosterSource) == "" {
			return fmt.Errorf("%w: row %d takes its roster from no team", ErrInvalidWrite, r)
		}
		keep = keep || row.Roster != RosterStated
		source = source || row.Roster == RosterSource
	}
	query, idIndex, orgIndex, err := insertStatement(columns, keep, source, admin)
	if err != nil {
		return err
	}
	params := make([][]any, len(columns))
	for i := range params {
		params[i] = make([]any, len(rows))
	}
	ids := make([]string, len(rows))
	providerIDs := make([]string, len(rows))
	providerNames := make([]string, len(rows))
	goneRules := make([]uint32, len(rows))
	goneValues := make([]any, len(rows))
	ordinals := make([]uint32, len(rows))
	rosterRules := make([]uint32, len(rows))
	rosterAdd := make([]any, len(rows))
	rosterRemove := make([]any, len(rows))
	rosterSources := make([]string, len(rows))
	rosterReadAt := make([]any, len(rows))
	rosterReadSynced := make([]any, len(rows))
	keptMasks := make([]uint32, len(rows))
	versionAfter := make([]uint32, len(rows))
	skipDeleted := make([]uint32, len(rows))
	for r, row := range rows {
		if len(row.Values) != len(columns) {
			return fmt.Errorf("%w: row %d has %d values for %d columns", ErrInvalidWrite, r, len(row.Values), len(columns))
		}
		if row.Gone > GoneStated {
			return fmt.Errorf("%w: row %d has gone rule %d", ErrInvalidWrite, r, row.Gone)
		}
		id, ok := row.Values[idIndex].(string)
		if !ok || id == "" {
			return fmt.Errorf("%w: row %d has no team id", ErrInvalidWrite, r)
		}
		if orgIndex >= 0 {
			if org, _ := row.Values[orgIndex].(string); org != orgID {
				return fmt.Errorf("%w: row %d is not of the organization written", ErrInvalidWrite, r)
			}
		}
		ids[r] = id
		for c, value := range row.Values {
			params[c][r], err = paramValue(columnTypes[columns[c]], value)
			if err != nil {
				return fmt.Errorf("%w: row %d column %s: %v", ErrInvalidWrite, r, columns[c], err)
			}
		}
		providerIDs[r] = strings.TrimSpace(row.ProviderTeamID)
		providerNames[r] = strings.TrimSpace(row.ProviderName)
		goneRules[r] = uint32(row.Gone)
		goneValues[r] = nil
		if row.Gone == GoneStated && row.GoneAt != nil {
			goneValues[r] = utcText(*row.GoneAt)
		}
		ordinals[r] = uint32(r)
		rosterRules[r] = uint32(row.Roster)
		rosterAdd[r] = append([]string{}, row.RosterAdd...)
		rosterRemove[r] = append([]string{}, row.RosterRemove...)
		rosterSources[r] = strings.TrimSpace(row.RosterSource)
		rosterReadAt[r] = versionText(row.RosterSourceRead.UpdatedAt)
		rosterReadSynced[r] = versionText(row.RosterSourceRead.LastSynced)
		keptMasks[r] = uint32(row.Kept)
		if row.VersionAfterStored {
			versionAfter[r] = 1
		}
		if row.SkipDeleted {
			skipDeleted[r] = 1
		}
	}
	var rules [3]int
	for _, row := range rows {
		rules[row.Gone]++
	}
	synclog.Default().Debug(ctx, synclog.MsgTeamsInsertCarriesProviderIdentity,
		synclog.Count(synclog.KeyTeamRows, len(rows)), synclog.Count(synclog.KeyGoneKept, rules[GoneKept]),
		synclog.Count(synclog.KeyGoneCleared, rules[GoneCleared]), synclog.Count(synclog.KeyGoneStated, rules[GoneStated]))
	args := make([]any, 0, len(columns)+8)
	for c := range columns {
		args = append(args, clickhouse.Named(fmt.Sprintf("v%d", c), params[c]))
	}
	args = append(args,
		clickhouse.Named("identity_id", providerIDs),
		clickhouse.Named("identity_name", providerNames),
		clickhouse.Named("gone_rule", goneRules),
		clickhouse.Named("gone_in", goneValues),
		clickhouse.Named("ordinal", ordinals),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("team_ids", ids),
	)
	if keep {
		args = append(args, clickhouse.Named("roster_rule", rosterRules),
			clickhouse.Named("roster_add", rosterAdd), clickhouse.Named("roster_remove", rosterRemove))
	}
	if source {
		args = append(args, clickhouse.Named("roster_source", rosterSources),
			clickhouse.Named("roster_read_at", rosterReadAt), clickhouse.Named("roster_read_synced", rosterReadSynced))
	}
	if admin {
		args = append(args, clickhouse.Named("kept_mask", keptMasks),
			clickhouse.Named("version_after", versionAfter), clickhouse.Named("skip_deleted", skipDeleted))
	}
	return conn.Exec(ctx, query, args...)
}

// StatedColumns returns the columns of an `INSERT INTO teams (...)` statement
// before the provider identity columns, which it must end with: the columns a
// writer states for Insert.
func StatedColumns(insert string) ([]string, error) {
	open, end := strings.IndexByte(insert, '('), strings.LastIndexByte(insert, ')')
	if open < 0 || end < open {
		return nil, fmt.Errorf("%w: no column list", ErrInvalidWrite)
	}
	var columns []string
	for _, column := range strings.Split(insert[open+1:end], ",") {
		columns = append(columns, strings.TrimSpace(column))
	}
	identity := []string{ColumnProviderTeamID, ColumnProviderName, ColumnGoneAt}
	if len(columns) <= len(identity) || strings.Join(columns[len(columns)-3:], ",") != strings.Join(identity, ",") {
		return nil, fmt.Errorf("%w: the column list does not end with the provider identity", ErrInvalidWrite)
	}
	return columns[:len(columns)-3], nil
}

func insertStatement(columns []string, keepRoster, sourceRoster, admin bool) (query string, idIndex, orgIndex int, err error) {
	idIndex, orgIndex, rosterIndex := -1, -1, -1
	index := map[string]int{}
	seen := map[string]bool{}
	arrays := make([]string, 0, len(columns)+5)
	fields := make([]string, 0, len(columns)+5)
	selected := make([]string, 0, len(columns)+3)
	for c, column := range columns {
		columnType, known := columnTypes[column]
		if !known || seen[column] {
			return "", 0, 0, fmt.Errorf("%w: column %q", ErrInvalidWrite, column)
		}
		seen[column] = true
		index[column] = c
		switch column {
		case "id":
			idIndex = c
		case "org_id":
			orgIndex = c
		case "manual_members":
			rosterIndex = c
		}
		arrays = append(arrays, fmt.Sprintf("{v%d:Array(%s)}", c, columnType))
		fields = append(fields, fmt.Sprintf("incoming_row.%d AS v%d", c+1, c))
		selected = append(selected, fmt.Sprintf("incoming.v%d", c))
	}
	if idIndex < 0 {
		return "", 0, 0, fmt.Errorf("%w: no id column", ErrInvalidWrite)
	}
	if keepRoster && rosterIndex < 0 {
		return "", 0, 0, fmt.Errorf("%w: a kept roster with no manual_members column", ErrInvalidWrite)
	}
	if admin {
		for _, column := range []string{"name", "description", "project_keys", "repo_patterns", "updated_at"} {
			if !seen[column] {
				return "", 0, 0, fmt.Errorf("%w: an admin write with no %s column", ErrInvalidWrite, column)
			}
		}
	}
	n := len(columns)
	arrays = append(arrays, "{identity_id:Array(String)}", "{identity_name:Array(String)}",
		"{gone_rule:Array(UInt32)}", "{gone_in:Array(Nullable(DateTime64(6, 'UTC')))}", "{ordinal:Array(UInt32)}")
	fields = append(fields,
		fmt.Sprintf("incoming_row.%d AS identity_id", n+1), fmt.Sprintf("incoming_row.%d AS identity_name", n+2),
		fmt.Sprintf("incoming_row.%d AS gone_rule", n+3), fmt.Sprintf("incoming_row.%d AS gone_in", n+4), fmt.Sprintf("incoming_row.%d AS ordinal", n+5))
	selected = append(selected,
		"if(incoming.identity_id != '', incoming.identity_id, ifNull(stored.stored_team_id, ''))",
		"if(incoming.identity_name != '', incoming.identity_name, ifNull(stored.stored_name, ''))",
		fmt.Sprintf("multiIf(incoming.gone_rule = %d, NULL, incoming.gone_rule = %d, CAST(incoming.gone_in, 'Nullable(DateTime64(6))'), stored.stored_gone)", GoneCleared, GoneStated))
	keptJoin := ""
	if keepRoster {
		arrays = append(arrays, "{roster_rule:Array(UInt32)}", "{roster_add:Array(Array(String))}", "{roster_remove:Array(Array(String))}")
		fields = append(fields, fmt.Sprintf("incoming_row.%d AS roster_rule", n+6),
			fmt.Sprintf("incoming_row.%d AS roster_add", n+7), fmt.Sprintf("incoming_row.%d AS roster_remove", n+8))
		sourceRule := ""
		if sourceRoster {
			arrays = append(arrays, "{roster_source:Array(String)}", "{roster_read_at:Array(DateTime64(6, 'UTC'))}", "{roster_read_synced:Array(DateTime64(6, 'UTC'))}")
			fields = append(fields, fmt.Sprintf("incoming_row.%d AS roster_source", n+9),
				fmt.Sprintf("incoming_row.%d AS roster_read_at", n+10), fmt.Sprintf("incoming_row.%d AS roster_read_synced", n+11))
			sourceRule = fmt.Sprintf("incoming.roster_rule = %d AND moved_from.moved_from_version > "+
				"(toUnixTimestamp64Micro(incoming.roster_read_at), toUnixTimestamp64Micro(incoming.roster_read_synced)), moved_from.moved_from_manual, ", RosterSource)
		}
		selected[rosterIndex] = fmt.Sprintf("multiIf(incoming.roster_rule = %d, kept.kept_manual, "+
			"incoming.roster_rule = %d, arraySort(arrayDistinct(arrayFilter(m -> m != '' AND NOT has(incoming.roster_remove, m), "+
			"arrayConcat(kept.kept_manual, incoming.roster_add)))), %sincoming.v%d)", RosterKept, RosterDelta, sourceRule, rosterIndex)
	}
	if keepRoster || admin {
		kept := keptRosterQuery
		if admin {
			kept = keptAdminQuery
		}
		keptJoin = "LEFT JOIN (" + kept + ") AS kept ON kept.kept_id = incoming.v" + fmt.Sprint(idIndex) + " "
	}
	if keepRoster && sourceRoster {
		keptJoin += "LEFT JOIN (" + sourceRosterQuery + ") AS moved_from ON moved_from.moved_from_id = incoming.roster_source "
	}
	where := ""
	if admin {
		arrays = append(arrays, "{kept_mask:Array(UInt32)}", "{version_after:Array(UInt32)}", "{skip_deleted:Array(UInt32)}")
		fields = append(fields, fmt.Sprintf("incoming_row.%d AS kept_mask", len(fields)+1),
			fmt.Sprintf("incoming_row.%d AS version_after", len(fields)+2), fmt.Sprintf("incoming_row.%d AS skip_deleted", len(fields)+3))
		for _, kept := range keptColumns {
			c := index[kept.column]
			selected[c] = fmt.Sprintf("if(bitAnd(incoming.kept_mask, %d) != 0 AND kept.kept_found = 1, kept.kept_%s, incoming.v%d)", kept.bit, kept.column, c)
		}
		updated := index["updated_at"]
		selected[updated] = fmt.Sprintf("if(incoming.version_after = 1 AND newest.newest_found = 1 AND newest.newest_at >= incoming.v%d, "+
			"addMicroseconds(newest.newest_at, 1), incoming.v%d)", updated, updated)
		keptJoin += "LEFT JOIN (" + newestVersionQuery + ") AS newest ON newest.newest_id = incoming.v" + fmt.Sprint(idIndex) + " "
		where = "WHERE NOT (incoming.skip_deleted = 1 AND (kept.kept_found != 1 OR kept.kept_deleted_at IS NOT NULL)) "
	}
	query = "INSERT INTO teams (" + strings.Join(columns, ", ") + ", " + Columns + ") " +
		"SELECT " + strings.Join(selected, ", ") + " " +
		"FROM (SELECT " + strings.Join(fields, ", ") + " FROM (SELECT arrayJoin(arrayZip(" + strings.Join(arrays, ", ") + ")) AS incoming_row)) AS incoming " +
		"LEFT JOIN (" + storedIdentityQuery + ") AS stored ON stored.stored_id = incoming.v" + fmt.Sprint(idIndex) + " " +
		keptJoin + where +
		"ORDER BY incoming.ordinal"
	return query, idIndex, orgIndex, nil
}

// keptRosterQuery is the stored roster a RosterKept row writes and a
// RosterDelta row edits: manual_members of the team's teams FINAL row. A team
// with no row gets the column default [], and so does a team whose newest
// version is an admin delete: the delete ends the manual roster, so a team
// written again after it (by an admin or a sync) starts with none.
const keptRosterQuery = "SELECT id AS kept_id, if(deleted_at IS NULL, manual_members, []) AS kept_manual FROM teams FINAL " +
	"WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}"

// keptAdminQuery is keptRosterQuery with the columns an admin write keeps
// (Kept), whether the team has a teams FINAL row (kept_found) and its
// deleted_at.
const keptAdminQuery = "SELECT id AS kept_id, if(deleted_at IS NULL, manual_members, []) AS kept_manual, 1 AS kept_found, " +
	"name AS kept_name, description AS kept_description, project_keys AS kept_project_keys, repo_patterns AS kept_repo_patterns, " +
	"deleted_at AS kept_deleted_at FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}"

// newestVersionQuery is the newest updated_at of each team over every stored
// version, deleted too: the version a VersionAfterStored row moves past.
const newestVersionQuery = "SELECT id AS newest_id, 1 AS newest_found, toTimeZone(max(updated_at), 'UTC') AS newest_at " +
	"FROM teams WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id"

// sourceRosterQuery is the newest stored version of each source team of a
// RosterSource row, by (updated_at, last_synced) over every raw version (a
// version of one id from another provider included, as teams FINAL would
// fold it), and its roster: [] when that version is an admin delete, as
// keptRosterQuery reads it.
const sourceRosterQuery = "SELECT id AS moved_from_id, " +
	"argMax(if(deleted_at IS NULL, manual_members, []), (updated_at, last_synced)) AS moved_from_manual, " +
	"max((toUnixTimestamp64Micro(updated_at), toUnixTimestamp64Micro(last_synced))) AS moved_from_version " +
	"FROM teams WHERE org_id = {org_id:String} AND id IN {roster_source:Array(String)} GROUP BY id"

// storedIdentitySelect is Query's rule with the names a writer joins on:
// stored_id, stored_team_id, stored_name and stored_gone.
const storedIdentitySelect = "SELECT id AS stored_id, " +
	"argMaxIf(provider_team_id, (updated_at, last_synced), provider_team_id != '') AS stored_team_id, " +
	"argMaxIf(provider_name, (updated_at, last_synced), provider_name != '') AS stored_name, " +
	"nullIf(argMax(ifNull(gone_at, toDateTime64(0, 6)), (updated_at, last_synced)), toDateTime64(0, 6)) AS stored_gone " +
	"FROM teams WHERE org_id = {org_id:String}"

const storedIdentityQuery = storedIdentitySelect + " AND id IN {team_ids:Array(String)} GROUP BY id"

// StoredIdentityOfOrganization is the stored identity of every team of the
// organization {org_id:String} by Query's rule. An INSERT..SELECT INTO teams
// outside Insert joins it (ON stored_id = id) and selects
// stored_team_id, stored_name and stored_gone: it is a writer of teams too.
const StoredIdentityOfOrganization = storedIdentitySelect + " GROUP BY id"

// Restatement is one stored team that a writer writes again from its newest
// stored version: AddMembers are the manual members it gains (the ones it
// holds already are not added twice), and Activate writes it active.
type Restatement struct {
	ID         string
	AddMembers []string
	Activate   bool
}

// Restate writes each team again in ONE INSERT INTO teams .. SELECT FROM
// teams FINAL: every column but manual_members, is_active and created_at is
// the newest stored version's own, at its own updated_at, so a write of
// another writer that committed before the statement starts is the version it
// copies and is not lost. created_at is the oldest creation evidence over
// every version and the provider identity is Insert's rule. A team whose
// newest version is deleted, or that has no row, is not written. It returns
// the rows the server reports written; a connection that reports no progress
// reports 0.
func Restate(ctx context.Context, conn Execer, orgID string, rows []Restatement) (uint64, error) {
	if isNilExecer(conn) || strings.TrimSpace(orgID) == "" {
		return 0, ErrInvalidArguments
	}
	if len(rows) == 0 {
		return 0, nil
	}
	ids := make([]string, len(rows))
	// []any of the element types Insert proves (Array(Array(String)),
	// Array(UInt8)): the parameter-type census holds every bound type to an
	// executed encoding case.
	members := make([]any, len(rows))
	activate := make([]any, len(rows))
	seen := map[string]bool{}
	for r, row := range rows {
		if row.ID == "" || seen[row.ID] {
			return 0, fmt.Errorf("%w: restatement %d has no team id or repeats one", ErrInvalidWrite, r)
		}
		seen[row.ID] = true
		ids[r] = row.ID
		members[r] = append([]string{}, row.AddMembers...)
		activate[r] = uint8(0)
		if row.Activate {
			activate[r] = uint8(1)
		}
	}
	var written uint64
	ctx = clickhouse.Context(ctx, clickhouse.WithProgress(func(progress *clickhouse.Progress) {
		written += progress.WroteRows
	}))
	err := conn.Exec(ctx, restateStatement(),
		clickhouse.Named("restate_ids", ids),
		clickhouse.Named("restate_members", members),
		clickhouse.Named("restate_activate", activate),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("team_ids", ids),
	)
	if err != nil {
		return 0, err
	}
	return written, nil
}

// restateStatement selects every column of columnTypes (the teams schema
// Insert writes) from the stored row, in a fixed order.
func restateStatement() string {
	columns := make([]string, 0, len(columnTypes))
	for column := range columnTypes {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	selected := make([]string, 0, len(columns)+3)
	for _, column := range columns {
		switch column {
		case "manual_members":
			selected = append(selected, "arrayConcat(t.manual_members, arrayDistinct(arrayFilter(m -> NOT has(t.manual_members, m), incoming.add_members)))")
		case "is_active":
			selected = append(selected, "if(incoming.activate = 1, toUInt8(1), t.is_active)")
		case "created_at":
			selected = append(selected, "oldest.created_min")
		default:
			selected = append(selected, "t."+column)
		}
	}
	selected = append(selected,
		"ifNull(stored.stored_team_id, '')", "ifNull(stored.stored_name, '')", "stored.stored_gone")
	return "INSERT INTO teams (" + strings.Join(columns, ", ") + ", " + Columns + ") " +
		"SELECT " + strings.Join(selected, ", ") + " " +
		"FROM teams AS t FINAL " +
		"INNER JOIN (SELECT incoming_row.1 AS restate_id, incoming_row.2 AS add_members, incoming_row.3 AS activate " +
		"FROM (SELECT arrayJoin(arrayZip({restate_ids:Array(String)}, {restate_members:Array(Array(String))}, {restate_activate:Array(UInt8)})) AS incoming_row)) AS incoming " +
		"ON incoming.restate_id = t.id " +
		"LEFT JOIN (" + storedIdentityQuery + ") AS stored ON stored.stored_id = t.id " +
		"LEFT JOIN (" + oldestCreatedQuery + ") AS oldest ON oldest.created_id = t.id " +
		"WHERE t.org_id = {org_id:String} AND t.deleted_at IS NULL"
}

// oldestCreatedQuery is the oldest creation evidence of each team over every
// stored version: a newer version written without created_at (NULL) must not
// hide the time an older one holds.
const oldestCreatedQuery = "SELECT id AS created_id, min(coalesce(created_at, updated_at)) AS created_min " +
	"FROM teams WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id"

// paramValue makes a value safe for a query parameter array of the column's
// type: a time is sent as UTC text (the parameter type names UTC, so no
// server time zone shifts it), a bool as 0/1, and a nil pointer as NULL.
func paramValue(columnType string, value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case time.Time:
		return utcText(v), nil
	case *time.Time:
		if v == nil {
			return nil, nil
		}
		return utcText(*v), nil
	case bool:
		if v {
			return uint8(1), nil
		}
		return uint8(0), nil
	}
	if strings.Contains(columnType, "DateTime64") {
		return nil, fmt.Errorf("a %T for a time column", value)
	}
	return value, nil
}

// versionText is a read version as UTC text; no time (a row of another rule)
// is the Unix epoch, the oldest time the parameter type holds.
func versionText(t time.Time) string {
	if t.IsZero() {
		t = time.Unix(0, 0)
	}
	return utcText(t)
}

func utcText(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}

func isNilExecer(conn Execer) bool {
	if conn == nil {
		return true
	}
	v := reflect.ValueOf(conn)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
