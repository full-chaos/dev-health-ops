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

// Write is one version of a team for Insert. Values are the writer's own
// columns, in Insert's column order. A non-empty ProviderTeamID or
// ProviderName is the run's own value; an empty one keeps the newest
// non-empty stored value.
type Write struct {
	Values         []any
	ProviderTeamID string
	ProviderName   string
	Gone           Gone
	GoneAt         *time.Time
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
// the newest version's gone_at), so no write of another writer can land
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
	query, idIndex, orgIndex, err := insertStatement(columns)
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

func insertStatement(columns []string) (query string, idIndex, orgIndex int, err error) {
	idIndex, orgIndex = -1, -1
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
		switch column {
		case "id":
			idIndex = c
		case "org_id":
			orgIndex = c
		}
		arrays = append(arrays, fmt.Sprintf("{v%d:Array(%s)}", c, columnType))
		fields = append(fields, fmt.Sprintf("incoming_row.%d AS v%d", c+1, c))
		selected = append(selected, fmt.Sprintf("incoming.v%d", c))
	}
	if idIndex < 0 {
		return "", 0, 0, fmt.Errorf("%w: no id column", ErrInvalidWrite)
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
	query = "INSERT INTO teams (" + strings.Join(columns, ", ") + ", " + Columns + ") " +
		"SELECT " + strings.Join(selected, ", ") + " " +
		"FROM (SELECT " + strings.Join(fields, ", ") + " FROM (SELECT arrayJoin(arrayZip(" + strings.Join(arrays, ", ") + ")) AS incoming_row)) AS incoming " +
		"LEFT JOIN (" + storedIdentityQuery + ") AS stored ON stored.stored_id = incoming.v" + fmt.Sprint(idIndex) + " " +
		"ORDER BY incoming.ordinal"
	return query, idIndex, orgIndex, nil
}

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
