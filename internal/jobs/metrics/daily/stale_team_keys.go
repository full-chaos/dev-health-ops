package daily

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// # The stale-key rule of the team-keyed daily tables
//
// A daily table whose sorting key holds a team id stores one row for each
// (scope, day, team). The tables are append only: a recompute writes new rows
// and a reader takes the newest row of each KEY. So when a recompute gives a
// day's work to another team id, the row under the old team id is a different
// key and stays the newest row of that key. A read with no team filter then
// counts the day under both ids.
//
// A team id changes for a stored day when a team is set inactive (a carry to
// a keyed id, a retired project-as-team row, an admin delete) and when an item
// moves to another team.
//
// The rule, the same for every such table: a run writes a ROW OF ZEROS over
// each live key of its own scope and day that it did not produce. Live means
// that the newest row of the key holds a measure. A key whose newest row is a
// row of zeros is left alone, so a second run writes nothing.
//
// A row of zeros holds the key, computed_at, 0 in every count and NULL in
// every Nullable measure. It is written only over a key that held a measure,
// never for a day or a team with no data: missing stays missing.
//
// The set of tables is StaleTeamKeyTables. issue_type_metrics_daily and
// investment_metrics_daily hold the same rule in their own writers
// (withIssueTypeMetricsZeroRows, withInvestmentMetricsZeroRows): they are plain
// MergeTree tables and their keys add the repository.

// StaleKeyTable is the declaration of one team-keyed daily table. The
// declarations live in package teamkeytables, which also builds the predicate
// the readers use, so the writer and the readers agree on what a row of zeros
// is.
type StaleKeyTable = teamkeytables.Table

// StaleTeamKeyTables is every table that takes the shared rule. The census
// (stale_team_keys_census_test.go) holds it against the schema baseline.
func StaleTeamKeyTables() []StaleKeyTable { return teamkeytables.All() }

// staleKey is the values of one key, in the order of StaleKeyTable.Keys. A
// NULL of a Nullable key column is the empty string, and a NULL of a
// Nullable(UUID) key column is the nil id: the forms the sorting key holds.
type staleKey []string

func (key staleKey) text() string { return strings.Join(key, "\x00") }

// staleKeyScope is the scope of one run: the values of StaleKeyTable.Scope of
// every unit the run computed. Nil is the whole organization and day.
type staleKeyScope map[string]struct{}

// newStaleKeyScope builds a scope from the values of the scope columns, one
// tuple for each unit.
func newStaleKeyScope(tuples ...[]string) staleKeyScope {
	scope := make(staleKeyScope, len(tuples))
	for _, tuple := range tuples {
		scope[strings.Join(tuple, "\x00")] = struct{}{}
	}
	return scope
}

// staleKeyConn is what the rule needs of a connection.
type staleKeyConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// loadLiveStaleKeys returns the live keys of one organization and day.
func loadLiveStaleKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
) ([]staleKey, error) {
	rows, err := conn.Query(ctx, table.LiveKeysQuery(), organizationID, staleKeyDay(day))
	if err != nil {
		return nil, fmt.Errorf("load %s live keys: %w", table.Table, err)
	}
	defer rows.Close()
	var keys []staleKey
	for rows.Next() {
		values := make([]string, len(table.Keys))
		targets := make([]any, len(values))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan %s live key: %w", table.Table, err)
		}
		keys = append(keys, staleKey(values))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s live keys: %w", table.Table, err)
	}
	return keys, nil
}

func staleKeyDay(day time.Time) time.Time {
	utc := day.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// staleKeysOf is the rule itself, with no I/O: the live keys of the run's
// scope that the run did not produce.
func staleKeysOf(table StaleKeyTable, live []staleKey, scope staleKeyScope, produced []staleKey) []staleKey {
	held := make(map[string]struct{}, len(produced))
	for _, key := range produced {
		held[key.text()] = struct{}{}
	}
	var stale []staleKey
	for _, key := range live {
		if _, ok := held[key.text()]; ok {
			continue
		}
		if len(table.Scope) > 0 {
			if _, inScope := scope[table.ScopeTuple(key)]; !inScope {
				continue
			}
		}
		stale = append(stale, key)
	}
	sort.Slice(stale, func(left, right int) bool { return stale[left].text() < stale[right].text() })
	return stale
}

// supersedeStaleTeamKeys applies the rule for one table after a run wrote its
// rows: it reads the live keys of the organization and day, keeps the ones of
// the run's scope that the run did not produce, and writes a row of zeros over
// each. It returns the number of rows of zeros.
//
// scope is the values of table.Scope of every unit the run computed; it is
// ignored for a table with no scope column. produced is every key the run
// wrote, in the order of table.Keys.
//
// A failed read is returned: the run then fails and is tried again, and a
// stale key is never taken as "no stale key". The write reports its row count
// on a Send error, because the rows may be stored.
func supersedeStaleTeamKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	scope staleKeyScope, produced []staleKey, computedAt time.Time,
) (int, error) {
	if computedAt.IsZero() {
		return 0, ErrInvalidState
	}
	return supersedeStaleTeamKeysAt(ctx, conn, table, organizationID, day, scope, produced,
		func(staleKey) time.Time { return computedAt })
}

// supersedeStaleTeamKeysAt is supersedeStaleTeamKeys for a family that gives
// the rows of one run more than one computed_at: computedAtOf returns the
// computed_at of the row of zeros of one key. A reader that takes the rows of
// the newest computed_at of a group (team_metrics_daily, by repository) must
// find the row of zeros in the same generation as the rows the run wrote for
// that group.
func supersedeStaleTeamKeysAt(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	scope staleKeyScope, produced []staleKey, computedAtOf func(staleKey) time.Time,
) (int, error) {
	if err := table.Valid(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || computedAtOf == nil {
		return 0, ErrInvalidState
	}
	if len(table.Scope) > 0 && len(scope) == 0 {
		// A run with no unit computed nothing: it holds no key to supersede.
		return 0, nil
	}
	live, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	return writeStaleKeyZeroRows(ctx, conn, table, organizationID, day, staleKeysOf(table, live, scope, produced), computedAtOf)
}

// retractStaleTeamKeysOfRun applies the rule once for a whole run, for a table
// whose keys more than one partition of a run can write (a work scope with
// items in the repositories of two partitions; a table that every partition
// computes for the whole organization).
//
// A partition cannot decide such a key. What a partition "did not produce" is
// measured against its own read, and its read can be older than the write of
// another partition of the same run: it then writes a row of zeros over the
// row the other partition just wrote. So the families of these tables write no
// row of zeros, and this runs once, after every partition of the run is done.
//
// The order of the two reads is the rule:
//
//  1. the live keys of the day are read FIRST;
//  2. THEN keys computes the keys of the day from the stored inputs as they
//     are now (the same compute the family runs);
//  3. a live key of the run's scope that step 2 did not compute gets a row of
//     zeros.
//
// A key that is right holds its inputs before its row is written (a family
// writes its rows after it read them). So a right key that is live in step 1
// is computed in step 2, whatever wrote it and whenever: no clock and no
// insert order decides which key is superseded. computedAt is only the
// version of the rows of zeros, and it is raised above the stored rows of the
// day when this host's clock is behind them.
func retractStaleTeamKeysOfRun(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	keys func(context.Context) (staleKeyScope, []staleKey, error), computedAt time.Time,
) (int, error) {
	if err := table.Valid(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || keys == nil || computedAt.IsZero() {
		return 0, ErrInvalidState
	}
	live, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	if len(live) == 0 {
		return 0, nil
	}
	scope, computed, err := keys(ctx)
	if err != nil {
		return 0, err
	}
	stale := staleKeysOf(table, live, scope, computed)
	if len(stale) == 0 {
		return 0, nil
	}
	// A row of zeros must be newer than the row it supersedes, or the old row
	// stays the newest row of its key. The clock of this host cannot promise
	// that: the old row can come from a host whose clock is ahead, and several
	// of the tables keep computed_at to the second. So the version is taken
	// from the stored rows: later than the newest row of the day by one
	// second, when this host's clock is not later still.
	newest, err := newestStaleKeyVersion(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	version := computedAt.UTC()
	if floor := newest.UTC().Add(time.Second); floor.After(version) {
		version = floor
	}
	return writeStaleKeyZeroRows(ctx, conn, table, organizationID, day, stale,
		func(staleKey) time.Time { return version })
}

// newestStaleKeyVersion is the newest computed_at of the rows of one
// organization and day.
func newestStaleKeyVersion(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
) (time.Time, error) {
	where := ""
	if table.Where != "" {
		where = " AND (" + table.Where + ")"
	}
	rows, err := conn.Query(ctx, "SELECT max(computed_at) FROM "+table.Table+
		" WHERE org_id = ? AND "+table.DayColumn+" = ?"+where, organizationID, staleKeyDay(day))
	if err != nil {
		return time.Time{}, fmt.Errorf("load the newest %s version: %w", table.Table, err)
	}
	defer rows.Close()
	var newest time.Time
	if rows.Next() {
		if err := rows.Scan(&newest); err != nil {
			return time.Time{}, fmt.Errorf("scan the newest %s version: %w", table.Table, err)
		}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("read the newest %s version: %w", table.Table, err)
	}
	return newest, nil
}

// writeStaleKeyZeroRows writes one row of zeros over each key. It reports its
// row count on a Send error, because the rows may be stored.
func writeStaleKeyZeroRows(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	stale []staleKey, computedAtOf func(staleKey) time.Time,
) (int, error) {
	if len(stale) == 0 {
		return 0, nil
	}
	names := make([]string, 0, len(table.Keys))
	for _, column := range table.Keys {
		names = append(names, column.Name)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table.Table+
		" (org_id, "+table.DayColumn+", "+strings.Join(names, ", ")+", computed_at)")
	if err != nil {
		return 0, fmt.Errorf("prepare %s zero rows: %w", table.Table, err)
	}
	dayValue := staleKeyDay(day)
	for _, key := range stale {
		computedAt := computedAtOf(key)
		if computedAt.IsZero() {
			return 0, fmt.Errorf("%w: %s zero row has no computed_at", ErrInvalidState, table.Table)
		}
		values := make([]any, 0, len(key)+3)
		values = append(values, organizationID, dayValue)
		for index, column := range table.Keys {
			value, err := column.StoredValue(key[index])
			if err != nil {
				return 0, err
			}
			values = append(values, value)
		}
		values = append(values, computedAt.UTC())
		if err := batch.Append(values...); err != nil {
			return 0, fmt.Errorf("append %s zero row: %w", table.Table, err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(stale), fmt.Errorf("send %s zero rows: %w", table.Table, err)
	}
	return len(stale), nil
}
