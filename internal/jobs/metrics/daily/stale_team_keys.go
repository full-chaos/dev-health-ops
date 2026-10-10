package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// Who applies the rule depends on who can write a key. A table whose keys two
// partitions of one run can write is decided once for the run, after every
// partition is done (retractStaleTeamKeysOfRun, stale_team_keys_run.go). A
// table whose key scope is the partition's own repository, and a table that a
// finalize family writes once for a run, keep the rule in their family
// (supersedeStaleTeamKeys). The census holds that split.
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

// loadLiveStaleKeys returns the live keys of one organization and day, and for
// each the newest computed_at stored under it.
func loadLiveStaleKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
) ([]staleKey, map[string]time.Time, error) {
	rows, err := conn.Query(ctx, table.LiveKeyVersionsQuery(), organizationID, staleKeyDay(day))
	if err != nil {
		return nil, nil, fmt.Errorf("load %s live keys: %w", table.Table, err)
	}
	defer rows.Close()
	var keys []staleKey
	versions := map[string]time.Time{}
	for rows.Next() {
		values := make([]string, len(table.Keys))
		var version time.Time
		targets := make([]any, 0, len(values)+1)
		for index := range values {
			targets = append(targets, &values[index])
		}
		targets = append(targets, &version)
		if err := rows.Scan(targets...); err != nil {
			return nil, nil, fmt.Errorf("scan %s live key: %w", table.Table, err)
		}
		keys = append(keys, staleKey(values))
		versions[staleKey(values).text()] = version
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate %s live keys: %w", table.Table, err)
	}
	return keys, versions, nil
}

// staleKeyVersionSteps holds, for each table of the rule, one unit of its
// computed_at column: the smallest step that gives a strictly newer version.
// A larger step would put a row of zeros further ahead of the clock than it
// must be, and a real row that a later compute writes for the same key at its
// own clock would then read as superseded for that much longer.
//
// A test reads the column types of the schema and fails when a step is not
// the unit of its column.
var staleKeyVersionSteps = map[string]time.Duration{
	teamkeytables.WorkItemMetricsDaily.Table:         time.Second,
	teamkeytables.WorkItemStateDurationsDaily.Table:  time.Second,
	teamkeytables.EstimateCoverageMetricsDaily.Table: time.Millisecond,
	teamkeytables.TeamMetricsDaily.Table:             time.Microsecond,
	teamkeytables.AIImpactMetricsDaily.Table:         time.Millisecond,
	teamkeytables.AIGovernanceCoverageDaily.Table:    time.Millisecond,
	teamkeytables.TeamCognitiveLoadDaily.Table:       time.Microsecond,
	teamkeytables.TeamComplexityDaily.Table:          time.Microsecond,
	teamkeytables.ICLandscapeRolling30d.Table:        time.Second,
	teamkeytables.CompoundingRiskDailyTeam.Table:     time.Second,
}

// staleKeyVersionAfter is the version of a row that must be strictly newer
// than a stored row: at, or one unit of the table's computed_at column after
// stored when at is not later than that. The floor is a value the column
// keeps exactly, so a later at, cut to the column's unit by the store, is
// still not before it.
func staleKeyVersionAfter(table StaleKeyTable, at, stored time.Time) (time.Time, error) {
	step, listed := staleKeyVersionSteps[table.Table]
	if !listed || step <= 0 {
		return time.Time{}, fmt.Errorf("%w: no computed_at unit is declared for %s", ErrInvalidState, table.Table)
	}
	version := at.UTC()
	if floor := stored.UTC().Add(step); floor.After(version) {
		version = floor
	}
	return version, nil
}

func staleKeyDay(day time.Time) time.Time {
	utc := day.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// staleKeysOf is the rule itself, with no I/O: the live keys of the run's
// scope that the run did not produce.
//
// everyScope is the scope of a run of the whole organization: it owns the
// whole day, so every live key it did not produce is stale, of any scope
// value. A work scope that the organization has no item in any more is not in
// scope (nothing computes it), and its keys of an earlier compute would stay
// for ever under the scope filter.
func staleKeysOf(table StaleKeyTable, live []staleKey, scope staleKeyScope, everyScope bool, produced []staleKey) []staleKey {
	held := make(map[string]struct{}, len(produced))
	for _, key := range produced {
		held[key.text()] = struct{}{}
	}
	var stale []staleKey
	for _, key := range live {
		if _, ok := held[key.text()]; ok {
			continue
		}
		if len(table.Scope) > 0 && !everyScope {
			if _, inScope := scope[table.ScopeTuple(key)]; !inScope {
				continue
			}
		}
		stale = append(stale, key)
	}
	sort.Slice(stale, func(left, right int) bool { return stale[left].text() < stale[right].text() })
	return stale
}

// supersedeStaleTeamKeys applies the rule for one table after a family wrote
// its rows: it reads the live keys of the organization and day, keeps the ones
// of the scope that the family did not produce, and writes a row of zeros over
// each. It returns the number of rows of zeros.
//
// It is for a table that no other partition of the run writes the same keys
// of: a table whose key scope is the partition's own repository, or a table a
// finalize family writes once for a run. A table that the partitions of a run
// share is decided by retractStaleTeamKeysOfRun.
//
// scope is the values of table.Scope of every unit the family computed; it is
// ignored for a table with no scope column. produced is every key the family
// wrote, in the order of table.Keys.
//
// The row of zeros of a key gets computedAt, or one unit of the table's
// computed_at column after the newest stored row of that key when computedAt
// is not later: a row of zeros is
// strictly newer than the row it supersedes, never of the same computed_at.
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
	if _, err := staleKeyVersionAfter(table, computedAt, time.Time{}); err != nil {
		return 0, err
	}
	return supersedeStaleKeys(ctx, conn, table, organizationID, day, scope, produced,
		func(_ staleKey, stored time.Time) time.Time {
			// The table has a declared unit (checked above).
			version, _ := staleKeyVersionAfter(table, computedAt, stored)
			return version
		})
}

// supersedeStaleTeamKeysAt is the rule for team_metrics_daily, whose family
// gives the rows of each repository their own computed_at: computedAtOf
// returns the computed_at of the row of zeros of one key, and the row gets
// exactly that. A reader that takes the rows of the newest computed_at of a
// repository must find the row of zeros in the same generation as the rows
// the family wrote for that repository, so this row is NOT raised above the
// row it supersedes (see the limits in the architecture document).
func supersedeStaleTeamKeysAt(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	scope staleKeyScope, produced []staleKey, computedAtOf func(staleKey) time.Time,
) (int, error) {
	if computedAtOf == nil {
		return 0, ErrInvalidState
	}
	return supersedeStaleKeys(ctx, conn, table, organizationID, day, scope, produced,
		func(key staleKey, _ time.Time) time.Time { return computedAtOf(key) })
}

// supersedeStaleKeys is the rule of a family. versionOf gets a stale key and
// the newest computed_at stored under it, and returns the computed_at of its
// row of zeros.
func supersedeStaleKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	scope staleKeyScope, produced []staleKey, versionOf func(key staleKey, stored time.Time) time.Time,
) (int, error) {
	if err := table.Valid(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || versionOf == nil {
		return 0, ErrInvalidState
	}
	if len(table.Scope) > 0 && len(scope) == 0 {
		// A run with no unit computed nothing: it holds no key to supersede.
		return 0, nil
	}
	live, versions, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	return writeStaleKeyZeroRows(ctx, conn, table, organizationID, day, staleKeysOf(table, live, scope, false, produced),
		func(key staleKey) time.Time { return versionOf(key, versions[key.text()]) })
}

// runStaleKeys is what the compute of a run gives the run-level rule for one
// table: the scope and the keys of the day, and for a table whose rows the
// run settles too, the write of those rows at a version.
type runStaleKeys struct {
	scope staleKeyScope
	// everyScope is true for a run of the whole organization: every live key
	// of the day that is not in keys is stale, whatever its scope value.
	everyScope bool
	// proveNoItem is the second read of an organization-wide run whose first
	// read gave no work item: it returns nil only when it states, by itself,
	// that the organization has no work item for the day.
	proveNoItem func(ctx context.Context) error
	keys        []staleKey
	// writeRows stores the rows of the day that were computed with the keys,
	// at the version. Nil for a table whose rows the partitions settle.
	writeRows func(ctx context.Context, version time.Time) (int, error)
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
//     zeros;
//  4. for a table whose rows the run settles (the three work-item tables),
//     the rows that step 2 computed are stored too, with the rows of zeros.
//     Two partitions that share a work scope both write the real rows of the
//     scope, each from the attributions stored at its read; the rows of this
//     step are computed once, after every partition wrote its attributions,
//     and are the newest version of their keys.
//
// A key that is right holds its inputs before its row is written (a family
// writes its rows after it read them). So a right key that is live in step 1
// is computed in step 2, whatever wrote it and whenever: no clock and no
// insert order decides which key is superseded. clock is only the version of
// the rows this step writes, and it is raised above the stored rows of the day
// when this host's clock is not later than they are.
func retractStaleTeamKeysOfRun(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	compute func(context.Context) (runStaleKeys, error), clock time.Time,
) (written, zeros int, err error) {
	if err := table.Valid(); err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || compute == nil || clock.IsZero() {
		return 0, 0, ErrInvalidState
	}
	live, _, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, 0, err
	}
	if len(live) == 0 {
		// The partitions stored no measure for the day: there is no key to
		// supersede and no row to settle.
		return 0, 0, nil
	}
	// Every row this step writes must be strictly newer than every row of the
	// day that is stored: a row of zeros newer than the row it supersedes, a
	// settled row newer than the row of a partition. The clock of this host
	// cannot promise that (a partition can run on a host whose clock is ahead,
	// and several tables keep computed_at to the second), so the version is
	// taken from the stored rows when the clock is not later.
	newest, err := newestStaleKeyVersion(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, 0, err
	}
	version, err := staleKeyVersionAfter(table, clock, newest)
	if err != nil {
		return 0, 0, err
	}
	computed, err := compute(ctx)
	if err != nil {
		return 0, 0, err
	}
	if computed.everyScope && len(computed.scope) == 0 {
		// The run of the whole organization read NO work item for a day that
		// holds stored keys with a measure. Every one of those keys is then
		// stale and gets a row of zeros: the whole day of the table. That is
		// right when the day is empty, and it is the worst result of a read
		// that came back empty for another reason. So an empty read alone
		// supersedes nothing: a second read of the same items must state
		// that there is none, or the step fails and the finalize is tried
		// again.
		if computed.proveNoItem == nil {
			return 0, 0, fmt.Errorf("%w: %s: the organization-wide read gave no work item and no proof of an empty day is set",
				ErrOrganizationDayNotProvenEmpty, table.Table)
		}
		if err := computed.proveNoItem(ctx); err != nil {
			return 0, 0, fmt.Errorf("%s: %w", table.Table, err)
		}
		slog.Default().Warn(StaleKeysEmptyOrganizationDayLogMessage,
			"organization_id", organizationID, "target_day", staleKeyDay(day).Format("2006-01-02"),
			"table", table.Table, "live_keys", len(live))
	}
	if computed.writeRows != nil {
		written, err = computed.writeRows(ctx, version)
		if err != nil {
			return written, 0, err
		}
	}
	zeros, err = writeStaleKeyZeroRows(ctx, conn, table, organizationID, day,
		staleKeysOf(table, live, computed.scope, computed.everyScope, computed.keys),
		func(staleKey) time.Time { return version })
	return written + zeros, zeros, err
}

// ErrOrganizationDayNotProvenEmpty means that a run of the whole organization
// read no work item for a day that holds stored keys, and the second read did
// not confirm that the day has none. Nothing is superseded.
var ErrOrganizationDayNotProvenEmpty = errors.New("daily stale team keys: the organization-day is not proven empty")

// StaleKeysEmptyOrganizationDayLogMessage is the line a run of the whole
// organization writes when it supersedes every stored key of a table for a
// day that two reads state to have no work item.
const StaleKeysEmptyOrganizationDayLogMessage = "daily stale team keys: the organization has no work item for the day; every stored key of the day is superseded"

// outsideRunRetraction is what one repository-scoped table got at the end of
// a run of the whole organization.
type outsideRunRetraction struct {
	// written is the rows of zeros written.
	written int
	// kept is the live keys left as they are: of a repository that the
	// organization holds and that is in no partition of the run.
	kept int
	// superseded is the repositories whose keys got a row of zeros, and
	// notInRun the repositories the organization holds that are in no
	// partition of the run. Both sorted, each repository once.
	superseded, notInRun []string
}

// retractStaleTeamKeysOutsideRun is the rule of a run of the whole
// organization for a table whose keys the partitions decide inside their own
// repositories (table.Scope is the repository): a live key of the day whose
// repository is in NO partition of the run, and that the organization does not
// hold now, gets a row of zeros. The keys of the run's repositories are left
// to their partition, which read and wrote them.
//
// Such a key is of a repository that is gone, or of a row written before the
// table held a repository, so no compute can make it right. owned is the
// repositories of every partition of the run, as scope tuples. A key of a
// repository that is present and in no partition is left as it is and counted
// (kept): the run's list is the one of its dispatch, and a run of that
// repository alone can have written the key since.
//
// readPresent gives the repositories the organization holds. It is called
// AFTER the live keys are read: a repository whose key that read saw exists
// in the present set unless it is gone. Read before, the set would not hold a
// repository that got its row and its keys between the two reads, and its keys
// would be superseded.
//
// The version is the clock, raised above every stored row of the day: the
// row of zeros is then the newest row of its key, and for a reader that keeps
// the newest generation of a repository it is the whole generation.
func retractStaleTeamKeysOutsideRun(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	owned staleKeyScope, readPresent func(context.Context) (staleKeyScope, error), clock time.Time,
) (outsideRunRetraction, error) {
	var result outsideRunRetraction
	if err := table.Valid(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || clock.IsZero() ||
		len(table.Scope) == 0 || readPresent == nil {
		return result, ErrInvalidState
	}
	live, _, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return result, err
	}
	present, err := readPresent(ctx)
	if err != nil {
		return result, fmt.Errorf("%s: %w", table.Table, err)
	}
	for repository := range present {
		if _, inRun := owned[repository]; !inRun {
			result.notInRun = append(result.notInRun, repository)
		}
	}
	sort.Strings(result.notInRun)
	var outside []staleKey
	supersededRepositories := map[string]struct{}{}
	for _, key := range live {
		repository := table.ScopeTuple(key)
		if _, inRun := owned[repository]; inRun {
			continue
		}
		if _, held := present[repository]; held {
			result.kept++
			continue
		}
		outside = append(outside, key)
		supersededRepositories[repository] = struct{}{}
	}
	if len(outside) == 0 {
		return result, nil
	}
	sort.Slice(outside, func(left, right int) bool { return outside[left].text() < outside[right].text() })
	newest, err := newestStaleKeyVersion(ctx, conn, table, organizationID, day)
	if err != nil {
		return result, err
	}
	version, err := staleKeyVersionAfter(table, clock, newest)
	if err != nil {
		return result, err
	}
	result.written, err = writeStaleKeyZeroRows(ctx, conn, table, organizationID, day, outside,
		func(staleKey) time.Time { return version })
	if result.written > 0 {
		for repository := range supersededRepositories {
			result.superseded = append(result.superseded, repository)
		}
		sort.Strings(result.superseded)
	}
	return result, err
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
