package remaining

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// The capacity reads, ported from capacity_queries.py.
//
// All three go through work_item_metrics_daily FINAL. FINAL is Python's choice
// and is reproduced rather than improved on: that table is a ReplacingMergeTree
// and dropping FINAL would read superseded rows, which is the same class of
// silent divergence the DORA ordering contract turned out to be.

// capacityScopeFilters builds the shared WHERE fragments.
//
// Kept in one place because Python builds the SAME condition list for the
// throughput and backlog queries, and the backlog query then splices it TWICE
// -- once in its outer WHERE and once inside the max(day) subquery. Two
// separately-written copies would be free to drift from each other while both
// still looked right.
func capacityScopeFilters(
	organizationID string, target capacityTarget, arguments map[string]any,
) []string {
	conditions := []string{"org_id = {org_id:String}"}
	arguments["org_id"] = organizationID
	if target.TeamID != nil && *target.TeamID != "" {
		conditions = append(conditions, "team_id = {team_id:String}")
		arguments["team_id"] = *target.TeamID
	}
	if target.WorkScopeID != nil && *target.WorkScopeID != "" {
		conditions = append(conditions, "work_scope_id = {work_scope_id:String}")
		arguments["work_scope_id"] = *target.WorkScopeID
	}
	return conditions
}

// loadThroughput ports load_throughput_from_sink.
//
// The window start is derived from the CALLER'S today rather than from
// ClickHouse's clock, matching Python's utc_today() on the client side. It
// therefore moves at UTC midnight -- which is why the parity harness refuses a
// run that crosses one, rather than this code pinning a window production
// never pins.
func (executor *CapacityExecutor) loadThroughput(
	ctx context.Context, organizationID string, target capacityTarget,
	historyDays int, today time.Time,
) (numerical.Throughput, error) {
	arguments := map[string]any{}
	conditions := capacityScopeFilters(organizationID, target, arguments)
	start := numerical.AddDays(today, -historyDays)
	// Python interpolates this date into the SQL rather than binding it. The
	// value is a formatted date and carries no injection surface, and binding
	// it instead would be a difference in the query text that the live-oracle
	// comparison would report.
	conditions = append([]string{
		fmt.Sprintf("day >= '%s'", start.Format("2006-01-02")),
	}, conditions...)

	query := fmt.Sprintf(`
        SELECT day, SUM(items_completed) AS items_completed
        FROM work_item_metrics_daily FINAL
        WHERE %s
        GROUP BY day
        ORDER BY day
    `, strings.Join(conditions, " AND "))

	rows, err := executor.conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return numerical.Throughput{}, fmt.Errorf("load throughput: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var history numerical.Throughput
	for rows.Next() {
		var day time.Time
		var completed uint64
		if err := rows.Scan(&day, &completed); err != nil {
			return numerical.Throughput{}, fmt.Errorf("scan throughput: %w", err)
		}
		count, err := capacityCountFromAggregate("items_completed", completed)
		if err != nil {
			return numerical.Throughput{}, err
		}
		history.DailyThroughputs = append(history.DailyThroughputs, count)
	}
	if err := rows.Err(); err != nil {
		return numerical.Throughput{}, fmt.Errorf("iterate throughput: %w", err)
	}
	// Python's days_of_history counts SAMPLES, and the query returns one row
	// per day, so the two are the same number here.
	history.DaysOfHistory = len(history.DailyThroughputs)
	return history, nil
}

// loadBacklog ports get_backlog_from_sink, including its self-joined max(day).
func (executor *CapacityExecutor) loadBacklog(
	ctx context.Context, organizationID string, target capacityTarget,
) (int, error) {
	arguments := map[string]any{}
	conditions := capacityScopeFilters(organizationID, target, arguments)
	where := strings.Join(conditions, " AND ")

	// The same predicate appears twice on purpose: the outer filter selects the
	// scope's rows, and the subquery finds the latest day WITHIN THAT SCOPE.
	// Hoisting the subquery out would find the latest day across the whole
	// organization and report a backlog of zero for any scope that had not
	// reported that day.
	query := fmt.Sprintf(`
        SELECT sum(wip_count_end_of_day) AS wip_count_end_of_day
        FROM work_item_metrics_daily FINAL
        WHERE %s
          AND day = (
              SELECT max(day)
              FROM work_item_metrics_daily FINAL
              WHERE %s
          )
    `, where, where)

	// sum() over a UInt32 column widens to UInt64, so the destination has to
	// be uint64 rather than the column's own type. An aggregate always returns
	// exactly one row -- zero when nothing matches -- so there is no empty
	// case to handle separately, matching Python's `int(... or 0)`.
	var backlog uint64
	if err := executor.conn.QueryRow(
		ctx, query, namedArguments(arguments)...,
	).Scan(&backlog); err != nil {
		return 0, fmt.Errorf("load backlog: %w", err)
	}
	return capacityCountFromAggregate("wip_count_end_of_day", backlog)
}

// capacityCountFromAggregate converts a ClickHouse aggregate to the int the
// kernel works in, refusing what int cannot represent.
//
// The mirror of narrowCapacityRow, which guards values going OUT. sum() over a
// UInt32 column widens to UInt64, so these arrive as uint64 and the kernel
// works in int; above MaxInt64 that conversion wraps NEGATIVE. A negative count
// is worse than a large one: loadBacklog returning a negative backlog makes the
// scope skip and the partition finish successfully having forecast nothing,
// which is indistinguishable from a genuinely quiet day.
//
// Guarding the write path and leaving this one unguarded was the actual defect
// -- half of a symmetric boundary, which is the shape that survives review
// precisely because the guarded half looks like the whole job.
func capacityCountFromAggregate(field string, value uint64) (int, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%w: aggregate %s = %d does not fit int",
			ErrCapacityValueOutOfRange, field, value)
	}
	return int(value), nil
}

// resolveScopes ports the all_teams branch of run_capacity_forecast.
func (executor *CapacityExecutor) resolveScopes(
	ctx context.Context, organizationID string, scope capacityScope,
) ([]capacityTarget, error) {
	if !scope.AllTeams {
		return []capacityTarget{{TeamID: scope.TeamID, WorkScopeID: scope.WorkScopeID}}, nil
	}

	// discover_team_scopes uses ClickHouse's own today(), not the client's --
	// a different clock from the throughput window's. Reproduced as written.
	query := `
        SELECT DISTINCT team_id, work_scope_id
        FROM work_item_metrics_daily FINAL
        WHERE day >= today() - 30
        AND org_id = {org_id:String}
    `
	rows, err := executor.conn.Query(
		ctx, query, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("discover team scopes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var targets []capacityTarget
	for rows.Next() {
		var teamID, workScopeID string
		if err := rows.Scan(&teamID, &workScopeID); err != nil {
			return nil, fmt.Errorf("scan team scope: %w", err)
		}
		// Kept EXACTLY as discovery returned them, empty strings included.
		//
		// team_id and work_scope_id are LowCardinality(String) in
		// work_item_metrics_daily, so ClickHouse returns "" and never NULL, and
		// Python's `str(row.get(...)) if ... is not None` therefore yields ""
		// too. That "" travels into the forecast and is WRITTEN as "" to
		// capacity_forecasts, whose columns are Nullable(String).
		//
		// Normalising "" to nil here -- which reads like tidying -- would write
		// NULL instead, and NULL is not "" to either ClickHouse or the
		// comparator. The distinction survives because it is real: an explicit
		// scope that omits team_id gives nil and writes NULL, while discovery
		// of an unteamed row gives "" and writes "". Only the FILTER treats the
		// two alike, which is Python's `if team_id:` falsy check.
		teamValue, workScopeValue := teamID, workScopeID
		targets = append(targets, capacityTarget{
			TeamID: &teamValue, WorkScopeID: &workScopeValue,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team scopes: %w", err)
	}
	return targets, nil
}

// writeForecasts appends the forecast rows.
func (executor *CapacityExecutor) writeForecasts(
	ctx context.Context, rows []capacityRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	// Column order and NAMES follow the Python sink's list exactly
	// (sinks/clickhouse/work_graph.py:811-833), org_id last -- there it is
	// appended by the sink from its bound context rather than passed by the
	// caller. A reordering here would not fail: ClickHouse binds by position,
	// so two same-typed columns swapped would write silently crossed values.
	batch, err := executor.conn.PrepareBatch(ctx, `
        INSERT INTO capacity_forecasts (
            forecast_id, computed_at, team_id, work_scope_id,
            backlog_size, target_items, target_date, history_days,
            simulation_count, p50_days, p85_days, p95_days,
            p50_date, p85_date, p95_date, p50_items, p85_items, p95_items,
            throughput_mean, throughput_stddev, insufficient_history,
            high_variance, org_id
        )`)
	if err != nil {
		return 0, fmt.Errorf("prepare capacity batch: %w", err)
	}
	for _, row := range rows {
		// Every narrowing is checked BEFORE the append, so an out-of-range
		// value aborts the batch instead of being written as a wrapped number
		// the row counter would then report as a healthy write.
		narrowed, err := narrowCapacityRow(row)
		if err != nil {
			return 0, err
		}
		if err := batch.Append(
			newForecastID(), row.ComputedAt,
			// Nullable(String) on both: Python writes None for an unscoped
			// forecast, and flattening that to "" would make an org-wide
			// forecast indistinguishable from one scoped to a team whose id is
			// the empty string.
			row.TeamID, row.WorkScopeID,
			narrowed.backlogSize, narrowed.targetItems, row.TargetDate,
			narrowed.historyDays, narrowed.simulationCount,
			narrowed.p50Days, narrowed.p85Days, narrowed.p95Days,
			row.Forecast.P50Date, row.Forecast.P85Date, row.Forecast.P95Date,
			narrowed.p50Items, narrowed.p85Items, narrowed.p95Items,
			row.Forecast.ThroughputMean, row.Forecast.ThroughputStddev,
			boolToUInt8(row.Forecast.InsufficientHistory),
			boolToUInt8(row.Forecast.HighVariance),
			row.OrgID,
		); err != nil {
			return 0, fmt.Errorf("append capacity row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send capacity batch: %w", err)
	}
	return len(rows), nil
}

// newForecastID mirrors str(uuid.uuid4()) (compute_capacity.py:305).
//
// Fresh per forecast on BOTH sides, so it can never be compared and is
// excluded from the parity table. It also means the table's
// ReplacingMergeTree(computed_at) ORDER BY (forecast_id) never actually
// collapses anything: every row carries a unique key, so a replay appends
// rather than replaces.
func newForecastID() string {
	return uuid.NewString()
}

// narrowedCapacityRow holds one row's values already narrowed to the widths
// their columns declare, so the append below cannot reach a raw int.
type narrowedCapacityRow struct {
	backlogSize                  uint32
	targetItems                  *uint32
	historyDays                  uint16
	simulationCount              uint32
	p50Days, p85Days, p95Days    *uint16
	p50Items, p85Items, p95Items *uint32
}

// narrowCapacityRow converts every integer this row writes, naming the first
// field that does not fit.
//
// One function rather than checks inline at the append: the append site binds
// by POSITION, and a guard written there would have to repeat the column order
// a third time. Here each value is named once, next to the width its column
// actually declares.
func narrowCapacityRow(row capacityRow) (narrowedCapacityRow, error) {
	var narrowed narrowedCapacityRow
	var err error

	if narrowed.backlogSize, err = capacityUint32(
		"backlog_size", row.BacklogSize); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.targetItems, err = capacityNullableUint32(
		"target_items", row.TargetItems); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.historyDays, err = capacityUint16(
		"history_days", row.Forecast.HistoryDays); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.simulationCount, err = capacityUint32(
		"simulation_count", row.Forecast.SimulationCount); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p50Days, err = capacityNullableUint16(
		"p50_days", row.Forecast.P50Days); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p85Days, err = capacityNullableUint16(
		"p85_days", row.Forecast.P85Days); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p95Days, err = capacityNullableUint16(
		"p95_days", row.Forecast.P95Days); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p50Items, err = capacityNullableUint32(
		"p50_items", row.Forecast.P50Items); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p85Items, err = capacityNullableUint32(
		"p85_items", row.Forecast.P85Items); err != nil {
		return narrowedCapacityRow{}, err
	}
	if narrowed.p95Items, err = capacityNullableUint32(
		"p95_items", row.Forecast.P95Items); err != nil {
		return narrowedCapacityRow{}, err
	}
	return narrowed, nil
}

func boolToUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

// ErrCapacityValueOutOfRange reports a computed value that does not fit the
// ClickHouse column it is destined for.
//
// FAIL-CLOSED, and deliberately not a clamp. Go narrows silently: uint32(6e9)
// is 1705032704, no panic and no error. The backlog is read into a uint64
// precisely because sum() over a UInt32 column widens to UInt64, and then the
// write narrowed it straight back with no guard -- so two legitimate rows at
// 3e9 wip produced a plausible-looking 1.7e9 forecast, the batch succeeded, and
// the partition counter recorded a healthy write. Saturating at MaxUint32
// instead would keep exactly that property: a wrong number that the telemetry
// then certifies. A value this large is corrupt input or schema drift, and the
// honest response is to refuse the partition and say which field overflowed.
var ErrCapacityValueOutOfRange = errors.New(
	"capacity value does not fit its ClickHouse column")

// capacityUint32 narrows for a UInt32 column, refusing rather than wrapping.
func capacityUint32(field string, value int) (uint32, error) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, fmt.Errorf("%w: %s = %d does not fit UInt32",
			ErrCapacityValueOutOfRange, field, value)
	}
	return uint32(value), nil
}

// capacityUint16 narrows for a UInt16 column, refusing rather than wrapping.
func capacityUint16(field string, value int) (uint16, error) {
	if value < 0 || uint64(value) > math.MaxUint16 {
		return 0, fmt.Errorf("%w: %s = %d does not fit UInt16",
			ErrCapacityValueOutOfRange, field, value)
	}
	return uint16(value), nil
}

// capacityNullableUint32 narrows an optional value for a Nullable(UInt32)
// column. A nil stays nil: absent is not out of range.
func capacityNullableUint32(field string, value *int) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := capacityUint32(field, *value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

// capacityNullableUint16 narrows an optional value for a Nullable(UInt16)
// column.
func capacityNullableUint16(field string, value *int) (*uint16, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := capacityUint16(field, *value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

// ErrCapacitySchemaIncompatible reports a deployed schema this executor cannot
// compute against.
var ErrCapacitySchemaIncompatible = errors.New(
	"deployed capacity schema does not support what this executor reads or writes")

// capacityTableRequirement is what THIS code needs from one table.
//
// Columns are the obvious half. The engine is the half the first version of
// this probe missed: every read below is `FROM work_item_metrics_daily FINAL`,
// and FINAL is not a hint. On a ReplacingMergeTree it collapses superseded
// versions to the newest; on any other engine it is silently a NO-OP. A table
// carrying every required column under a pre-migration plain MergeTree
// therefore passes a column-only probe, and then two versions of one
// (org, day, scope, team) both survive into sum() -- items_completed of 5 and
// 7 reported as 12 rather than 7. That is a successful, well-telemetered,
// WRONG forecast, which is worse than the retry storm this probe was built to
// prevent: the retry storm at least announces itself.
type capacityTableRequirement struct {
	columns []string
	// readWithFINAL marks a table this code reads with FINAL, making the
	// Replacing family a precondition rather than a deployment detail.
	readWithFINAL bool
}

// capacityTableRequirements is every table the executor touches, and what it
// needs from each.
//
// Derived from the queries and the insert in this file rather than from the
// migration: the question at startup is not "is the chain at head" but "can
// THIS code run", and those differ whenever a migration is partially applied or
// a column is renamed ahead of the code that uses it. The engine entry is that
// same principle carried one layer deeper -- an engine this code's queries
// depend on is as much a precondition as a column they name.
var capacityTableRequirements = map[string]capacityTableRequirement{
	"work_item_metrics_daily": {
		columns: []string{
			"day", "org_id", "team_id", "work_scope_id",
			"items_completed", "wip_count_end_of_day",
		},
		readWithFINAL: true,
	},
	"capacity_forecasts": {
		columns: []string{
			"forecast_id", "computed_at", "org_id", "team_id", "work_scope_id",
			"backlog_size", "target_items", "target_date", "history_days",
			"simulation_count", "p50_days", "p85_days", "p95_days",
			"p50_date", "p85_date", "p95_date", "p50_items", "p85_items", "p95_items",
			"throughput_mean", "throughput_stddev", "insufficient_history",
			"high_variance",
		},
		// Written, never read back with FINAL, so this code does not depend on
		// the engine collapsing anything here.
		readWithFINAL: false,
	},
}

// capacityReplacingEngineMarker matches the Replacing family.
//
// Substring rather than equality on purpose: production runs
// ReplicatedReplacingMergeTree, local and test stacks run ReplacingMergeTree,
// and both collapse superseded versions under FINAL. Requiring an exact string
// would refuse a perfectly correct replicated deployment.
const capacityReplacingEngineMarker = "ReplacingMergeTree"

// verifyCapacitySchema refuses a database this executor cannot compute against.
//
// Without it a worker with a reachable but stale database registers the kind,
// claims partitions, fails the query or the insert, and retries -- turning
// migration drift into a retry storm that burns attempts while capacity stays
// unavailable. Refusing at construction turns the same drift into an unclaimed
// backlog plus one loud, reason-labelled signal, which is the shape the rest of
// this cutover already promises.
//
// It also makes the refusal telemetry honest: before this, the inspect_failed
// reason could effectively never fire, because nothing inspected anything.
func verifyCapacitySchema(ctx context.Context, conn driver.Conn) error {
	// Ordered explicitly rather than ranged over the map: a map range would
	// report a different table first on different runs, so the same broken
	// deployment would produce a different refusal message each restart.
	for _, table := range []string{"work_item_metrics_daily", "capacity_forecasts"} {
		requirement := capacityTableRequirements[table]
		present, err := capacityTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("%w: table %s does not exist",
				ErrCapacitySchemaIncompatible, table)
		}
		var missing []string
		for _, column := range requirement.columns {
			if !present[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: %s is missing %s",
				ErrCapacitySchemaIncompatible, table, strings.Join(missing, ", "))
		}
		if !requirement.readWithFINAL {
			continue
		}
		engine, err := capacityTableEngine(ctx, conn, table)
		if err != nil {
			return err
		}
		if !strings.Contains(engine, capacityReplacingEngineMarker) {
			return fmt.Errorf(
				"%w: %s is %s, but this executor reads it with FINAL, which only "+
					"collapses superseded rows on the %s family -- on %s the "+
					"duplicates stay visible and aggregate into the forecast",
				ErrCapacitySchemaIncompatible, table, engine,
				capacityReplacingEngineMarker, engine)
		}
	}
	return nil
}

func capacityTableColumns(
	ctx context.Context, conn driver.Conn, table string,
) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `
        SELECT name FROM system.columns
        WHERE database = currentDatabase() AND table = {table:String}
    `, clickhouse.Named("table", table))
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan %s columns: %w", table, err)
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s columns: %w", table, err)
	}
	return present, nil
}

// capacityTableEngine reports the engine backing one table.
//
// system.tables rather than SHOW CREATE TABLE: the engine column is already the
// bare family name, so this needs no parsing of DDL text that varies between
// server versions.
func capacityTableEngine(
	ctx context.Context, conn driver.Conn, table string,
) (string, error) {
	var engine string
	if err := conn.QueryRow(ctx, `
        SELECT engine FROM system.tables
        WHERE database = currentDatabase() AND name = {table:String}
    `, clickhouse.Named("table", table)).Scan(&engine); err != nil {
		return "", fmt.Errorf("inspect %s engine: %w", table, err)
	}
	return engine, nil
}
