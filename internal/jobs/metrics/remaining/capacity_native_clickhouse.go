package remaining

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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
		history.DailyThroughputs = append(history.DailyThroughputs, int(completed))
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

	var backlog float64
	if err := executor.conn.QueryRow(
		ctx, query, namedArguments(arguments)...,
	).Scan(&backlog); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, fmt.Errorf("load backlog: %w", err)
	}
	return int(backlog), nil
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
		target := capacityTarget{}
		if teamID != "" {
			value := teamID
			target.TeamID = &value
		}
		if workScopeID != "" {
			value := workScopeID
			target.WorkScopeID = &value
		}
		targets = append(targets, target)
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
		if err := batch.Append(
			newForecastID(), row.ComputedAt,
			// Nullable(String) on both: Python writes None for an unscoped
			// forecast, and flattening that to "" would make an org-wide
			// forecast indistinguishable from one scoped to a team whose id is
			// the empty string.
			row.TeamID, row.WorkScopeID,
			uint32(row.BacklogSize), nullableUint32(row.TargetItems), row.TargetDate,
			uint16(row.Forecast.HistoryDays), uint32(row.Forecast.SimulationCount),
			nullableUint16(row.Forecast.P50Days), nullableUint16(row.Forecast.P85Days),
			nullableUint16(row.Forecast.P95Days),
			row.Forecast.P50Date, row.Forecast.P85Date, row.Forecast.P95Date,
			nullableUint32(row.Forecast.P50Items), nullableUint32(row.Forecast.P85Items),
			nullableUint32(row.Forecast.P95Items),
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

func nullableUint16(value *int) *uint16 {
	if value == nil {
		return nil
	}
	converted := uint16(*value)
	return &converted
}

func nullableUint32(value *int) *uint32 {
	if value == nil {
		return nil
	}
	converted := uint32(*value)
	return &converted
}

func boolToUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
