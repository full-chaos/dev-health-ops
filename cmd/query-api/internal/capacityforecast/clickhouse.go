package capacityforecast

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// The reads, ported from metrics/capacity_queries.py (the two compute-path
// queries) and from resolvers/capacity.py (the connection SELECT, which lives
// in capacityforecast.go beside the resolver that builds its WHERE clause).
//
// The compute-path queries are the SAME two the native worker executor already
// ports at internal/jobs/metrics/remaining/capacity_native_clickhouse.go. They
// are re-expressed here against this service's read-only QueryClient boundary
// rather than imported, because that executor's copies are methods on a type
// that owns a WRITE-capable driver connection and a schema verifier -- neither
// of which belongs in a query path. The SQL text is what has to match, and it
// does; a shared helper would have to be parameterised over two different
// client interfaces to save six lines.

// newForecastID mirrors str(uuid.uuid4()) (compute_capacity.py:305).
//
// Fresh per forecast on BOTH sides, so it can never be compared against a
// Python response and is excluded from every fixture.
func newForecastID() string {
	return uuid.NewString()
}

// capacityScopeFilters builds the shared WHERE fragments for the two
// compute-path reads.
//
// Kept in one place because Python builds the SAME condition list for the
// throughput and backlog queries, and the backlog query then splices it TWICE
// -- once in its outer WHERE and once inside the max(day) subquery. Two
// separately written copies would be free to drift while both still looked
// right.
func capacityScopeFilters(orgID string, teamID, workScopeID *string) ([]string, []clickhouse.Binding) {
	conditions := []string{"org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	// Falsy checks, matching capacity_queries.py: an empty string is "unscoped",
	// not "scoped to the empty id".
	if teamID != nil && *teamID != "" {
		conditions = append(conditions, "team_id = {team_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *teamID})
	}
	if workScopeID != nil && *workScopeID != "" {
		conditions = append(conditions, "work_scope_id = {work_scope_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "work_scope_id", Value: *workScopeID})
	}
	return conditions, bindings
}

// countFromAggregate narrows a ClickHouse aggregate to int, refusing a value
// that cannot survive the trip.
//
// sum() over a UInt32 column widens to UInt64, so the destination has to be
// uint64 even though the column is 32-bit. Checking rather than converting
// blind means an absurd value aborts the read instead of wrapping into a
// plausible small number that then feeds a forecast.
func countFromAggregate(column string, value uint64) (int, error) {
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("capacityForecast: %s aggregate %d exceeds the representable range", column, value)
	}
	return int(value), nil
}

// loadThroughput ports load_throughput_from_sink.
//
// The window start is derived from the CALLER's clock rather than ClickHouse's,
// matching Python's client-side utc_today(). It therefore moves at UTC midnight
// -- reproduced rather than pinned, because pinning it in Go alone would be the
// divergence.
func loadThroughput(
	ctx context.Context, client QueryClient, orgID string,
	teamID, workScopeID *string, historyDays int, today time.Time,
) ([]int, error) {
	conditions, bindings := capacityScopeFilters(orgID, teamID, workScopeID)
	start := today.UTC().AddDate(0, 0, -historyDays).Format("2006-01-02")
	// Python interpolates this date into the SQL rather than binding it
	// (capacity_queries.py:26). The value is a formatted date built here and
	// carries no injection surface, and binding it instead would be a
	// difference in the query TEXT -- which is the artefact a live-oracle
	// comparison diffs. The native worker executor reproduces it the same way
	// and for the same reason.
	conditions = append([]string{fmt.Sprintf("day >= '%s'", start)}, conditions...)

	// FINAL here, and NOT on the capacityForecasts read below. That asymmetry is
	// Python's: work_item_metrics_daily is a ReplacingMergeTree whose rows are
	// genuinely superseded by recomputes, so dropping FINAL would read stale
	// throughput.
	query := fmt.Sprintf(`
        SELECT day, SUM(items_completed) AS items_completed
        FROM work_item_metrics_daily FINAL
        WHERE %s
        GROUP BY day
        ORDER BY day
    `, strings.Join(conditions, " AND "))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("capacityForecast: throughput query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var history []int
	for rows.Next() {
		var day time.Time
		var completed uint64
		if scanErr := rows.Scan(&day, &completed); scanErr != nil {
			return nil, fmt.Errorf("capacityForecast: throughput scan: %w", scanErr)
		}
		count, convErr := countFromAggregate("items_completed", completed)
		if convErr != nil {
			return nil, convErr
		}
		history = append(history, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("capacityForecast: throughput rows: %w", err)
	}
	return history, nil
}

// loadBacklog ports get_backlog_from_sink, including its self-joined max(day).
func loadBacklog(
	ctx context.Context, client QueryClient, orgID string, teamID, workScopeID *string,
) (int, error) {
	conditions, bindings := capacityScopeFilters(orgID, teamID, workScopeID)
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

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("capacityForecast: backlog query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// An aggregate with no GROUP BY returns exactly one row -- zero when
	// nothing matches -- which is why Python's `int(... or 0)` never has an
	// empty case to handle either.
	var backlog uint64
	if rows.Next() {
		if scanErr := rows.Scan(&backlog); scanErr != nil {
			return 0, fmt.Errorf("capacityForecast: backlog scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("capacityForecast: backlog rows: %w", err)
	}
	return countFromAggregate("wip_count_end_of_day", backlog)
}

// scanForecastRow maps one persisted capacity_forecasts row onto the GraphQL
// type, mirroring _row_to_forecast.
//
// The scan destinations follow migration 023's declared column types exactly --
// UInt16 for the day percentiles, UInt32 for the item ones, Nullable for
// everything the writer may leave unset. Widening them all to int64 would
// compile and then fail at runtime against a real ClickHouse, which is the one
// place a fake row-scanner cannot catch the mistake.
func scanForecastRow(rows clickhouse.RowScanner) (*model.CapacityForecast, error) {
	var (
		forecastID                        string
		computedAt                        time.Time
		teamID, workScopeID               *string
		backlogSize                       uint32
		targetItems                       *uint32
		targetDate, p50Date, p85Date      *time.Time
		p95Date                           *time.Time
		p50Days, p85Days, p95Days         *uint16
		p50Items, p85Items, p95Items      *uint32
		throughputMean, throughputStddev  float64
		historyDays                       uint16
		insufficientHistory, highVariance uint8
	)

	if err := rows.Scan(
		&forecastID, &computedAt, &teamID, &workScopeID, &backlogSize,
		&targetItems, &targetDate, &p50Date, &p85Date, &p95Date,
		&p50Days, &p85Days, &p95Days, &p50Items, &p85Items, &p95Items,
		&throughputMean, &throughputStddev, &historyDays,
		&insufficientHistory, &highVariance,
	); err != nil {
		return nil, fmt.Errorf("capacityForecasts: scan: %w", err)
	}

	return &model.CapacityForecast{
		ForecastID: forecastID,
		// CHAOS-5450 / R55. This is the field the 2026-09-07 parity run
		// caught: clickhouse_connect hands Python a NAIVE datetime for this
		// DateTime64(3, 'UTC') column and resolvers/capacity.py:24 renders
		// it with str(), so Python's wire value carries a space separator
		// and NO offset at all -- while its own singular resolver
		// (capacity.py:50) stringifies a tz-aware value and does emit one.
		// Go renders RFC 3339 with an explicit offset on both paths through
		// the shared helper. Python stays frozen; the difference is the
		// recorded CHAOS-5450 baseline defect.
		ComputedAt:       graphqldate.RFC3339UTC(computedAt),
		TeamID:           teamID,
		WorkScopeID:      workScopeID,
		BacklogSize:      int(backlogSize),
		TargetItems:      intFromUint32(targetItems),
		TargetDate:       dateOrNil(targetDate),
		P50Date:          dateOrNil(p50Date),
		P85Date:          dateOrNil(p85Date),
		P95Date:          dateOrNil(p95Date),
		P50Days:          intFromUint16(p50Days),
		P85Days:          intFromUint16(p85Days),
		P95Days:          intFromUint16(p95Days),
		P50Items:         intFromUint32(p50Items),
		P85Items:         intFromUint32(p85Items),
		P95Items:         intFromUint32(p95Items),
		ThroughputMean:   throughputMean,
		ThroughputStddev: throughputStddev,
		HistoryDays:      int(historyDays),
		// UInt8 columns, not Bool: Python's bool() treats any non-zero as true,
		// so `!= 0` rather than `== 1`. A writer that ever stored 2 would be
		// read as true on both sides.
		InsufficientHistory: insufficientHistory != 0,
		HighVariance:        highVariance != 0,
	}, nil
}

func intFromUint16(value *uint16) *int {
	if value == nil {
		return nil
	}
	converted := int(*value)
	return &converted
}

func intFromUint32(value *uint32) *int {
	if value == nil {
		return nil
	}
	converted := int(*value)
	return &converted
}
