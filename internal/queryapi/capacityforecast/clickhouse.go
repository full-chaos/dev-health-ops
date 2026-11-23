package capacityforecast

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/newestrow"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
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
func capacityScopeFilters(orgID string, teamIDs []string, workScopeID *string) ([]string, []clickhouse.Binding) {
	// No team or a team set: each item once in the organization (CHAOS-8498 sums a team set, CHAOS-8863 views), legacy
	// rows and the org view. One team reads the team view instead (CHAOS-14376): every view stays in the filter and
	// capacitySource picks one per key.
	conditions := []string{"org_id = {org_id:String}"}
	if len(teamIDs) != 1 {
		conditions = append(conditions, rowview.OrgPredicate("work_item_metrics_daily", ""))
	}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	// Falsy checks, matching capacity_queries.py: an empty string is "unscoped",
	// not "scoped to the empty id".
	// CHAOS-7964: every selected team counts, not the first. One team keeps
	// the single-bind text it always had; several use one typed Array(String)
	// bind under IN, never a string built from the ids. The table holds one
	// pre-aggregated row per (org, provider, day, work_scope, team) -- there is
	// no item key -- so SUM over the IN set adds each team's row once and an
	// item cannot be counted twice.
	switch len(teamIDs) {
	case 0:
	case 1:
		conditions = append(conditions, "team_id = {team_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: teamIDs[0]})
	default:
		conditions = append(conditions, "team_id IN {team_ids:Array(String)}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_ids", Value: teamIDs})
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

// capacityOrgNewest is the newest version of each full key (the sorting key, row_view included) of the organization by
// rowview.WorkItemMetricsNewestOrder, aliased as the table. It is scoped by org_id in this text, as the served team view
// is; the caller binds org_id. where names key columns only, so filtering before the pick is filtering after it. The
// live-row rule is not in it: a reader applies it to the picked row.
func capacityOrgNewest(where string) string {
	declared, ok := teamkeytables.ByTable("work_item_metrics_daily")
	if !ok {
		panic("capacityforecast: work_item_metrics_daily is not declared in teamkeytables")
	}
	key := []string{"work_item_metrics_daily.org_id", "work_item_metrics_daily." + declared.DayColumn}
	for _, column := range declared.Keys {
		key = append(key, "work_item_metrics_daily."+column.Name)
	}
	return fmt.Sprintf("(SELECT * FROM work_item_metrics_daily WHERE work_item_metrics_daily.org_id = {org_id:String} AND (%s) ORDER BY %s DESC LIMIT 1 BY %s) AS work_item_metrics_daily",
		where, rowview.WorkItemMetricsNewestOrder, strings.Join(key, ", "))
}

// capacitySource is the FROM source of a capacity read of work_item_metrics_daily. No team or a team set reads the
// newest version of each key (capacityOrgNewest; the org view is in the where). One team reads the team view of the
// organization (CHAOS-14376): of the newest row of each view of a key, a live row first, then team, org, legacy
// (rowview.ServedTeamSource); a newest-day read takes the pick with no live-row rule after it (rowview.ServedTeamPick),
// so a key whose every view is retracted stays that key's newest day, as in the org form. Both forms take each key's
// version by rowview.WorkItemMetricsNewestOrder, the order of the throughput forecast and operating review reads, so
// they serve one version of a computed_at tie (CHAOS-14406). The source is aliased as the table.
func capacitySource(teamIDs []string, where string, newestDay bool) string {
	switch {
	case len(teamIDs) != 1:
		return capacityOrgNewest(where)
	case newestDay:
		return rowview.ServedTeamPick("work_item_metrics_daily", where, rowview.WorkItemMetricsNewestOrder) + " AS work_item_metrics_daily"
	default:
		return rowview.ServedTeamSource("work_item_metrics_daily", where, rowview.WorkItemMetricsNewestOrder) + " AS work_item_metrics_daily"
	}
}

// loadThroughput ports load_throughput_from_sink.
//
// The window start is derived from the CALLER's clock rather than ClickHouse's,
// matching Python's client-side utc_today(). It therefore moves at UTC midnight
// -- reproduced rather than pinned, because pinning it in Go alone would be the
// divergence.
//
// One difference from Python: a newest row that holds no measure is a
// retraction row (package liverow) and is left out. It adds 0 to the sum of a
// day, but the result is a LIST of days and the forecast counts them: a day
// whose rows in scope are all retraction rows (the keys of a retired team id)
// is a day with no data, not a day on which 0 items were completed.
func loadThroughput(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string, historyDays int, today time.Time,
) ([]int, error) {
	conditions, bindings := capacityScopeFilters(orgID, teamIDs, workScopeID)
	start := today.UTC().AddDate(0, 0, -historyDays).Format("2006-01-02")
	// Python interpolates this date into the SQL rather than binding it
	// (capacity_queries.py:26). The value is a formatted date built here and
	// carries no injection surface, and binding it instead would be a
	// difference in the query TEXT -- which is the artefact a live-oracle
	// comparison diffs. The native worker executor reproduces it the same way
	// and for the same reason.
	conditions = append([]string{fmt.Sprintf("day >= '%s'", start)}, conditions...)

	// The newest version of each key here (capacitySource), and NOT on the
	// capacityForecasts read below. That asymmetry is Python's:
	// work_item_metrics_daily is a ReplacingMergeTree whose rows are genuinely
	// superseded by recomputes, so reading every version would read stale
	// throughput.
	where := strings.Join(conditions, " AND ")
	query := fmt.Sprintf(`
        SELECT day, SUM(items_completed) AS items_completed
        FROM %s
        WHERE %s
          AND %s
        GROUP BY day
        ORDER BY day
    `, capacitySource(teamIDs, where, false), where, liverow.Predicate("work_item_metrics_daily", ""))

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

// backlogRead is the backlog of a scope and how it was found (CHAOS-7508): the newest day of a key can be long before
// the forecast window, and a sum read from it is not a current backlog.
type backlogRead struct {
	size  int
	state model.NewestDayState
	// asOfDay is the oldest newest day of the measured keys; nil for NO_ROWS.
	asOfDay *time.Time
}

// loadBacklog ports get_backlog_from_sink, including its self-joined max(day), and says how fresh the sum is. A
// measured key whose newest day is before the window start (the start of the throughput read, today minus
// historyDays days) makes the read STALE; the sum keeps that key's value. A key whose newest row is a retraction adds
// 0 and is not a measured key, so a scope of such keys only is NO_ROWS, not a measured zero.
func loadBacklog(
	ctx context.Context, client QueryClient, orgID string, teamIDs []string, workScopeID *string,
	historyDays int, today time.Time,
) (backlogRead, error) {
	conditions, bindings := capacityScopeFilters(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")
	bindings = append(bindings, clickhouse.Binding{
		Name: "start_date", Value: today.UTC().AddDate(0, 0, -historyDays).Format("2006-01-02"),
	})

	// Each selected team contributes its OWN newest row (CHAOS-8498). One
	// maximum day over the whole set dropped any team whose newest day was
	// older than another team's. The scope predicate appears twice on purpose:
	// the outer filter selects the scope's rows, and the subquery finds each
	// team's newest day within that scope.
	source := capacitySource(teamIDs, where, true)
	// The rule reads the picked row's own columns (qualified), never the sum below.
	live := liverow.Predicate("work_item_metrics_daily", "")
	query := fmt.Sprintf(`
        SELECT
            sum(wip_count_end_of_day) AS backlog,
            countIf(%s) AS measured_keys,
            countIf(%s AND day < {start_date:Date}) AS stale_keys,
            minIf(day, %s) AS oldest_day
        FROM %s
        WHERE %s
          AND %s
    `, live, live, live, source, where, newestrow.PerTeamPredicate(source, where))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return backlogRead{}, fmt.Errorf("capacityForecast: backlog query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// An aggregate with no GROUP BY returns exactly one row -- zero when
	// nothing matches -- which is why Python's `int(... or 0)` never has an
	// empty case to handle either.
	var backlog, measuredKeys, staleKeys uint64
	var oldestDay time.Time
	if rows.Next() {
		if scanErr := rows.Scan(&backlog, &measuredKeys, &staleKeys, &oldestDay); scanErr != nil {
			return backlogRead{}, fmt.Errorf("capacityForecast: backlog scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return backlogRead{}, fmt.Errorf("capacityForecast: backlog rows: %w", err)
	}
	size, err := countFromAggregate("wip_count_end_of_day", backlog)
	if err != nil {
		return backlogRead{}, err
	}
	read := backlogRead{size: size, state: model.NewestDayStateNoRows}
	if measuredKeys == 0 {
		return read, nil
	}
	read.state = model.NewestDayStateMeasured
	if staleKeys > 0 {
		read.state = model.NewestDayStateStale
	}
	day := oldestDay.UTC()
	read.asOfDay = &day
	return read, nil
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
		daysValues                        []uint16
		daysCounts, itemsValues           []uint32
		itemsCounts                       []uint32
	)

	if err := rows.Scan(
		&forecastID, &computedAt, &teamID, &workScopeID, &backlogSize,
		&targetItems, &targetDate, &p50Date, &p85Date, &p95Date,
		&p50Days, &p85Days, &p95Days, &p50Items, &p85Items, &p95Items,
		&throughputMean, &throughputStddev, &historyDays,
		&insufficientHistory, &highVariance,
		&daysValues, &daysCounts, &itemsValues, &itemsCounts,
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
		// CHAOS-7624, migration 101. Empty arrays (every row written before it)
		// are a null distribution, not zeros.
		CompletionDistribution: distributionToModel(
			histogramFromUint16(daysValues, daysCounts),
			histogramFromUint32(itemsValues, itemsCounts),
		),
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
