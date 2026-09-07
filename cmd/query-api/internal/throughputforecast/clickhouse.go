package throughputforecast

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the read-only ClickHouse query boundary this package needs --
// the same single-method shape featureflags.QueryClient, reviewedges.QueryClient
// and hotspots.QueryClient declare, redeclared locally per those packages'
// documented convention that each operation package is self-contained.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// The seven reads, ported from resolvers/forecast.py.
//
// NONE of them uses FINAL. Every one deduplicates with
// argMax(<column>, computed_at) instead, which is Python's choice here and is
// reproduced rather than improved on -- note that this is the OPPOSITE of the
// capacityForecast path next door, which reads work_item_metrics_daily FINAL.
// The two resolvers really do read the same table two different ways, and
// unifying them would be a silent change to one of their answers.
//
// Every read is org-scoped by an `org_id = {org_id:String}` predicate bound
// from the AUTHORIZED org, never from the client-supplied orgId argument.

// scopeFilter builds the shared team/scope predicates.
//
// Kept in one place because Python builds the same list for all seven reads via
// _team_filter plus an inline work_scope_id clause, and three of them then
// splice the result TWICE -- once in an outer WHERE and once inside a
// `max(day)` subquery. Two separately written copies would be free to drift
// while both still looked right.
//
// The team predicate switches shape on cardinality exactly as _team_filter
// does: `=` for one id, `IN` for several, and NOTHING at all for none, which is
// what makes an empty selection mean "org-wide" rather than "no teams".
func scopeFilter(orgID string, teamIDs []string, workScopeID *string) ([]string, []clickhouse.Binding) {
	conditions := []string{"org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}

	switch {
	case len(teamIDs) == 1:
		conditions = append(conditions, "team_id = {team_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: teamIDs[0]})
	case len(teamIDs) > 1:
		conditions = append(conditions, "team_id IN {team_ids:Array(String)}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_ids", Value: teamIDs})
	}

	// `if work_scope_id:` -- a FALSY check, so an explicitly empty string means
	// "unscoped" rather than "scoped to the empty scope id". Treating it as set
	// would filter every row out.
	if workScopeID != nil && *workScopeID != "" {
		conditions = append(conditions, "work_scope_id = {work_scope_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "work_scope_id", Value: *workScopeID})
	}
	return conditions, bindings
}

// startDate ports `utc_today() - timedelta(weeks=history_weeks)`.
//
// Derived from the caller's clock on the CLIENT side, exactly as Python does,
// so the window moves at UTC midnight rather than at ClickHouse's midnight.
// Reproduced rather than pinned: pinning it in Go alone would be the
// divergence.
func startDate(today time.Time, historyWeeks int) string {
	return today.UTC().AddDate(0, 0, -7*historyWeeks).Format("2006-01-02")
}

// countFromAggregate narrows a ClickHouse aggregate to int, refusing a value
// that cannot survive the trip.
//
// sum() over a UInt32 column widens to UInt64, so the destination has to be
// uint64 even though the column is 32-bit. Checking the narrowing rather than
// converting blind means an absurd value aborts the read instead of being
// wrapped into a plausible-looking small number that then feeds a forecast.
func countFromAggregate(column string, value uint64) (int, error) {
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("%s aggregate %d exceeds the representable range", column, value)
	}
	return int(value), nil
}

// loadThroughputHistory ports _load_throughput_history.
//
// Returns the per-day completed counts in day order -- Python builds
// ThroughputHistory from the same rows and every consumer reads only
// items_completed, so the sample's day/team/scope fields are informational
// there and are not carried here.
func loadThroughputHistory(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string, historyWeeks int, today time.Time,
) ([]int, error) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	conditions = append([]string{"day >= {start_date:Date}"}, conditions...)
	bindings = append(bindings, clickhouse.Binding{
		Name: "start_date", Value: startDate(today, historyWeeks),
	})

	query := fmt.Sprintf(`
        SELECT day, sum(items_completed) AS items_completed
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                argMax(items_completed, computed_at) AS items_completed
            FROM work_item_metrics_daily
            WHERE %s
            GROUP BY day, provider, work_scope_id, team_id
        )
        GROUP BY day
        ORDER BY day
    `, strings.Join(conditions, " AND "))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("throughputForecast: throughput history query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var history []int
	for rows.Next() {
		var day time.Time
		var completed uint64
		if scanErr := rows.Scan(&day, &completed); scanErr != nil {
			return nil, fmt.Errorf("throughputForecast: throughput history scan: %w", scanErr)
		}
		count, convErr := countFromAggregate("items_completed", completed)
		if convErr != nil {
			return nil, convErr
		}
		history = append(history, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("throughputForecast: throughput history rows: %w", err)
	}
	return history, nil
}

// loadWorkItemOverlay ports _load_work_item_overlay, returning
// (current_wip, average_wip) in Python's own tuple order.
//
// The triple nesting is not decoration: the innermost level deduplicates each
// (day, provider, scope, team) partition by computed_at, the middle level sums
// those into ONE wip figure per day, and only then does the outer level average
// across days and pick the latest day's value. Collapsing any level changes
// what "average WIP" means -- a two-level form averages partitions, not days.
func loadWorkItemOverlay(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string, historyWeeks int, today time.Time,
) (float64, float64, error) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	conditions = append([]string{"day >= {start_date:Date}"}, conditions...)
	bindings = append(bindings, clickhouse.Binding{
		Name: "start_date", Value: startDate(today, historyWeeks),
	})

	query := fmt.Sprintf(`
        SELECT
            avg(wip_count_end_of_day) AS average_wip,
            argMax(wip_count_end_of_day, day) AS current_wip
        FROM (
            SELECT
                day,
                sum(wip_count_end_of_day) AS wip_count_end_of_day
            FROM (
                SELECT
                    day,
                    provider,
                    work_scope_id,
                    team_id,
                    argMax(wip_count_end_of_day, computed_at) AS wip_count_end_of_day
                FROM work_item_metrics_daily
                WHERE %s
                GROUP BY day, provider, work_scope_id, team_id
            )
            GROUP BY day
        )
    `, strings.Join(conditions, " AND "))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, 0, fmt.Errorf("throughputForecast: work item overlay query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Python reads `rows[0] if rows else {}` and coerces each field with
	// `float(... or 0.0)`, so a result set with no rows is zeros rather than an
	// error. An aggregate with no GROUP BY returns exactly one row, so this
	// branch is defensive on both sides.
	var averageWIP float64
	var currentWIP uint64
	if rows.Next() {
		if scanErr := rows.Scan(&averageWIP, &currentWIP); scanErr != nil {
			return 0, 0, fmt.Errorf("throughputForecast: work item overlay scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("throughputForecast: work item overlay rows: %w", err)
	}

	// avg() over an empty set is NaN in ClickHouse, and Python keeps it: `nan
	// or 0.0` evaluates to nan, because nan is truthy. Kept here too -- the
	// ratio guard downstream (`averageWIP > 0`) is false for NaN, so both sides
	// reach the same 0.0 ratio by the same route.
	return float64(currentWIP), averageWIP, nil
}

// loadStaleWIP ports _load_stale_wip.
//
// Returns nil when BOTH ages are null, which Python distinguishes from a zero:
// staleWip is an optional field on the wire, and a scope whose rows carry no
// age percentiles must render as absent rather than as "0 hours old".
func loadStaleWIP(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string,
) (*float64, *float64, error) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")

	// The same predicate appears twice on purpose: the subquery finds the
	// latest day WITHIN THIS SCOPE, and the outer filter selects that scope's
	// rows on it. Hoisting the subquery out would find the latest day across
	// the whole organization and report nothing for any scope that had not
	// reported that day.
	query := fmt.Sprintf(`
        SELECT
            avg(wip_age_p50_hours) AS p50_age_hours,
            avg(wip_age_p90_hours) AS p90_age_hours
        FROM (
            SELECT
                provider,
                work_scope_id,
                team_id,
                argMax(wip_age_p50_hours, computed_at) AS wip_age_p50_hours,
                argMax(wip_age_p90_hours, computed_at) AS wip_age_p90_hours
            FROM work_item_metrics_daily
            WHERE day = (
                SELECT max(day) FROM work_item_metrics_daily WHERE %s
            )
            AND %s
            GROUP BY provider, work_scope_id, team_id
        )
        WHERE wip_age_p50_hours IS NOT NULL OR wip_age_p90_hours IS NOT NULL
    `, where, where)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, nil, fmt.Errorf("throughputForecast: stale wip query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var p50, p90 *float64
	if rows.Next() {
		if scanErr := rows.Scan(&p50, &p90); scanErr != nil {
			return nil, nil, fmt.Errorf("throughputForecast: stale wip scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("throughputForecast: stale wip rows: %w", err)
	}
	return p50, p90, nil
}

// estimateCoverage is _load_estimate_coverage's result before it becomes a
// model type.
type estimateCoverage struct {
	ratio            *float64
	estimatedCount   int
	unestimatedCount int
	backlogSize      int
}

// loadEstimateCoverage ports _load_estimate_coverage.
//
// INHERITED QUIRK, reproduced rather than fixed: the inner dedup groups by raw
// team_id, while estimate_coverage_metrics_daily's own sorting key is
// `(org_id, day, provider, work_scope_id, ifNull(team_id, ”))` (migration
// 063). team_id is Nullable(String) there, so the ReplacingMergeTree collapses
// a NULL row and an empty-string row into ONE key while this GROUP BY treats
// them as two groups -- a scope that has both would be counted twice here and
// once by the engine.
//
// Python's query has exactly this shape (resolvers/forecast.py:284, byte
// identical), so matching it is the parity contract and diverging would be the
// defect. Recorded here rather than silently carried: if the quirk is ever
// worth fixing it must be fixed on BOTH sides, in its own change, with the
// row-count difference measured first.
func loadEstimateCoverage(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string,
) (*estimateCoverage, error) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
        SELECT
            sum(estimated_count) AS estimated_count,
            sum(unestimated_count) AS unestimated_count,
            sum(backlog_size) AS backlog_size
        FROM (
            SELECT
                provider,
                work_scope_id,
                team_id,
                argMax(estimated_count, computed_at) AS estimated_count,
                argMax(unestimated_count, computed_at) AS unestimated_count,
                argMax(backlog_size, computed_at) AS backlog_size
            FROM estimate_coverage_metrics_daily
            WHERE day = (
                SELECT max(day) FROM estimate_coverage_metrics_daily WHERE %s
            )
            AND %s
            GROUP BY provider, work_scope_id, team_id
        )
    `, where, where)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("throughputForecast: estimate coverage query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var estimated, unestimated, backlog uint64
	seen := false
	if rows.Next() {
		if scanErr := rows.Scan(&estimated, &unestimated, &backlog); scanErr != nil {
			return nil, fmt.Errorf("throughputForecast: estimate coverage scan: %w", scanErr)
		}
		seen = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("throughputForecast: estimate coverage rows: %w", err)
	}
	// Python returns None when backlog_size is None, which is the no-rows case
	// -- `rows[0] if rows else {}` then `.get("backlog_size")` is None. sum()
	// over a non-Nullable column never yields NULL, so on a real ClickHouse
	// this branch is reached only when the driver hands back no row at all.
	if !seen {
		return nil, nil
	}

	estimatedCount, err := countFromAggregate("estimated_count", estimated)
	if err != nil {
		return nil, err
	}
	unestimatedCount, err := countFromAggregate("unestimated_count", unestimated)
	if err != nil {
		return nil, err
	}
	backlogSize, err := countFromAggregate("backlog_size", backlog)
	if err != nil {
		return nil, err
	}

	// `ratio = estimated / backlog if backlog else None` -- a FALSY check, so a
	// backlog of zero yields a null ratio rather than a division by zero. The
	// counts are still reported: "we know the backlog is empty" is different
	// from "we do not know the backlog".
	var ratio *float64
	if backlogSize != 0 {
		value := float64(estimatedCount) / float64(backlogSize)
		ratio = &value
	}
	return &estimateCoverage{
		ratio:            ratio,
		estimatedCount:   estimatedCount,
		unestimatedCount: unestimatedCount,
		backlogSize:      backlogSize,
	}, nil
}

// loadReviewOverlay ports _load_review_overlay.
//
// Org-scoped only: it carries NO team or work-scope predicate, because
// repo_metrics_daily has neither column. Review latency is therefore an
// org-wide signal even on a single-team forecast -- Python's behaviour, and
// visible in the output as a team-scoped forecast whose review overlay does not
// change when the team does.
func loadReviewOverlay(
	ctx context.Context, client QueryClient, orgID string, historyWeeks int, today time.Time,
) (float64, error) {
	query := `
        SELECT avg(pr_first_review_p50_hours) AS review_latency_hours
        FROM (
            SELECT
                repo_id,
                day,
                argMax(pr_first_review_p50_hours, computed_at) AS pr_first_review_p50_hours
            FROM repo_metrics_daily
            WHERE day >= {start_date:Date} AND org_id = {org_id:String}
            GROUP BY repo_id, day
        )
        WHERE pr_first_review_p50_hours IS NOT NULL
    `
	bindings := []clickhouse.Binding{
		{Name: "start_date", Value: startDate(today, historyWeeks)},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("throughputForecast: review overlay query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var latency *float64
	if rows.Next() {
		if scanErr := rows.Scan(&latency); scanErr != nil {
			return 0, fmt.Errorf("throughputForecast: review overlay scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("throughputForecast: review overlay rows: %w", err)
	}
	// `float(... or 0.0)`: a NULL average -- every row filtered out by the
	// IS NOT NULL predicate -- reads as zero latency, which is INACTIVE rather
	// than unknown. Python's behaviour; a port that surfaced "unknown" would
	// change the overlay set.
	if latency == nil {
		return 0, nil
	}
	return *latency, nil
}

// loadBacklog ports _load_backlog.
func loadBacklog(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string,
) (int, error) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
        SELECT sum(wip_count_end_of_day) AS backlog
        FROM (
            SELECT
                team_id,
                work_scope_id,
                provider,
                argMax(wip_count_end_of_day, computed_at) AS wip_count_end_of_day
            FROM work_item_metrics_daily
            WHERE day = (
                SELECT max(day) FROM work_item_metrics_daily WHERE %s
            )
            AND %s
            GROUP BY team_id, work_scope_id, provider
        )
    `, where, where)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("throughputForecast: backlog query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var backlog uint64
	if rows.Next() {
		if scanErr := rows.Scan(&backlog); scanErr != nil {
			return 0, fmt.Errorf("throughputForecast: backlog scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("throughputForecast: backlog rows: %w", err)
	}
	return countFromAggregate("wip_count_end_of_day", backlog)
}

// loadIncidentOverlay ports _load_incident_overlay.
//
// The division is done IN ClickHouse, by the same expression Python sends, so
// the rate is computed once and identically on both sides -- and greatest(...,
// 1) is what stops a same-day window from dividing by zero.
//
// Org-scoped only, like the review overlay: incident_metrics_daily has no team
// column.
func loadIncidentOverlay(
	ctx context.Context, client QueryClient, orgID string, historyWeeks int, today time.Time,
) (float64, error) {
	query := `
        SELECT sum(incidents_count) / greatest(dateDiff('week', {start_date:Date}, today()), 1) AS incident_count
        FROM (
            SELECT
                repo_id,
                day,
                argMax(incidents_count, computed_at) AS incidents_count
            FROM incident_metrics_daily
            WHERE day >= {start_date:Date} AND org_id = {org_id:String}
            GROUP BY repo_id, day
        )
    `
	bindings := []clickhouse.Binding{
		{Name: "start_date", Value: startDate(today, historyWeeks)},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("throughputForecast: incident overlay query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var incidents float64
	if rows.Next() {
		if scanErr := rows.Scan(&incidents); scanErr != nil {
			return 0, fmt.Errorf("throughputForecast: incident overlay scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("throughputForecast: incident overlay rows: %w", err)
	}
	return incidents, nil
}
