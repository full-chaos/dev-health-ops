package throughputforecast

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/newestrow"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// measuredWorkItem keeps a (day, provider, scope, team) key only when its
// picked newest row is a measurement (package liverow). It is in the reads that make
// a list of days: a day that holds only retraction rows (the keys of a retired
// team id a caller named) is a day with no data, not a day with a throughput
// or a WIP of 0. The reads that take each key's newest day (backlog, current
// WIP, stale WIP) do not filter on it (the backlog read only counts its
// measured keys): a retraction row must stay the newest row
// of its key there, so that the key gives nothing and not its older backlog,
// and it adds 0 to a sum and no value to a Nullable mean.
var measuredWorkItem = liverow.Predicate("work_item_metrics_daily", "newest")

// The newest version of a key is the one with the newest computed_at, then the highest hash of the version's values, so
// two versions that share a computed_at serve one of them whatever the scan order. Each table has ONE order key, the
// same columns in the same order as the operating review reads of that table, so the pages serve the same version of a
// tied key; the key of work_item_metrics_daily is rowview.WorkItemMetricsNewestOrder, shared by every served reader of
// the table. workItemNewestColumns picks every liverow measure of the table from that one version.
const workItemNewestColumns = `argMax(tuple(
                    work_item_metrics_daily.items_started, work_item_metrics_daily.items_completed,
                    work_item_metrics_daily.items_started_unassigned,
                    work_item_metrics_daily.items_completed_unassigned,
                    work_item_metrics_daily.wip_count_end_of_day,
                    work_item_metrics_daily.wip_unassigned_end_of_day,
                    work_item_metrics_daily.bug_completed_ratio,
                    work_item_metrics_daily.story_points_completed, work_item_metrics_daily.new_bugs_count,
                    work_item_metrics_daily.new_items_count, work_item_metrics_daily.defect_intro_rate,
                    work_item_metrics_daily.wip_congestion_ratio, work_item_metrics_daily.predictability_score,
                    work_item_metrics_daily.cycle_time_p50_hours, work_item_metrics_daily.cycle_time_p90_hours,
                    work_item_metrics_daily.lead_time_p50_hours, work_item_metrics_daily.lead_time_p90_hours,
                    work_item_metrics_daily.wip_age_p50_hours, work_item_metrics_daily.wip_age_p90_hours
                ), ` + rowview.WorkItemMetricsNewestOrder + `) AS picked,
                picked.1 AS items_started,
                picked.2 AS items_completed,
                picked.3 AS items_started_unassigned,
                picked.4 AS items_completed_unassigned,
                picked.5 AS wip_count_end_of_day,
                picked.6 AS wip_unassigned_end_of_day,
                picked.7 AS bug_completed_ratio,
                picked.8 AS story_points_completed,
                picked.9 AS new_bugs_count,
                picked.10 AS new_items_count,
                picked.11 AS defect_intro_rate,
                picked.12 AS wip_congestion_ratio,
                picked.13 AS predictability_score,
                picked.14 AS cycle_time_p50_hours,
                picked.15 AS cycle_time_p90_hours,
                picked.16 AS lead_time_p50_hours,
                picked.17 AS lead_time_p90_hours,
                picked.18 AS wip_age_p50_hours,
                picked.19 AS wip_age_p90_hours`

const repoMetricsNewestOrder = `(repo_metrics_daily.computed_at, cityHash64(tuple(
    repo_metrics_daily.prs_merged, repo_metrics_daily.pr_first_review_p50_hours,
    repo_metrics_daily.single_owner_file_ratio_30d, repo_metrics_daily.code_ownership_gini,
    repo_metrics_daily.bus_factor, repo_metrics_daily.revert_rate, repo_metrics_daily.mttr_hours,
    repo_metrics_daily.retracted)))`

const incidentNewestOrder = `(incident_metrics_daily.computed_at, cityHash64(tuple(
    incident_metrics_daily.incidents_count, incident_metrics_daily.mttr_p50_hours,
    incident_metrics_daily.retracted)))`

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

// workItemScopeFilter is scopeFilter for a read of work_item_metrics_daily: it adds the view predicate, so every
// item counts once in the organization (legacy rows and the org view, CHAOS-8863). estimate_coverage_metrics_daily has
// no view column and keeps scopeFilter.
func workItemScopeFilter(orgID string, teamIDs []string, workScopeID *string) ([]string, []clickhouse.Binding) {
	conditions, bindings := scopeFilter(orgID, teamIDs, workScopeID)
	if len(teamIDs) == 1 {
		// One team reads the team view (CHAOS-14376): every view stays in the filter, workItemSource picks one per key.
		return conditions, bindings
	}
	return append(conditions, rowview.OrgPredicate("work_item_metrics_daily", "")), bindings
}

// workItemSource is the FROM source of a read of work_item_metrics_daily. No team or a team set reads the table (the
// org view is in the where of workItemScopeFilter). One team reads the team view of the organization (CHAOS-14376):
// of the newest row of each view of a key, a live row first, then team, org, legacy (rowview.ServedTeamSource); a read
// of each key's newest day takes the pick with no live-row rule after it (rowview.ServedTeamPick), so a retracted key
// stays its key's newest row there, as in the org form (see measuredWorkItem). Each view of a key takes its version by
// rowview.WorkItemMetricsNewestOrder, the order of the org form, so both forms serve one version of a computed_at tie
// (CHAOS-14405).
// The source is aliased as the table.
func workItemSource(teamIDs []string, where string, newestDay bool) string {
	switch {
	case len(teamIDs) != 1:
		return "work_item_metrics_daily"
	case newestDay:
		return rowview.ServedTeamPick("work_item_metrics_daily", where, rowview.WorkItemMetricsNewestOrder) + " AS work_item_metrics_daily"
	default:
		return rowview.ServedTeamSource("work_item_metrics_daily", where, rowview.WorkItemMetricsNewestOrder) + " AS work_item_metrics_daily"
	}
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
	conditions, bindings := workItemScopeFilter(orgID, teamIDs, workScopeID)
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
                `+workItemNewestColumns+`
            FROM %s
            WHERE %s
            GROUP BY day, provider, work_scope_id, team_id, row_view
        ) AS newest
        WHERE %s
        GROUP BY day
        ORDER BY day
    `, workItemSource(teamIDs, strings.Join(conditions, " AND "), false), strings.Join(conditions, " AND "), measuredWorkItem)

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
	conditions, bindings := workItemScopeFilter(orgID, teamIDs, workScopeID)
	conditions = append([]string{"day >= {start_date:Date}"}, conditions...)
	bindings = append(bindings, clickhouse.Binding{
		Name: "start_date", Value: startDate(today, historyWeeks),
	})

	where := strings.Join(conditions, " AND ")
	newestDaySource := workItemSource(teamIDs, where, true)
	// current_wip is the sum of each team's OWN newest row in the window
	// (CHAOS-8498); one maximum day over the team set dropped a team whose
	// newest day was older. average_wip keeps the per-day sums.
	query := fmt.Sprintf(`
        SELECT average_wip, current_wip
        FROM (
            SELECT avg(wip_count_end_of_day) AS average_wip
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
                        `+workItemNewestColumns+`
                    FROM %s
                    WHERE %s
                    GROUP BY day, provider, work_scope_id, team_id, row_view
                ) AS newest
                WHERE %s
                GROUP BY day
            )
        ) AS average_part
        CROSS JOIN (
            SELECT sum(wip_count_end_of_day) AS current_wip
            FROM (
                SELECT
                    day,
                    provider,
                    work_scope_id,
                    team_id,
                    `+workItemNewestColumns+`
                FROM %s
                WHERE %s
                  AND %s
                GROUP BY day, provider, work_scope_id, team_id, row_view
            )
        ) AS current_part
    `, workItemSource(teamIDs, where, false), where, measuredWorkItem,
		newestDaySource, where, newestrow.PerTeamPredicate(newestDaySource, where))

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
// wip_age_p50_hours and wip_age_p90_hours are Nullable(Float64) on
// work_item_metrics_daily; both are tuple-wrapped so argMax cannot skip
// the latest day's row when either carries NULL.
func loadStaleWIP(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string,
) (*float64, *float64, error) {
	conditions, bindings := workItemScopeFilter(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")
	source := workItemSource(teamIDs, where, true)

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
                `+workItemNewestColumns+`
            FROM %s
            WHERE %s
            AND %s
            GROUP BY provider, work_scope_id, team_id, row_view
        )
        WHERE wip_age_p50_hours IS NOT NULL OR wip_age_p90_hours IS NOT NULL
    `, source, newestrow.PerTeamPredicate(source, where), where)

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
	grainCount       int
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
            sum(backlog_size) AS backlog_size,
            count() AS grains
        FROM (
            SELECT
                provider,
                work_scope_id,
                team_id,
                argMax(
                    tuple(estimate_coverage_metrics_daily.estimated_count, estimate_coverage_metrics_daily.unestimated_count, estimate_coverage_metrics_daily.backlog_size),
                    (estimate_coverage_metrics_daily.computed_at, cityHash64(tuple(
                        estimate_coverage_metrics_daily.estimated_count, estimate_coverage_metrics_daily.unestimated_count, estimate_coverage_metrics_daily.backlog_size)))
                ) AS picked,
                picked.1 AS estimated_count,
                picked.2 AS unestimated_count,
                picked.3 AS backlog_size
            FROM estimate_coverage_metrics_daily
            WHERE %s
            AND %s
            GROUP BY provider, work_scope_id, team_id
        )
    `, newestrow.PerTeamPredicate("estimate_coverage_metrics_daily", where), where)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("throughputForecast: estimate coverage query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var estimated, unestimated, backlog, grains uint64
	seen := false
	if rows.Next() {
		if scanErr := rows.Scan(&estimated, &unestimated, &backlog, &grains); scanErr != nil {
			return nil, fmt.Errorf("throughputForecast: estimate coverage scan: %w", scanErr)
		}
		seen = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("throughputForecast: estimate coverage rows: %w", err)
	}
	// No coverage row for the scope (the driver hands back no row, or the sum
	// came from ZERO grains -- on a real ClickHouse an aggregate over nothing
	// still yields one row of zeros, which is why the grain count is read):
	// the zero object, ALL zero, ratio 0 included, never nil (D4373 "Keep
	// zero", D4376). A scope WITH rows whose backlog_size is 0 keeps its null
	// ratio below.
	if !seen || grains == 0 {
		zero := 0.0
		return &estimateCoverage{ratio: &zero}, nil
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

	grainCount, err := countFromAggregate("grains", grains)
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
		grainCount:       grainCount,
	}, nil
}

// loadReviewOverlay ports _load_review_overlay.
//
// Org-scoped only: it carries NO team or work-scope predicate, because
// repo_metrics_daily has neither column. Review latency is therefore an
// org-wide signal even on a single-team forecast -- Python's behaviour, and
// visible in the output as a team-scoped forecast whose review overlay does not
// change when the team does.
// pr_first_review_p50_hours is Nullable(Float64) on repo_metrics_daily;
// tuple-wrapped so argMax cannot skip the newest row when it carries
// NULL.
func loadReviewOverlay(
	ctx context.Context, client QueryClient, orgID string, historyWeeks int, today time.Time,
) (float64, error) {
	query := `
        SELECT avg(pr_first_review_p50_hours) AS review_latency_hours
        FROM (
            SELECT
                repo_id,
                day,
                argMax(
                    tuple(repo_metrics_daily.pr_first_review_p50_hours, repo_metrics_daily.retracted),
                    ` + repoMetricsNewestOrder + `
                ) AS picked,
                picked.1 AS pr_first_review_p50_hours,
                picked.2 AS retracted
            FROM repo_metrics_daily
            WHERE day >= {start_date:Date} AND org_id = {org_id:String}
            GROUP BY repo_id, day
        ) AS newest
        WHERE pr_first_review_p50_hours IS NOT NULL
          AND ` + liverow.Predicate("repo_metrics_daily", "newest") + `
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

// backlogRead is the backlog of a scope and how it was found (CHAOS-8167): the newest day of a key can be long before
// the forecast window, and a sum read from it is not a current backlog.
type backlogRead struct {
	size  int
	state model.ThroughputBacklogState
	// asOfDay is the oldest newest day of the measured keys; nil for NO_ROWS and SUPPLIED.
	asOfDay *time.Time
}

// loadBacklog ports _load_backlog and says how fresh the sum is. A measured key whose newest day is before the window
// start (the bound of the throughput history and current WIP reads) makes the read STALE; the sum keeps that key's
// value. A key whose newest row is a retraction adds 0 and is not a measured key, so a scope of such keys only is
// NO_ROWS, not a measured zero.
func loadBacklog(
	ctx context.Context, client QueryClient, orgID string,
	teamIDs []string, workScopeID *string, historyWeeks int, today time.Time,
) (backlogRead, error) {
	conditions, bindings := workItemScopeFilter(orgID, teamIDs, workScopeID)
	where := strings.Join(conditions, " AND ")
	source := workItemSource(teamIDs, where, true)
	bindings = append(bindings, clickhouse.Binding{Name: "start_date", Value: startDate(today, historyWeeks)})

	query := fmt.Sprintf(`
        SELECT
            sum(wip_count_end_of_day) AS backlog,
            countIf(measured) AS measured_keys,
            countIf(measured AND newest_day < {start_date:Date}) AS stale_keys,
            minIf(newest_day, measured) AS oldest_day
        FROM (
            SELECT newest_day, wip_count_end_of_day, %[1]s AS measured
            FROM (
                SELECT
                    team_id,
                    work_scope_id,
                    provider,
                    max(day) AS newest_day,
                    `+workItemNewestColumns+`
                FROM %[4]s
                WHERE %[2]s
                AND %[3]s
                GROUP BY team_id, work_scope_id, provider, row_view
            ) AS newest
        )
    `, measuredWorkItem, newestrow.PerTeamPredicate(source, where), where, source)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return backlogRead{}, fmt.Errorf("throughputForecast: backlog query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var backlog, measuredKeys, staleKeys uint64
	var oldestDay time.Time
	if rows.Next() {
		if scanErr := rows.Scan(&backlog, &measuredKeys, &staleKeys, &oldestDay); scanErr != nil {
			return backlogRead{}, fmt.Errorf("throughputForecast: backlog scan: %w", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		return backlogRead{}, fmt.Errorf("throughputForecast: backlog rows: %w", err)
	}
	size, err := countFromAggregate("wip_count_end_of_day", backlog)
	if err != nil {
		return backlogRead{}, err
	}
	read := backlogRead{size: size, state: model.ThroughputBacklogStateNoRows}
	if measuredKeys == 0 {
		return read, nil
	}
	read.state = model.ThroughputBacklogStateMeasured
	if staleKeys > 0 {
		read.state = model.ThroughputBacklogStateStale
	}
	day := oldestDay.UTC()
	read.asOfDay = &day
	return read, nil
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
                argMax(
                    tuple(incident_metrics_daily.incidents_count, incident_metrics_daily.retracted),
                    ` + incidentNewestOrder + `
                ) AS picked,
                picked.1 AS incidents_count,
                picked.2 AS retracted
            FROM incident_metrics_daily
            WHERE day >= {start_date:Date} AND org_id = {org_id:String}
            GROUP BY repo_id, day
        ) AS newest
        WHERE ` + liverow.Predicate("incident_metrics_daily", "newest") + `
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
