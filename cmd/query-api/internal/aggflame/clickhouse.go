package aggflame

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

const dateLayout = "2006-01-02"

func formatDay(t time.Time) string {
	return t.Format(dateLayout)
}

// primaryWorkItemTeamAttributionSource is this package's own copy of
// PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE (api/queries/investment.py),
// byte-for-byte -- internal/quadrant already carries an identical
// independent copy (its own package doc comment), the established
// per-file convention this binary uses instead of a shared constant.
const primaryWorkItemTeamAttributionSource = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
)`

// cycleBreakdownRow is one row fetch_cycle_breakdown (api/queries/
// aggregated_flame.py:14-74) returns -- restricted to the two columns
// _build_cycle_breakdown_tree actually reads (status, total_hours); the
// query's own `total_items` output column is never read by any caller
// (confirmed: neither _build_cycle_breakdown_tree nor its milestone-
// fallback remap reads a cycle_breakdown row's total_items), so it is
// scanned and discarded here rather than modelled on this type.
type cycleBreakdownRow struct {
	Status     string
	TotalHours float64
}

// fetchCycleBreakdown ports fetch_cycle_breakdown's SQL (api/queries/
// aggregated_flame.py:14-74) -- the inner argMax(..., computed_at) GROUP
// BY the full ReplacingMergeTree sort key, matching internal/
// operatingreview's fetchStateDurations exactly for this same table (see
// this package's own doc comment).
func fetchCycleBreakdown(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, teamID, provider, workScopeID string) ([]cycleBreakdownRow, error) {
	filter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
	}
	if teamID != "" {
		filter += "\n            AND team_id = {team_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "team_id", Value: teamID})
	}
	if provider != "" {
		filter += "\n            AND provider = {provider:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "provider", Value: provider})
	}
	if workScopeID != "" {
		filter += "\n            AND work_scope_id = {work_scope_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "work_scope_id", Value: workScopeID})
	}

	query := `
        SELECT
            status,
            sum(duration_hours) AS total_hours,
            sum(items_touched) AS total_items
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                status,
                argMax(duration_hours, computed_at) AS duration_hours,
                argMax(items_touched, computed_at) AS items_touched
            FROM work_item_state_durations_daily
            WHERE org_id = {org_id:String}
              AND day >= {start_day:Date}
              AND day < {end_day:Date}` + filter + `
            GROUP BY day, provider, work_scope_id, team_id, status
        )
        GROUP BY status
        ORDER BY total_hours DESC
    `

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_cycle_breakdown query: %w", err)
	}
	defer rows.Close()

	var out []cycleBreakdownRow
	for rows.Next() {
		var status string
		var totalHours float64
		var totalItems uint64
		if err := rows.Scan(&status, &totalHours, &totalItems); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_cycle_breakdown scan: %w", err)
		}
		out = append(out, cycleBreakdownRow{Status: status, TotalHours: totalHours})
	}
	return out, rows.Err()
}

// milestoneRow is one row fetch_cycle_milestones (api/queries/
// aggregated_flame.py:271-314) returns.
type milestoneRow struct {
	Milestone  string
	AvgHours   float64
	TotalItems uint64
}

// fetchCycleMilestones ports fetch_cycle_milestones (api/queries/
// aggregated_flame.py:271-314) -- work_item_cycle_milestones_daily is a
// plain (non-dedup-registered) rollup Python itself reads raw, no FINAL
// and no argMax; this port matches that -- no divergence to declare.
func fetchCycleMilestones(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, teamID, provider, workScopeID string) ([]milestoneRow, error) {
	filter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
	}
	if teamID != "" {
		filter += "\n          AND team_id = {team_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "team_id", Value: teamID})
	}
	if provider != "" {
		filter += "\n          AND provider = {provider:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "provider", Value: provider})
	}
	if workScopeID != "" {
		filter += "\n          AND work_scope_id = {work_scope_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "work_scope_id", Value: workScopeID})
	}

	query := `
        SELECT
            milestone,
            avg(duration_hours) AS avg_hours,
            count(*) AS total_items
        FROM work_item_cycle_milestones_daily
        WHERE org_id = {org_id:String}
          AND day >= {start_day:Date}
          AND day < {end_day:Date}` + filter + `
        GROUP BY milestone
        ORDER BY avg_hours DESC
    `

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_cycle_milestones query: %w", err)
	}
	defer rows.Close()

	var out []milestoneRow
	for rows.Next() {
		var row milestoneRow
		if err := rows.Scan(&row.Milestone, &row.AvgHours, &row.TotalItems); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_cycle_milestones scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// hotspotRow is one row fetch_code_hotspots (api/queries/
// aggregated_flame.py:77-124) returns.
type hotspotRow struct {
	RepoID     string
	FilePath   string
	TotalChurn float64
}

// fetchCodeHotspots ports fetch_code_hotspots (api/queries/
// aggregated_flame.py:77-124) -- see this package's own doc comment for
// why the inner subquery uses argMax(churn, computed_at) GROUP BY
// (repo_id, day, path) rather than Python's dedup_from() LIMIT-1-BY shape.
func fetchCodeHotspots(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, limit, minChurn int) ([]hotspotRow, error) {
	filter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "limit", Value: limit},
		{Name: "min_churn", Value: minChurn},
	}
	if repoID != "" {
		filter = "\n              AND toString(repo_id) = {repo_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "repo_id", Value: repoID})
	}

	query := `
        SELECT
            repo_id,
            path AS file_path,
            sum(churn) AS total_churn
        FROM (
            SELECT
                repo_id,
                day,
                path,
                argMax(churn, computed_at) AS churn
            FROM file_metrics_daily
            WHERE org_id = {org_id:String}
              AND day >= {start_day:Date}
              AND day < {end_day:Date}` + filter + `
            GROUP BY repo_id, day, path
        )
        GROUP BY repo_id, path
        HAVING total_churn >= {min_churn:UInt64}
        ORDER BY total_churn DESC
        LIMIT {limit:UInt64}
    `

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_code_hotspots query: %w", err)
	}
	defer rows.Close()

	var out []hotspotRow
	for rows.Next() {
		var repoID string
		var filePath string
		var totalChurn uint64
		if err := rows.Scan(&repoID, &filePath, &totalChurn); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_code_hotspots scan: %w", err)
		}
		out = append(out, hotspotRow{RepoID: repoID, FilePath: filePath, TotalChurn: float64(totalChurn)})
	}
	return out, rows.Err()
}

// fetchRepoNames ports fetch_repo_names (api/queries/aggregated_flame.py:
// 127-148) -- see this package's own doc comment on why this port reads
// `repos FINAL` (a style choice, not a declared divergence). Returns an
// empty, non-nil map for an empty repoIDs input, matching Python's own
// `if not repo_ids: return {}` short circuit (and also avoiding an
// `IN ()` query ClickHouse would otherwise have to run).
func fetchRepoNames(ctx context.Context, client QueryClient, orgID string, repoIDs []string) (map[string]string, error) {
	out := map[string]string{}
	if len(repoIDs) == 0 {
		return out, nil
	}

	query := `
        SELECT
            toString(id) AS repo_id,
            repo AS repo_name
        FROM repos FINAL
        WHERE toString(id) IN {repo_ids:Array(String)}
          AND org_id = {org_id:String}
    `
	bindings := []dhclickhouse.Binding{
		{Name: "repo_ids", Value: repoIDs},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_repo_names query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var repoID, repoName string
		if err := rows.Scan(&repoID, &repoName); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_repo_names scan: %w", err)
		}
		out[repoID] = repoName
	}
	return out, rows.Err()
}

// throughputRow is one row fetch_throughput/fetch_throughput_by_type
// (api/queries/aggregated_flame.py:151-268) returns -- restricted to the
// columns buildThroughputTree actually reads; see this package's own doc
// comment for why WorkType defaults handle both readers' shapes.
type throughputRow struct {
	WorkType       string
	TeamName       string
	ItemsCompleted float64
}

// fetchThroughput ports fetch_throughput (api/queries/aggregated_flame.py:
// 151-213) -- provider/work_scope_id/repo_id parameters dropped, see this
// package's own doc comment on the confirmed Python dead-parameter quirks
// this port does not reproduce as query filters (repo_id/provider/
// work_scope_id are never forwarded here by the only caller,
// build_aggregated_flame_response's throughput branch).
func fetchThroughput(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, teamID string, limit int) ([]throughputRow, error) {
	teamFilter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "limit", Value: limit},
	}
	if teamID != "" {
		teamFilter = "\n          AND t.team_id = {team_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "team_id", Value: teamID})
	}

	query := `
        SELECT
            'All' AS work_type,
            if(
                coalesce(nullIf(t.team_id, ''), 'unassigned') = 'unassigned',
                'Unassigned',
                coalesce(nullIf(any(t.team_name), ''), coalesce(nullIf(t.team_id, ''), 'unassigned'))
            ) AS team_name,
            uniqExact(wct.work_item_id) AS items_completed,
            0 AS items_started
        FROM work_item_cycle_times AS wct FINAL
        LEFT JOIN ` + primaryWorkItemTeamAttributionSource + ` AS t
          ON t.work_item_id = wct.work_item_id
        WHERE wct.day >= {start_day:Date}
          AND wct.day < {end_day:Date}
          AND wct.org_id = {org_id:String}` + teamFilter + `
        GROUP BY coalesce(nullIf(t.team_id, ''), 'unassigned')
        HAVING items_completed > 0
        ORDER BY items_completed DESC
        LIMIT {limit:UInt64}
    `

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_throughput query: %w", err)
	}
	defer rows.Close()

	var out []throughputRow
	for rows.Next() {
		var workType, teamName string
		var itemsCompleted uint64
		var itemsStarted uint64
		if err := rows.Scan(&workType, &teamName, &itemsCompleted, &itemsStarted); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_throughput scan: %w", err)
		}
		out = append(out, throughputRow{WorkType: workType, TeamName: teamName, ItemsCompleted: float64(itemsCompleted)})
	}
	return out, rows.Err()
}

// fetchThroughputByType ports fetch_throughput_by_type (api/queries/
// aggregated_flame.py:216-268) -- repo_id parameter dropped, see this
// package's own doc comment (fetch_throughput_by_type's own WHERE clause
// never references it either).
func fetchThroughputByType(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, teamID string, limit int) ([]throughputRow, error) {
	teamFilter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "limit", Value: limit},
	}
	if teamID != "" {
		teamFilter = "\n          AND t.team_id = {team_id:String}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "team_id", Value: teamID})
	}

	query := `
        SELECT
            coalesce(nullIf(wct.type, ''), 'unclassified') AS work_type,
            if(
                coalesce(nullIf(t.team_id, ''), 'unassigned') = 'unassigned',
                'Unassigned',
                coalesce(nullIf(any(t.team_name), ''), coalesce(nullIf(t.team_id, ''), 'unassigned'))
            ) AS team_name,
            uniqExact(wct.work_item_id) AS items_completed
        FROM work_item_cycle_times AS wct FINAL
        LEFT JOIN ` + primaryWorkItemTeamAttributionSource + ` AS t
          ON t.work_item_id = wct.work_item_id
        WHERE wct.completed_at >= toDateTime({start_day:Date})
          AND wct.completed_at < toDateTime({end_day:Date})
          AND wct.completed_at IS NOT NULL
          AND wct.org_id = {org_id:String}` + teamFilter + `
        GROUP BY work_type, coalesce(nullIf(t.team_id, ''), 'unassigned')
        HAVING items_completed > 0
        ORDER BY items_completed DESC
        LIMIT {limit:UInt64}
    `

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("aggflame: fetch_throughput_by_type query: %w", err)
	}
	defer rows.Close()

	var out []throughputRow
	for rows.Next() {
		var workType, teamName string
		var itemsCompleted uint64
		if err := rows.Scan(&workType, &teamName, &itemsCompleted); err != nil {
			return nil, fmt.Errorf("aggflame: fetch_throughput_by_type scan: %w", err)
		}
		out = append(out, throughputRow{WorkType: workType, TeamName: teamName, ItemsCompleted: float64(itemsCompleted)})
	}
	return out, rows.Err()
}
