// ClickHouse readers -- ports api/queries/sankey.py's five functions,
// api/services/sankey.py's _tables_present/_columns_present schema-drift
// guard, and (for the investment mode) reuses the analytics package's
// already-fixed LATEST_WORK_UNIT_INVESTMENTS_CTE port instead of a second,
// unsynced copy of its argMax-tuple dedup history -- same reuse
// convention internal/investmentexplain's own reader already follows.
package sankey

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
)

// tablesPresent ports _tables_present (services/sankey.py:125-147): true
// when every named table exists in the current database. A lookup
// failure degrades to false (not present), matching Python's own
// try/except-and-log-warning fallback.
func tablesPresent(ctx context.Context, client QueryClient, tables []string) bool {
	if len(tables) == 0 {
		return true
	}
	query := fmt.Sprintf(`
SELECT name
FROM system.tables
WHERE database = currentDatabase()
  AND name IN {tables:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{{Name: "tables", Value: tables}})
	if err != nil {
		return false
	}
	defer rows.Close()
	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		present[name] = true
	}
	if rows.Err() != nil {
		return false
	}
	for _, t := range tables {
		if !present[t] {
			return false
		}
	}
	return true
}

// columnsPresent ports _columns_present (services/sankey.py:150-177).
func columnsPresent(ctx context.Context, client QueryClient, table string, columns []string) bool {
	if len(columns) == 0 {
		return true
	}
	query := fmt.Sprintf(`
SELECT name
FROM system.columns
WHERE database = currentDatabase()
  AND table = {table:String}
  AND name IN {columns:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "table", Value: table},
		{Name: "columns", Value: columns},
	})
	if err != nil {
		return false
	}
	defer rows.Close()
	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		present[name] = true
	}
	if rows.Err() != nil {
		return false
	}
	for _, c := range columns {
		if !present[c] {
			return false
		}
	}
	return true
}

// investmentFlowItemRow is fetch_investment_flow_items' row shape
// (queries/sankey.py:14-44).
type investmentFlowItemRow struct {
	Source string
	Target string
	Value  float64
}

// fetchInvestmentFlowItems ports fetch_investment_flow_items verbatim,
// reusing analytics.LatestWorkUnitInvestmentsSource() (the already
// argMax-tuple-fixed port of the SAME LATEST_WORK_UNIT_INVESTMENTS_CTE
// Python's own reader chains onto by name) in place of a raw, undeduped
// `FROM work_unit_investments`.
//
// Target: Python wraps its own equivalent select expression in
// `ifNull(r.repo, toString(repo_id))`, but this deployment runs with
// ClickHouse's default join_use_nulls=0 (confirmed elsewhere in this
// codebase, e.g. internal/workgraph/pr.go's own doc comment on the same
// setting): an unmatched LEFT JOIN yields the joined column's ZERO value
// (an empty string for r.repo), never a true SQL NULL, so ifNull's
// fallback branch never actually triggers -- an unmatched row's target is
// always "" either way. This port selects r.repo directly; normalizeLabel
// (response.go) applies the SAME "" -> "Other" fallback
// _normalize_label itself applies downstream, so the observable result is
// unchanged, only the redundant ifNull wrapper is dropped.
//
// repos is joined here the same way scopefilter.go's resolveRepoID reads
// it: FINAL, org_id inside the JOIN's own ON clause. Python's own LEFT
// JOIN neither dedups repos nor scopes it to org_id at all -- a declared
// Python-plane defect (internal/goapiproof/restcorpus.go's
// sankeyRepoDedupParity): an unmerged repos row, or a same-id row from a
// different org (this table has no compound key preventing that), could
// surface as an extra/relabeled target node here. Go is correct.
func fetchInvestmentFlowItems(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]investmentFlowItemRow, error) {
	query := fmt.Sprintf(`
        SELECT
            theme_kv.1 AS source,
            r.repo AS target,
            sum(theme_kv.2 * work_unit_investments.effort_value) AS value
        FROM %s AS work_unit_investments
        LEFT JOIN repos FINAL AS r ON toString(r.id) = toString(work_unit_investments.repo_id) AND r.org_id = {org_id:String}
        ARRAY JOIN CAST(work_unit_investments.theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')} AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
            %s
        GROUP BY source, target
        ORDER BY value DESC
        LIMIT {limit:UInt64}
        %s
    `, analytics.LatestWorkUnitInvestmentsSource(), scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("sankey: fetch investment flow items: %w", err)
	}
	defer rows.Close()

	out := make([]investmentFlowItemRow, 0)
	for rows.Next() {
		var row investmentFlowItemRow
		if err := rows.Scan(&row.Source, &row.Target, &row.Value); err != nil {
			return nil, fmt.Errorf("sankey: scan investment flow item row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sankey: iterate investment flow item rows: %w", err)
	}
	return out, nil
}

// expenseCountsRow is fetch_expense_counts' row shape (queries/sankey.py:
// 47-69).
type expenseCountsRow struct {
	NewBugs              float64
	BugCompletedEstimate float64
}

// fetchExpenseCounts ports fetch_expense_counts verbatim: Python already
// reads work_item_metrics_daily FINAL, no divergence.
func fetchExpenseCounts(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]expenseCountsRow, error) {
	query := fmt.Sprintf(`
        SELECT
            sum(new_items_count) AS new_items,
            sum(new_bugs_count) AS new_bugs,
            sum(items_completed * bug_completed_ratio) AS bug_completed_estimate
        FROM work_item_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: startDay},
		{Name: "end_day", Value: endDay},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("sankey: fetch expense counts: %w", err)
	}
	defer rows.Close()

	out := make([]expenseCountsRow, 0)
	for rows.Next() {
		var newItems float64
		var row expenseCountsRow
		if err := rows.Scan(&newItems, &row.NewBugs, &row.BugCompletedEstimate); err != nil {
			return nil, fmt.Errorf("sankey: scan expense counts row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sankey: iterate expense counts rows: %w", err)
	}
	return out, nil
}

// fetchExpenseAbandoned ports fetch_expense_abandoned. work_item_cycle_
// times is ReplacingMergeTree(computed_at) ordered by (provider,
// work_item_id) since its very first migration; Python's own reader here
// carries no FINAL or other dedup at all -- a declared Python-plane
// defect (sankeyCycleTimesDedupParity): a redrive/recompute leaving an
// unmerged physical version of the same work item can double-count it
// into canceled_items. This port reads FINAL.
func fetchExpenseAbandoned(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) (int64, error) {
	query := fmt.Sprintf(`
        SELECT
            countIf(status = 'canceled') AS canceled_items
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: startDay},
		{Name: "end_day", Value: endDay},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("sankey: fetch expense abandoned: %w", err)
	}
	defer rows.Close()

	var canceled int64
	if rows.Next() {
		if err := rows.Scan(&canceled); err != nil {
			return 0, fmt.Errorf("sankey: scan expense abandoned row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sankey: iterate expense abandoned rows: %w", err)
	}
	return canceled, nil
}

// stateStatusCountRow is fetch_state_status_counts' row shape (queries/
// sankey.py:95-133).
type stateStatusCountRow struct {
	Status       string
	ItemsTouched float64
}

// fetchStateStatusCounts ports fetch_state_status_counts. Python's own
// manual argMax(items_touched, computed_at) dedup, grouped by exactly the
// table's ReplacingMergeTree sorting key (org_id, provider, work_scope_id,
// team_id, status, day), is equivalent to FINAL for this table -- and
// items_touched is a non-nullable UInt32 per DDL, so no null-skip risk --
// this port reads FINAL directly instead, no divergence.
func fetchStateStatusCounts(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]stateStatusCountRow, error) {
	query := fmt.Sprintf(`
        SELECT
            status,
            sum(items_touched) AS items_touched
        FROM work_item_state_durations_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        GROUP BY status
        ORDER BY items_touched DESC
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: startDay},
		{Name: "end_day", Value: endDay},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("sankey: fetch state status counts: %w", err)
	}
	defer rows.Close()

	out := make([]stateStatusCountRow, 0)
	for rows.Next() {
		var row stateStatusCountRow
		if err := rows.Scan(&row.Status, &row.ItemsTouched); err != nil {
			return nil, fmt.Errorf("sankey: scan state status counts row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sankey: iterate state status counts rows: %w", err)
	}
	return out, nil
}

// hotspotFlowRow is fetch_hotspot_rows' row shape (queries/sankey.py:
// 136-208).
type hotspotFlowRow struct {
	Repo       string
	Directory  string
	FilePath   string
	ChangeType string
	Churn      float64
}

// fetchHotspotRows ports fetch_hotspot_rows verbatim. file_metrics_daily
// is ReplacingMergeTree(computed_at); Python already dedups it correctly
// (clickhouse_dedup.dedup_from's ORDER BY computed_at DESC LIMIT 1 BY
// key, functionally equivalent to FINAL for this table since its sorting
// key already IS the reader key) in all three of its own readers here.
// This port's own class ruling is one dedup shape throughout ("never
// LIMIT 1 BY alone"), so every one of those three reads is FINAL here
// instead -- no observable divergence, same reasoning
// cmd/query-api/internal/heatmap/heatmap.go's own doc comment gives for
// its own file_metrics_daily reads. repos is FINAL too, org_id inside the
// JOIN's own ON clause -- Python's own INNER JOIN neither dedups repos nor
// scopes it to org_id, a declared Python-plane defect
// (sankeyRepoDedupParity, same citation as fetchInvestmentFlowItems').
func fetchHotspotRows(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]hotspotFlowRow, error) {
	query := fmt.Sprintf(`
        WITH
            (
                SELECT quantileExact(0.7)(churn) FROM (
                    SELECT
                        sum(metrics.churn) AS churn
                    FROM file_metrics_daily FINAL AS metrics
                    INNER JOIN repos FINAL AS r ON r.id = metrics.repo_id AND r.org_id = {org_id:String}
                    WHERE metrics.day >= {start_day:Date} AND metrics.day < {end_day:Date}
                        AND metrics.org_id = {org_id:String}
                        %[1]s
                    GROUP BY r.repo, metrics.path
                )
            ) AS churn_hi,
            (
                SELECT quantileExact(0.3)(churn) FROM (
                    SELECT
                        sum(metrics.churn) AS churn
                    FROM file_metrics_daily FINAL AS metrics
                    INNER JOIN repos FINAL AS r ON r.id = metrics.repo_id AND r.org_id = {org_id:String}
                    WHERE metrics.day >= {start_day:Date} AND metrics.day < {end_day:Date}
                        AND metrics.org_id = {org_id:String}
                        %[1]s
                    GROUP BY r.repo, metrics.path
                )
            ) AS churn_mid
        SELECT
            r.repo AS repo,
            if(
                position(metrics.path, '/') > 0,
                arrayElement(splitByChar('/', metrics.path), 1),
                '(root)'
            ) AS directory,
            metrics.path AS file_path,
            multiIf(
                sum(metrics.churn) >= churn_hi,
                'refactor',
                sum(metrics.churn) >= churn_mid,
                'fix',
                'feature'
            ) AS change_type,
            sum(metrics.churn) AS churn
        FROM file_metrics_daily FINAL AS metrics
        INNER JOIN repos FINAL AS r
            ON r.id = metrics.repo_id AND r.org_id = {org_id:String}
        WHERE metrics.day >= {start_day:Date} AND metrics.day < {end_day:Date}
          AND metrics.path != ''
          AND metrics.org_id = {org_id:String}
            %[1]s
        GROUP BY repo, directory, file_path
        ORDER BY churn DESC
        LIMIT {limit:UInt64}
        %[2]s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: startDay},
		{Name: "end_day", Value: endDay},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("sankey: fetch hotspot rows: %w", err)
	}
	defer rows.Close()

	out := make([]hotspotFlowRow, 0)
	for rows.Next() {
		var row hotspotFlowRow
		if err := rows.Scan(&row.Repo, &row.Directory, &row.FilePath, &row.ChangeType, &row.Churn); err != nil {
			return nil, fmt.Errorf("sankey: scan hotspot row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sankey: iterate hotspot rows: %w", err)
	}
	return out, nil
}
