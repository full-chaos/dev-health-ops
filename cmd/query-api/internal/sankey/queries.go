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

// dateBindingValue formats t as a bare "YYYY-MM-DD" string for binding
// into a {name:Date}-typed native ClickHouse parameter -- REQUIRED, not
// cosmetic: dev-health-go's clickHouseParameter formats every time.Time
// value with a full DateTime literal regardless of the placeholder's
// declared type, so a {start_day:Date}/{end_day:Date} placeholder bound
// to a raw time.Time always fails live with "Value ... cannot be parsed
// as Date ... only 10 of 23 bytes was parsed". Same fix shape as
// analytics.dateBindingValue and every other package carrying
// this class of bug; duplicated here per this binary's own
// "repeat, don't couple" convention for a helper this narrow.
func dateBindingValue(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}

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
        LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(work_unit_investments.repo_id) AND r.org_id = {org_id:String}
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
//
// new_items_count and new_bugs_count are UInt32 per DDL, but sum() over
// any unsigned integer column promotes to UInt64 in ClickHouse -- the
// driver refuses to scan a UInt64 result into *float64. Python's
// _build_expense_flow (services/sankey.py) wraps new_bugs in float(...)
// before use, so the wire contract for these two sums is a float: CAST
// to Float64 in SQL, not a widened Go scan target, keeps that contract
// with a single float64 path end to end. bug_completed_estimate is
// already a Float64-typed expression (UInt32 * Float64), so it needs no
// cast.
func fetchExpenseCounts(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]expenseCountsRow, error) {
	query := fmt.Sprintf(`
        SELECT
            CAST(sum(new_items_count) AS Float64) AS new_items,
            CAST(sum(new_bugs_count) AS Float64) AS new_bugs,
            sum(items_completed * bug_completed_ratio) AS bug_completed_estimate
        FROM work_item_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
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
//
// canceled_items is countIf(...), a UInt64-returning aggregate over a
// String-typed predicate column -- ClickHouse's aggregate result type,
// not the source column's type, is what the driver scans. Python's own
// _build_expense_flow (services/sankey.py) wraps this value in float(...)
// before using it: the wire contract for this field is a float, matching
// the other three counters this file sums out of work_item_metrics_daily
// and work_item_state_durations_daily. CAST to Float64 in SQL rather than
// widening the Go return type keeps a single float64 request-to-response
// path, same choice this file makes for new_items/new_bugs/items_touched/
// churn.
func fetchExpenseAbandoned(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) (float64, error) {
	query := fmt.Sprintf(`
        SELECT
            CAST(countIf(status = 'canceled') AS Float64) AS canceled_items
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("sankey: fetch expense abandoned: %w", err)
	}
	defer rows.Close()

	var canceled float64
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
//
// sum(items_touched) promotes UInt32 to UInt64, which the driver refuses
// to scan into *float64. _build_state_flow reads this field with
// float(row.get("items_touched") or 0.0): the wire contract is a float,
// so this reader CASTs the sum to Float64 in SQL rather than widening the
// Go scan target.
func fetchStateStatusCounts(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]stateStatusCountRow, error) {
	query := fmt.Sprintf(`
        SELECT
            status,
            CAST(sum(items_touched) AS Float64) AS items_touched
        FROM work_item_state_durations_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
            %s
        GROUP BY status
        ORDER BY items_touched DESC
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
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
//
// NO LEADING WITH: Python's own reader names churn_hi/
// churn_mid as a two-entry `WITH` CTE list. dev-health-go's ClickHouse
// client requires a literal SELECT as the query's first token and
// refuses anything else (clickhouse/client.go's validateReadOnlyStatement),
// confirmed live -- the same restructuring
// analytics/investmentmembershipscope.go's own doc comment documents for
// the six flowMatrix CTEs already ported that way: every named CTE
// becomes a bare `(SELECT ...)` scalar subquery inlined at its use site
// instead of referenced by name, re-evaluated at each use rather than
// shared. churn_hi/churn_mid are each used once, so each is inlined
// exactly once, directly inside the multiIf predicate below.
//
// The main SELECT's sum(metrics.churn) promotes churn (UInt32 per DDL)
// to UInt64, which the driver refuses to scan into *float64.
// _build_hotspot_flow reads this field with float(row.get("churn") or
// 0.0): the wire contract is a float, so this reader CASTs the outer
// sum to Float64 in SQL. The two churn_hi/churn_mid subquery sums feed
// quantileExact() only, never a Go scan destination, so they are left
// as ClickHouse's native UInt64 -- no cast needed there.
func fetchHotspotRows(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]hotspotFlowRow, error) {
	query := fmt.Sprintf(`
        SELECT
            r.repo AS repo,
            if(
                position(metrics.path, '/') > 0,
                arrayElement(splitByChar('/', metrics.path), 1),
                '(root)'
            ) AS directory,
            metrics.path AS file_path,
            multiIf(
                sum(metrics.churn) >= (
                    SELECT quantileExact(0.7)(churn) FROM (
                        SELECT
                            sum(metrics.churn) AS churn
                        FROM file_metrics_daily AS metrics FINAL
                        INNER JOIN repos AS r FINAL ON r.id = metrics.repo_id AND r.org_id = {org_id:String}
                        WHERE metrics.day >= {start_day:Date} AND metrics.day < {end_day:Date}
                            AND metrics.org_id = {org_id:String}
                            %[1]s
                        GROUP BY r.repo, metrics.path
                    )
                ),
                'refactor',
                sum(metrics.churn) >= (
                    SELECT quantileExact(0.3)(churn) FROM (
                        SELECT
                            sum(metrics.churn) AS churn
                        FROM file_metrics_daily AS metrics FINAL
                        INNER JOIN repos AS r FINAL ON r.id = metrics.repo_id AND r.org_id = {org_id:String}
                        WHERE metrics.day >= {start_day:Date} AND metrics.day < {end_day:Date}
                            AND metrics.org_id = {org_id:String}
                            %[1]s
                        GROUP BY r.repo, metrics.path
                    )
                ),
                'fix',
                'feature'
            ) AS change_type,
            CAST(sum(metrics.churn) AS Float64) AS churn
        FROM file_metrics_daily AS metrics FINAL
        INNER JOIN repos AS r FINAL
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
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
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
