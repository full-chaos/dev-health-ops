// ClickHouse readers for the freshness/coverage/source-status panel --
// ports api/queries/freshness.py.
//
// DEDUP (declared Python-plane defects fixed here):
//   - fetchCoverage's two work_item_cycle_times reads: Python's
//     pr_link_query/cycle_query (api/queries/freshness.py:83-115) run
//     count(*)/countIf(...) over the raw table -- an unmerged physical
//     version inflates both counters. work_item_cycle_times is
//     ReplacingMergeTree(computed_at), sorting key (org_id, provider,
//     work_item_id) with day NOT part of the key (a mutable field on the
//     row) -- confirmed against prod. This reader reads it FINAL so day
//     filtering applies to each key's LATEST row, not to every physical
//     version.
//   - fetchSourceStatuses' ci_pipeline_runs branch: Python's raw
//     count()/max(last_synced) (api/queries/freshness.py:149-152) can
//     double-count a re-synced run under the HAVING count() > 0 gate.
//     ci_pipeline_runs is ReplacingMergeTree(last_synced), sorting key
//     (org_id, repo_id, run_id) -- read FINAL here. The repos/work_items
//     branches' raw max(last_synced) reads are ALSO fixed to FINAL for
//     uniformity with this package's own FINAL-everywhere convention
//     (max() itself is dedup-invariant, but a raw scan of an RMT table
//     is never the declared-safe shape -- same precedent as sankey/
//     heatmap's scopefilter.go, which reads repos/user_metrics_daily
//     FINAL regardless of whether a specific aggregate needs it).
package home

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// fetchLastIngestedAt ports fetch_last_ingested_at (api/queries/
// freshness.py:31-51). repo_metrics_daily is read FINAL, matching this
// package's own FINAL-everywhere convention, though maxOrNull
// (computed_at) is itself dedup-invariant.
func fetchLastIngestedAt(ctx context.Context, client QueryClient, orgID string) (*time.Time, error) {
	query := `
        SELECT maxOrNull(computed_at) AS last_ingested_at
        FROM repo_metrics_daily FINAL
        WHERE org_id = {org_id:String}
    `
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_last_ingested_at query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var value *time.Time
	if err := rows.Scan(&value); err != nil {
		return nil, fmt.Errorf("home: fetch_last_ingested_at scan: %w", err)
	}
	return value, rows.Err()
}

// fetchCoverage ports fetch_coverage (api/queries/freshness.py:54-121).
// Each percentage is null when its own denominator has no records. This
// preserves the difference between unavailable coverage and observed 0%.
func fetchCoverage(ctx context.Context, client QueryClient, startDay, endDay time.Time, orgID string) (Coverage, error) {
	orgBinding := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}
	windowBindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}

	totalRepos, err := queryOneFloat(ctx, client, `
        SELECT toFloat64(countDistinct(id)) AS total
        FROM repos FINAL
        WHERE org_id = {org_id:String}
    `, orgBinding)
	if err != nil {
		return Coverage{}, fmt.Errorf("home: fetch_coverage total repos: %w", err)
	}

	covered, err := queryOneFloat(ctx, client, `
        SELECT toFloat64(countDistinct(repo_id)) AS covered
        FROM repo_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
    `, windowBindings)
	if err != nil {
		return Coverage{}, fmt.Errorf("home: fetch_coverage covered repos: %w", err)
	}
	reposCoveredPct := coveragePercent(covered, totalRepos)

	linked, total, err := queryTwoFloats(ctx, client, `
        SELECT
            toFloat64(countIf(work_scope_id != '')) AS linked,
            toFloat64(count(*)) AS total
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
    `, windowBindings)
	if err != nil {
		return Coverage{}, fmt.Errorf("home: fetch_coverage pr link: %w", err)
	}
	prsLinkedPct := coveragePercent(linked, total)

	withCycle, totalCycle, err := queryTwoFloats(ctx, client, `
        SELECT
            toFloat64(countIf(cycle_time_hours IS NOT NULL)) AS with_cycle,
            toFloat64(count(*)) AS total
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
    `, windowBindings)
	if err != nil {
		return Coverage{}, fmt.Errorf("home: fetch_coverage issue cycle: %w", err)
	}
	issuesCyclePct := coveragePercent(withCycle, totalCycle)

	return Coverage{
		ReposCoveredPct:          reposCoveredPct,
		PRsLinkedToIssuesPct:     prsLinkedPct,
		IssuesWithCycleStatesPct: issuesCyclePct,
	}, nil
}

func coveragePercent(numerator, denominator float64) *float64 {
	if denominator == 0 {
		return nil
	}
	percentage := numerator / denominator * 100.0
	return &percentage
}

// fetchScopeDataConfidence reads only the repositories selected by the Home
// request. It uses repoScopeFilter so team scope is always resolved through
// repository ownership, never team membership or a manual mapping.
//
// The joined metrics are restricted to the response window. Therefore a
// covered repository has both an in-window metric row and an ingestion time;
// a scope with no current metrics is visible as low confidence rather than a
// made-up zero percentage.
func fetchScopeDataConfidence(ctx context.Context, client QueryClient, f Filters, startDay, endDay time.Time, orgID string, asOf time.Time) (ScopeDataConfidence, error) {
	scopeFilter, scopeBindings, err := repoScopeFilter(ctx, client, f, orgID, "toString(r.id)", asOf)
	if err != nil {
		return ScopeDataConfidence{}, fmt.Errorf("home: resolve scope data confidence repositories: %w", err)
	}

	query := fmt.Sprintf(`
        SELECT
            toInt64(countDistinct(r.id)) AS total_repos,
            toInt64(countDistinctIf(r.id, metrics.is_covered = 1)) AS covered_repos,
            maxOrNull(if(metrics.is_covered = 1, metrics.last_ingested_at, NULL)) AS last_ingested_at
        FROM repos AS r FINAL
        LEFT JOIN (
            SELECT
                repo_id,
                toUInt8(1) AS is_covered,
                max(computed_at) AS last_ingested_at
            FROM repo_metrics_daily FINAL
            WHERE day >= {start_day:Date} AND day < {end_day:Date}
              AND org_id = {org_id:String}
            GROUP BY repo_id
        ) AS metrics ON metrics.repo_id = r.id
        WHERE r.org_id = {org_id:String}
        %s
    `, scopeFilter)
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}
	bindings = append(bindings, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return ScopeDataConfidence{}, fmt.Errorf("home: fetch scope data confidence query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return ScopeDataConfidence{}, rows.Err()
	}
	var totalRepos, coveredRepos int64
	var lastIngested *time.Time
	if err := rows.Scan(&totalRepos, &coveredRepos, &lastIngested); err != nil {
		return ScopeDataConfidence{}, fmt.Errorf("home: fetch scope data confidence scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return ScopeDataConfidence{}, err
	}
	return buildScopeDataConfidence(totalRepos, coveredRepos, lastIngested), nil
}

func buildScopeDataConfidence(totalRepos, coveredRepos int64, lastIngested *time.Time) ScopeDataConfidence {
	confidence := ScopeDataConfidence{Level: "low", Caveats: []string{}}
	if totalRepos <= 0 {
		confidence.Caveats = append(confidence.Caveats, "The selected scope has no repositories, so coverage could not be computed.")
		return confidence
	}

	coveragePct := float64(coveredRepos) / float64(totalRepos) * 100
	confidence.CoveragePct = &coveragePct
	if lastIngested != nil {
		value := pytime.NaiveDateTime(*lastIngested)
		confidence.LastIngestedAt = &value
	}

	switch {
	case coveragePct >= 75 && lastIngested != nil:
		confidence.Level = "high"
	case coveragePct >= 40 && lastIngested != nil:
		confidence.Level = "medium"
	}
	if coveredRepos == 0 || lastIngested == nil {
		confidence.Caveats = append(confidence.Caveats, "No repository metrics were ingested for the selected scope and window.")
	} else if coveragePct < 60 {
		confidence.Caveats = append(confidence.Caveats, "Repository coverage appears partial for the selected scope and window.")
	}
	return confidence
}

// fetchSourceStatuses ports fetch_source_statuses (api/queries/
// freshness.py:124-162).
func fetchSourceStatuses(ctx context.Context, client QueryClient, startDay time.Time, orgID string) (map[string]string, error) {
	query := `
        SELECT source, max(last_seen_at) AS last_seen_at
        FROM (
            SELECT lower(provider) AS source, max(last_synced) AS last_seen_at
            FROM repos FINAL
            WHERE org_id = {org_id:String}
              AND lower(provider) NOT IN ('', 'unknown', 'synthetic')
            GROUP BY source

            UNION ALL

            SELECT lower(provider) AS source, max(last_synced) AS last_seen_at
            FROM work_items FINAL
            WHERE org_id = {org_id:String}
              AND lower(provider) NOT IN ('', 'unknown', 'synthetic')
            GROUP BY source

            UNION ALL

            SELECT 'ci' AS source, max(last_synced) AS last_seen_at
            FROM ci_pipeline_runs FINAL
            WHERE org_id = {org_id:String}
            HAVING count() > 0
        )
        GROUP BY source
        ORDER BY source
    `
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_source_statuses query: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var source string
		var lastSeenAt *time.Time
		if err := rows.Scan(&source, &lastSeenAt); err != nil {
			return nil, fmt.Errorf("home: fetch_source_statuses scan: %w", err)
		}
		if source == "" {
			continue
		}
		out[source] = sourceStatus(lastSeenAt, startDay)
	}
	return out, rows.Err()
}

// sourceStatus ports _source_status (api/queries/freshness.py:22-28).
func sourceStatus(seenAt *time.Time, startDay time.Time) string {
	if seenAt == nil {
		return "down"
	}
	if seenAt.Before(time.Date(startDay.Year(), startDay.Month(), startDay.Day(), 0, 0, 0, 0, time.UTC)) {
		return "degraded"
	}
	return "ok"
}

// fetchReworkThemeAllocation ports fetch_rework_theme_allocation
// (api/queries/metrics.py:276-336). investment_metrics_daily is plain
// MergeTree, never converted (confirmed against prod system.tables) --
// Python already dedups it by hand with a per-key argMax(...,
// computed_at) subquery matching its natural key; ported verbatim, no
// divergence.
func fetchReworkThemeAllocation(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, workCategorySQL string, workCategoryBindings []dhclickhouse.Binding, orgID string) ([]ReworkThemeAllocation, error) {
	canonicalThemeExpr := canonicalInvestmentThemeSQL("investment_area")
	query := fmt.Sprintf(`
        SELECT
            canonical_theme AS theme,
            toFloat64(sum(work_items_completed)) AS allocation,
            toInt64(sum(prs_merged)) AS prs_merged,
            toInt64(sum(churn_loc)) AS churn_loc
        FROM (
            SELECT
                day,
                repo_id,
                team_id,
                %s AS canonical_theme,
                project_stream,
                argMax(work_items_completed, computed_at) AS work_items_completed,
                argMax(prs_merged, computed_at) AS prs_merged,
                argMax(churn_loc, computed_at) AS churn_loc
            FROM investment_metrics_daily
            WHERE day >= {start_day:Date} AND day < {end_day:Date}
            %s
            %s
              AND org_id = {org_id:String}
            GROUP BY day, repo_id, team_id, canonical_theme, project_stream
            HAVING %s
        )
        WHERE canonical_theme != ''
        GROUP BY canonical_theme
        ORDER BY allocation DESC
    `, canonicalThemeExpr, scopeFilter, workCategorySQL, liverow.NewestPredicate("investment_metrics_daily", ""))

	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, workCategoryBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_rework_theme_allocation query: %w", err)
	}
	defer rows.Close()

	type themeRow struct {
		Theme      string
		Allocation float64
		PRsMerged  int64
		ChurnLOC   int64
	}
	var rawRows []themeRow
	for rows.Next() {
		var r themeRow
		if err := rows.Scan(&r.Theme, &r.Allocation, &r.PRsMerged, &r.ChurnLOC); err != nil {
			return nil, fmt.Errorf("home: fetch_rework_theme_allocation scan: %w", err)
		}
		if units.IsTheme(r.Theme) {
			rawRows = append(rawRows, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	total := 0.0
	for _, r := range rawRows {
		total += r.Allocation
	}
	out := make([]ReworkThemeAllocation, 0, len(rawRows))
	for _, r := range rawRows {
		pct := 0.0
		if total != 0 {
			pct = r.Allocation / total * 100.0
		}
		out = append(out, ReworkThemeAllocation{
			Theme:         r.Theme,
			Label:         investmentThemeLabels[r.Theme],
			Allocation:    r.Allocation,
			AllocationPct: pct,
			PRsMerged:     r.PRsMerged,
			ChurnLOC:      r.ChurnLOC,
		})
	}
	return out, nil
}

func queryOneFloat(ctx context.Context, client QueryClient, query string, bindings []dhclickhouse.Binding) (float64, error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, rows.Err()
	}
	var v float64
	if err := rows.Scan(&v); err != nil {
		return 0, err
	}
	return v, rows.Err()
}

func queryTwoFloats(ctx context.Context, client QueryClient, query string, bindings []dhclickhouse.Binding) (float64, float64, error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, rows.Err()
	}
	var a, b float64
	if err := rows.Scan(&a, &b); err != nil {
		return 0, 0, err
	}
	return a, b, rows.Err()
}
