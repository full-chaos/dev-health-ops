package report

import (
	"fmt"
	"strings"
)

// CHAOS-4246: dedup sources for re-run-safe daily rollup tables, mirroring
// src/dev_health_ops/clickhouse_dedup.py (the Python source of truth this Go
// package must stay consistent with -- update both together).
//
// The weekly-report engine (this package) reads every daily metric table
// listed in metric_registry.json raw, aggregating with a bare avg()/sum()
// (buildChartQuery). Before CHAOS-4246, a re-drive of one of these
// append-only MergeTree tables was rare enough in practice to go unnoticed;
// CHAOS-4246 made it a designed, expected occurrence (metrics.daily_partition
// now legitimately recomputes a day after later-arriving cicd/deploy/incident
// sync data), so a naive aggregate over these tables now double-counts by
// construction, not by accident. dedupFromSource closes that for every table
// this package reads, not just the three CHAOS-4246 widened.

// rerunDedupedDailyTables are ReplacingMergeTree(computed_at) -- FINAL
// collapses a repeated compute generation.
var rerunDedupedDailyTables = map[string]bool{
	"work_item_metrics_daily":      true,
	"work_item_user_metrics_daily": true,
}

// appendOnlyDailyKeys are legacy plain MergeTree daily tables. FINAL cannot
// collapse a repeated compute generation on these; the latest computed_at row
// per natural key must be selected explicitly. Keys mirror
// clickhouse_dedup._APPEND_ONLY_DAILY_KEYS exactly.
var appendOnlyDailyKeys = map[string][]string{
	"repo_metrics_daily":             {"org_id", "repo_id", "day"},
	"user_metrics_daily":             {"org_id", "repo_id", "author_email", "day"},
	"team_metrics_daily":             {"org_id", "team_id", "day"},
	"testops_pipeline_metrics_daily": {"org_id", "repo_id", "day"},
	"testops_test_metrics_daily":     {"org_id", "repo_id", "day"},
	"testops_coverage_metrics_daily": {"org_id", "repo_id", "day"},
	"testops_quality_drag":           {"org_id", "repo_id", "day"},
	"cicd_metrics_daily":             {"org_id", "repo_id", "day"},
	"deploy_metrics_daily":           {"org_id", "repo_id", "day"},
	"incident_metrics_daily":         {"org_id", "repo_id", "day"},
	"testops_release_confidence":     {"org_id", "repo_id", "day"},
	"testops_pipeline_stability":     {"org_id", "repo_id", "day"},
}

// dedupFromSource returns the FROM source for table: table + " FINAL" for a
// ReplacingMergeTree rollup, a latest-generation subquery for a registered
// append-only daily table, or the bare table name otherwise (a table this
// package reads that carries no known re-drive risk, e.g. a single-write
// snapshot table).
func dedupFromSource(table string) string {
	if rerunDedupedDailyTables[table] {
		return table + " FINAL"
	}
	if keys, ok := appendOnlyDailyKeys[table]; ok {
		return fmt.Sprintf(
			"(SELECT * FROM %s ORDER BY computed_at DESC LIMIT 1 BY %s) AS %s",
			table, strings.Join(keys, ", "), table,
		)
	}
	return table
}
