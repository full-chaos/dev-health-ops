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
	"repo_metrics_daily": {"org_id", "repo_id", "day"},
	"user_metrics_daily": {"org_id", "repo_id", "author_email", "day"},
	// CHAOS-4329: repo_id added -- mirrors clickhouse_dedup.py's identical
	// change. Legacy rows all share repo_id='' (migration 080) so they
	// still collapse to one row per (team_id, day); new per-repo rows are
	// kept apart. This package's own bare avg()/sum() aggregate
	// (buildChartQuery) over this dedup source now sums additive counts
	// correctly across repos; an avg()-based series here is a coarser
	// average-of-per-repo-values than the recomputed-from-summed-counts
	// ratio the Python readers use, tracked as a known follow-up rather
	// than blocking this fix (see this PR's RISK-NOTES).
	"team_metrics_daily":             {"org_id", "team_id", "repo_id", "day"},
	"testops_pipeline_metrics_daily": {"org_id", "repo_id", "day"},
	"testops_test_metrics_daily":     {"org_id", "repo_id", "day"},
	"testops_coverage_metrics_daily": {"org_id", "repo_id", "day"},
	"testops_quality_drag":           {"org_id", "repo_id", "day"},
	"cicd_metrics_daily":             {"org_id", "repo_id", "day"},
	"deploy_metrics_daily":           {"org_id", "repo_id", "day"},
	"incident_metrics_daily":         {"org_id", "repo_id", "day"},
	"testops_release_confidence":     {"org_id", "repo_id", "day"},
	"testops_pipeline_stability":     {"org_id", "repo_id", "day"},
	// CHAOS-4140: dora_metrics_daily was missing from this map entirely, so
	// dedupFromSource fell through to the bare table name for the "value"
	// chart metric (metric_registry.json's dora entry) -- the exact
	// unguarded shape TestBuildChartQueryDedupsCicdMetricsDaily exists to
	// catch for the other tables. A DORA partition retry writes a fresh
	// computed_at generation for every (org_id, repo_id, day, metric_name)
	// it recomputes without deleting the prior generation (job_dora.py /
	// dora_native.go, by design -- see CHAOS-4130's
	// preserve-Python's-disposition ruling), so this package's bare
	// avg()/sum() aggregate summed every generation together. Python's
	// mirror (clickhouse_dedup._APPEND_ONLY_DAILY_KEYS) has registered this
	// table since CHAOS-4242; this Go map, created later by CHAOS-4246,
	// never picked up that entry. metric_name is part of the key (not just
	// org_id/repo_id/day) because compute_dora.py's contract is one row per
	// (repo, metric_name, day) -- omitting it would collapse the 4 distinct
	// DORA metrics for one repo/day into a single arbitrary row.
	"dora_metrics_daily": {"org_id", "repo_id", "day", "metric_name"},
	// CHAOS-4459 (codex review, rounds 2-3): registered in
	// clickhouse_dedup.py's _APPEND_ONLY_DAILY_KEYS but missing here --
	// this package's own bare avg()/sum() aggregate over
	// metric_registry.json's file_metrics_daily/file_hotspot_daily entries
	// was reading them raw, exactly the CHAOS-4246 gap this file exists to
	// close. Natural keys mirror the Python registry exactly.
	"file_metrics_daily": {"org_id", "repo_id", "day", "path"},
	"file_hotspot_daily": {"org_id", "repo_id", "day", "file_path"},
	// CHAOS-4459 (codex review round 4): review_edges_daily is a plain
	// MergeTree source_table for metric_registry.json's review-load charts
	// (sum(reviews_count)), and this recompute verb's own doc comment
	// (partition_recompute.go's SupportedPartitionRecomputeFamilies) already
	// admits every family in a partition -- not just repo_user_commit --
	// gets re-executed on a recompute, since there is no per-family publish
	// scoping. review_edges_daily has no org_id column (repo_id is the
	// tenant boundary here); natural key mirrors clickhouse_dedup.py.
	"review_edges_daily": {"repo_id", "reviewer", "author", "day"},
	// CHAOS-4459 (self-audit against families.json's full write-table list,
	// requested by team-lead after codex round 4): commit_metrics -- THIS
	// TICKET'S OWN target table -- is a plain MergeTree source_table for
	// metric_registry.json's commit charts (Commit Hash/Files Changed/Size
	// Bucket/Total LOC) and had no dedup key registered, same gap class as
	// the three tables above. A repeat partition-recompute of the same day
	// (this PR's own integration test exercises exactly that) re-inserts
	// the SAME (repo_id, day, author_email, commit_hash) rows under a fresh
	// computed_at, doubling these charts' sums. Natural key mirrors the
	// table's own ORDER BY (migration 001).
	"commit_metrics": {"repo_id", "day", "author_email", "commit_hash"},
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
