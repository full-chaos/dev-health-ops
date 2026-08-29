-- CHAOS-4459 (codex review round 2): v_file_hotspots_windowed
-- (008_grafana_panel_views.sql) reads file_metrics_daily raw, with no
-- dedup. file_metrics_daily is append-only -- a redrive/recompute writes a
-- second (repo_id, day, path) row with a fresher computed_at, never
-- replacing the first -- so this Grafana-facing view's churn_loc_window
-- silently doubles for any day that was ever recomputed (the exact class
-- CHAOS-4459's own `metrics partition-recompute` verb would trigger).
--
-- 008 already ran in every environment, so this re-issues the SAME
-- CREATE OR REPLACE VIEW under a new migration number rather than editing
-- 008 in place (an already-applied migration file must never change --
-- see ops/AGENTS.md's CHAOS-4043 note). Collapses to the latest
-- generation per (org_id, repo_id, day, path) before aggregating, the
-- same inlined-SQL shape clickhouse_dedup.dedup_from() produces for every
-- Python reader of this table (api/queries/heatmap.py,
-- api/queries/sankey.py, api/queries/aggregated_flame.py).
CREATE OR REPLACE VIEW v_file_hotspots_windowed AS
WITH
    toDate(now()) - toIntervalDay(30) AS start_day,
    toDate(now()) AS end_day
SELECT
    metrics.repo_id AS repo_id,
    metrics.path AS file_path,
    sumIf(metrics.churn, metrics.day >= start_day AND metrics.day < end_day) AS churn_loc_window,
    lookup.cyclomatic_total AS cyclomatic_total,
    lookup.ownership_concentration AS ownership_concentration,
    0 AS incident_count,
    log1p(churn_loc_window) AS churn_signal,
    log1p(coalesce(cyclomatic_total, 0)) AS complexity_signal,
    coalesce(ownership_concentration, 0) AS ownership_signal,
    log1p(incident_count) AS incident_signal,
    (0.5 * churn_signal + 0.3 * complexity_signal + 0.2 * ownership_signal) AS risk_score
FROM (
    SELECT *
    FROM file_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, day, path
) AS metrics
LEFT JOIN (
    SELECT
        repo_id,
        file_path,
        argMax(cyclomatic_total, computed_at) AS cyclomatic_total,
        argMax(blame_concentration, computed_at) AS ownership_concentration
    FROM file_hotspot_daily
    GROUP BY repo_id, file_path
) AS lookup
    ON lookup.repo_id = metrics.repo_id
    AND lookup.file_path = metrics.path
WHERE metrics.day >= start_day AND metrics.day < end_day
GROUP BY
    metrics.repo_id,
    metrics.path,
    lookup.cyclomatic_total,
    lookup.ownership_concentration;
