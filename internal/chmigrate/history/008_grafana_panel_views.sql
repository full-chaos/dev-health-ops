CREATE OR REPLACE VIEW v_ic_landscape_points AS
SELECT
    as_of_day,
    map_name,
    team_id,
    identity_id,
    x_raw,
    y_raw,
    x_norm,
    y_norm
FROM ic_landscape_rolling_30d;

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
FROM file_metrics_daily AS metrics
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

CREATE OR REPLACE VIEW v_investment_flow_edges AS
SELECT
    day,
    toNullable(team_id) AS team_id,
    investment_area AS source,
    project_stream AS target,
    delivery_units,
    churn_loc,
    work_items_completed,
    '' AS provider
FROM investment_metrics_daily;
