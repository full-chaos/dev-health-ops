-- IC Landscape rolling 30d metrics
CREATE TABLE IF NOT EXISTS ic_landscape_rolling_30d (
    repo_id UUID,
    as_of_day Date,
    identity_id String,
    team_id LowCardinality(String),
    
    map_name LowCardinality(String), -- 'churn_throughput' | 'cycle_throughput' | 'wip_throughput'
    
    x_raw Float64,
    y_raw Float64,
    x_norm Float64,
    y_norm Float64,
    
    churn_loc_30d UInt64 DEFAULT 0,
    delivery_units_30d UInt32 DEFAULT 0,
    cycle_p50_30d_hours Float64 DEFAULT 0,
    wip_max_30d UInt32 DEFAULT 0,
    
    computed_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(as_of_day)
ORDER BY (repo_id, team_id, map_name, as_of_day, identity_id);

-- Extend user_metrics_daily with new fields
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS identity_id String DEFAULT author_email;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS loc_touched UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS prs_opened UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS work_items_completed UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS work_items_active UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS delivery_units UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS cycle_p50_hours Float64 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS cycle_p90_hours Float64 DEFAULT 0;