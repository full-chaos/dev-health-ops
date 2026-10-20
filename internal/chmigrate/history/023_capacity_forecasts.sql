-- Migration: capacity_forecasts table for Monte Carlo capacity planning

CREATE TABLE IF NOT EXISTS capacity_forecasts (
    forecast_id String,
    computed_at DateTime64(3, 'UTC'),
    team_id Nullable(String),
    work_scope_id Nullable(String),
    
    -- Input parameters
    backlog_size UInt32,
    target_items Nullable(UInt32),
    target_date Nullable(Date),
    history_days UInt16,
    simulation_count UInt32,
    
    -- Fixed-scope results (days to complete)
    p50_days Nullable(UInt16),
    p85_days Nullable(UInt16),
    p95_days Nullable(UInt16),
    p50_date Nullable(Date),
    p85_date Nullable(Date),
    p95_date Nullable(Date),
    
    -- Fixed-date results (items completable)
    p50_items Nullable(UInt32),
    p85_items Nullable(UInt32),
    p95_items Nullable(UInt32),
    
    -- Throughput statistics
    throughput_mean Float64,
    throughput_stddev Float64,
    
    -- Quality indicators
    insufficient_history UInt8 DEFAULT 0,
    high_variance UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (forecast_id)
SETTINGS index_granularity = 8192;

-- Index for team/scope queries
ALTER TABLE capacity_forecasts ADD INDEX IF NOT EXISTS idx_team_scope (team_id, work_scope_id) TYPE bloom_filter GRANULARITY 4;
