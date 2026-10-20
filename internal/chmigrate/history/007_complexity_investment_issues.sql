-- 2.1 file_complexity_snapshots
CREATE TABLE IF NOT EXISTS file_complexity_snapshots
(
    repo_id UUID,
    as_of_day Date,
    ref String,
    file_path String,
    language LowCardinality(String),
    loc UInt32,
    functions_count UInt32,
    cyclomatic_total UInt32,
    cyclomatic_avg Float64,
    high_complexity_functions UInt32,
    very_high_complexity_functions UInt32,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(as_of_day)
ORDER BY (repo_id, as_of_day, file_path);

-- 2.2 repo_complexity_daily
CREATE TABLE IF NOT EXISTS repo_complexity_daily
(
    repo_id UUID,
    day Date,
    loc_total UInt64,
    cyclomatic_total UInt64,
    cyclomatic_per_kloc Float64,
    high_complexity_functions UInt64,
    very_high_complexity_functions UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

-- 2.3 file_hotspot_daily
CREATE TABLE IF NOT EXISTS file_hotspot_daily
(
    repo_id UUID,
    day Date,
    file_path String,
    churn_loc_30d UInt64,
    churn_commits_30d UInt32,
    cyclomatic_total UInt32,
    cyclomatic_avg Float64,
    blame_concentration Nullable(Float64),
    risk_score Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, file_path);

-- 3.3 Investment areas
CREATE TABLE IF NOT EXISTS investment_classifications_daily
(
    repo_id Nullable(UUID),
    day Date,
    artifact_type LowCardinality(String),
    artifact_id String,
    provider LowCardinality(String),
    investment_area LowCardinality(String),
    project_stream LowCardinality(String),
    confidence Float64,
    rule_id String,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, provider, artifact_type, investment_area, project_stream, artifact_id);

CREATE TABLE IF NOT EXISTS investment_metrics_daily
(
    repo_id Nullable(UUID),
    day Date,
    team_id LowCardinality(Nullable(String)),
    investment_area LowCardinality(String),
    project_stream LowCardinality(String),
    delivery_units UInt32,
    work_items_completed UInt32,
    prs_merged UInt32,
    churn_loc UInt64,
    cycle_p50_hours Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, team_id, investment_area, project_stream)
SETTINGS allow_nullable_key = 1;

-- 4.2 Issue type metrics
CREATE TABLE IF NOT EXISTS issue_type_metrics_daily
(
    repo_id Nullable(UUID),
    day Date,
    provider LowCardinality(String),
    team_id LowCardinality(String),
    issue_type_norm LowCardinality(String),
    created_count UInt32,
    completed_count UInt32,
    active_count UInt32,
    cycle_p50_hours Float64,
    cycle_p90_hours Float64,
    lead_p50_hours Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, provider, team_id, issue_type_norm);
