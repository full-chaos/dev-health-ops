-- Migration 040: compounding_risk_daily — append-only composite risk score.
--
-- CHAOS-1641 (Define + compute Compounding Risk composite)
--
-- Combines four normalized signals from existing daily tables:
--   churn       (rework_churn_ratio_30d)
--   complexity  (cyclomatic_per_kloc delta over the window)
--   ownership   (max of single_owner_file_ratio_30d, code_ownership_gini)
--   review      (review-latency p90 hours (pr_first_review_p90_hours), normalized against REVIEW_REF)
--
-- One row per (org_id, day, scope, scope_id) per computed_at. Append-only,
-- read latest with `argMax(..., computed_at)`.
--
-- Inspectability: raw inputs, normalized components, weights actually used,
-- and the severity bucket all persist alongside the composite score so
-- historical rows remain auditable even if defaults change.

CREATE TABLE IF NOT EXISTS compounding_risk_daily
(
    org_id              String,
    day                 Date,
    scope               Enum8('repo' = 1, 'team' = 2),
    scope_id            String,

    -- composite
    compounding_risk    Nullable(Float64),                -- 0..1, NULL if any required input missing
    severity            Enum8('unknown' = 0, 'low' = 1, 'elevated' = 2, 'high' = 3),

    -- normalized components (0..1 each, NULL when underlying input missing)
    churn_norm          Nullable(Float64),
    complexity_norm     Nullable(Float64),
    ownership_norm      Nullable(Float64),
    review_norm         Nullable(Float64),

    -- raw inputs (for inspectability)
    rework_churn        Nullable(Float64),
    complexity_delta    Nullable(Float64),
    bus_factor          Nullable(Float64),
    ownership_gini      Nullable(Float64),
    single_owner_ratio  Nullable(Float64),
    review_latency_p90h Nullable(Float64),

    -- weights used (audit trail)
    w_churn             Float64,
    w_complexity        Float64,
    w_ownership         Float64,
    w_review            Float64,

    -- severity thresholds used (audit trail — historical rows stay bucketed
    -- under the thresholds in force at compute time)
    threshold_elevated  Float64,
    threshold_high      Float64,

    computed_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, scope, scope_id, day, computed_at);
