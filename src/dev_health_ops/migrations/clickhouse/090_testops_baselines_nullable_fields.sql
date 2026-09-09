-- CHAOS-4806, ruling R73: a benchmark baseline's current value, its rolling
-- mean, or one of its percentile order statistics can be genuinely undefined
-- (a scope's own latest metric reading is a NaN, the metric window drops to
-- zero valid samples after non-finite entries are filtered, or an order
-- statistic overflows to +-Inf on an extreme-but-finite input) -- R73 bans
-- writing that undefined value as NaN/+-Inf, and separately bans 0.0 (or any
-- other finite sentinel) standing in for it. Only a real ClickHouse NULL
-- says "undefined" without lying about the data. These six columns were
-- plain (non-Nullable) Float64, so there was no NULL to write for a single
-- undefined field without refusing the whole row -- widen them to
-- Nullable(Float64). percentile_rank is NOT included: it is a bounded
-- ratio-of-counts (0-100) that cannot itself become non-finite even when fed
-- a non-finite comparison value (see PercentileRank's own doc comment),
-- so it stays required.
ALTER TABLE testops_metric_baselines MODIFY COLUMN current_value Nullable(Float64);
ALTER TABLE testops_metric_baselines MODIFY COLUMN baseline_value Nullable(Float64);
ALTER TABLE testops_metric_baselines MODIFY COLUMN p25_value Nullable(Float64);
ALTER TABLE testops_metric_baselines MODIFY COLUMN p50_value Nullable(Float64);
ALTER TABLE testops_metric_baselines MODIFY COLUMN p75_value Nullable(Float64);
ALTER TABLE testops_metric_baselines MODIFY COLUMN p90_value Nullable(Float64);

-- testops_maturity_bands.value is populated directly from a baseline row's
-- current_value (ClassifyMaturityBands) -- the same undefined-current-value
-- case above flows straight through, so this column widens for the same
-- reason.
ALTER TABLE testops_maturity_bands MODIFY COLUMN value Nullable(Float64);
