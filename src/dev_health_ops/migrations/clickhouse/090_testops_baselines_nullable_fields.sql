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

-- CHAOS-4806, codex round chaos-4806-r1 confirmation-pass weakest point
-- (team-lead, verbatim): argMax(<col>, computed_at) SKIPS NULL values
-- (CHAOS-4547 trap family) -- any FUTURE reader that dedups one of these
-- six columns with plain argMax(...) will silently pick an OLDER non-null
-- row instead of the latest (correctly) NULL one, resurrecting a stale
-- number instead of reporting "undefined." Swept the whole repo
-- (`rg --hidden -n 'FROM testops_metric_baselines|FROM testops_maturity_bands'`)
-- at the time of this migration: NO production reader exists for either
-- table yet (write-only from internal/jobs/metrics/daily/benchmarking's
-- Writer -- only test SELECTs and a fixture manifest reference them). The
-- FIRST reader built against these tables (Go or Python, GraphQL resolver
-- or CLI) MUST dedup with argMaxIf/a FINAL-with-tuple-ordering pattern
-- (never a bare argMax on one of these six columns) and should pin it with
-- a fixture row shaped exactly like this: latest computed_at row has the
-- column NULL, an OLDER row has a real value (e.g. 5.0) -- a correct
-- reader returns NULL, a reader using plain argMax silently returns 5.0.
