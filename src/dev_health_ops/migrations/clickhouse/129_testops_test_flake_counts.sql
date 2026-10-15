-- CHAOS-9737: store the numerator and denominator of flake_rate beside it:
-- flake_cases (distinct case names that both passed and failed in the day) and
-- distinct_cases (distinct case names of the day), so an org rate is
-- SUM(flake_cases) / SUM(distinct_cases) and not a mean of rates.
-- Nullable with no default on purpose: a row written before this column reads
-- NULL (counts unknown), which the org series skips, until a recompute of its
-- day (the daily job over that range) rewrites it with both counts.
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS flake_cases Nullable(UInt32) AFTER flake_rate;
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS distinct_cases Nullable(UInt32) AFTER flake_cases;
