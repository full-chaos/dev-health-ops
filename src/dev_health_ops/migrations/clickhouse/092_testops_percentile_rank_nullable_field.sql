-- CHAOS-4806, ruling R73, codex round chaos-4806-r3b P1 (executed repro):
-- percentile_rank was left required in migrations 090/091 on the stated
-- reasoning that PercentileRank's own ratio-of-counts formula is always a
-- bounded, finite 0-100 -- true for a NaN comparison value (every
-- comparison against NaN is false), but FALSE for a +-Inf comparison
-- value: every finite candidate genuinely compares less-than +Inf (or
-- greater-than -Inf), so a scope whose own CurrentValue was correctly
-- nulled as undefined could still rank at a fully-confident 100
-- ("leading" maturity, 1.0 confidence) -- a worse defect than a bland
-- 0.0, because it actively misrepresents undefined data as top-tier.
-- Widen both tables' percentile_rank column so a non-finite (or
-- no-cohort) rank can be written as a real NULL instead.
ALTER TABLE testops_metric_baselines MODIFY COLUMN percentile_rank Nullable(Float64);
ALTER TABLE testops_maturity_bands MODIFY COLUMN percentile_rank Nullable(Float64);
