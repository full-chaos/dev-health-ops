-- Store the numerator and denominator of branch_coverage_pct beside it in testops_coverage_metrics_daily:
--
--   branches_total   Nullable(UInt32)  the branches of the newest coverage snapshot of the repository and day
--   branches_covered Nullable(UInt32)  the covered branches of that snapshot
--
-- A benchmarking point over several repositories is then 100 * SUM(branches_covered) / SUM(branches_total), not a mean of
-- percentages, the way the line coverage is pooled over lines_covered and lines_total.
-- Nullable with no default on purpose: a row stored before these columns reads NULL (counts unknown), which the pooled
-- series leaves out of both sums until a recompute of its day rewrites it with the counts. A row that retires a day
-- writes neither column, so it also reads NULL. ADD COLUMN changes the table metadata only: no part is rewritten and no
-- mutation is queued. Every statement can run again.
ALTER TABLE testops_coverage_metrics_daily ADD COLUMN IF NOT EXISTS branches_total Nullable(UInt32) AFTER lines_covered;
ALTER TABLE testops_coverage_metrics_daily ADD COLUMN IF NOT EXISTS branches_covered Nullable(UInt32) AFTER branches_total;
