-- Rollback of 129_testops_test_flake_counts.sql (CHAOS-9737). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after
-- the writers are back on a build that does not name the columns in its INSERT.
--
-- Dropping the columns loses the stored counts; flake_rate is not touched. The
-- next recompute on the newer build writes the counts again.
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS distinct_cases;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS flake_cases;
