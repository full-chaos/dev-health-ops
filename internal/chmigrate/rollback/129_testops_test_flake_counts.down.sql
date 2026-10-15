-- Rollback of 129_testops_test_flake_counts.sql (CHAOS-9737). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after
-- the writers are back on a build that does not name the columns in its INSERT.
--
-- Dropping the columns loses the stored counts; flake_rate is not touched. The
-- next recompute on the newer build writes the counts again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS distinct_cases;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS flake_cases;
ALTER TABLE schema_migrations DELETE WHERE version = '129_testops_test_flake_counts.sql' SETTINGS mutations_sync = 2;
