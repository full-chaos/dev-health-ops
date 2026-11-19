-- Rollback of 168_testops_test_failure_recurrence_counts.sql. It is NOT part of the chain (the migrator reads sql/ only); an
-- operator runs it by hand, and only after the worker, stream-runner and query-api images are back on a build without 168 (a
-- build with 168 refuses to start on a schema that lacks it).
--
-- The two count columns are dropped with the counts they hold. The older build names its columns and never names them.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS recurrent_failed_names SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS distinct_failed_names SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '168_testops_test_failure_recurrence_counts.sql' SETTINGS mutations_sync = 2;
