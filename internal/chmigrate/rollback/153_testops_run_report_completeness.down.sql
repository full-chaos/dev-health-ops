-- Rollback of 153_testops_run_report_completeness.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator
-- runs it by hand, and only after the worker, stream-runner and query-api images are back on a build without 153 (a worker or
-- stream-runner build with 153 refuses to start on a schema that lacks it).
--
-- The completeness rows are dropped with the table, and the three daily columns with their values. A build without 153 neither
-- writes nor reads them. The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade`
-- on a build that has the migration reads it as applied and never applies it again, so the schema would stay rolled back under a
-- build that expects it.
DROP TABLE IF EXISTS testops_run_report_completeness SYNC;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS report_runs SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS partial_report_runs SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS unmarked_report_runs SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '153_testops_run_report_completeness.sql' SETTINGS mutations_sync = 2;
