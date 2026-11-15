-- Rollback of 150_testops_test_flake_unknown_nullable.sql. It is NOT part of the chain (the migrator reads sql/
-- only); an operator runs it by hand, and only after the worker and query-api images are back on a build that writes
-- 0 for a day that measured no case.
--
-- Each NULL is first written as 0 (the value the older build writes), then each column is narrowed back to Float64,
-- its original type. ClickHouse narrows a Nullable column only with a DEFAULT; the DEFAULT 0 is then removed, so the
-- columns are again plain Float64 with no default, as before 150. The difference between a measured 0 and an unknown rate is lost for the rows already written.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build
-- that has the migration reads it as applied and never applies it again, so the schema would stay rolled back under
-- a build that expects it.
ALTER TABLE testops_test_metrics_daily UPDATE flake_rate = 0 WHERE flake_rate IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily UPDATE retry_dependency_rate = 0 WHERE retry_dependency_rate IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily UPDATE failure_recurrence_score = 0 WHERE failure_recurrence_score IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN flake_rate Float64 DEFAULT 0 SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN flake_rate REMOVE DEFAULT SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN retry_dependency_rate Float64 DEFAULT 0 SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN retry_dependency_rate REMOVE DEFAULT SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN failure_recurrence_score Float64 DEFAULT 0 SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN failure_recurrence_score REMOVE DEFAULT SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '150_testops_test_flake_unknown_nullable.sql' SETTINGS mutations_sync = 2;
