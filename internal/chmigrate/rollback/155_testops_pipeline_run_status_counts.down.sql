-- Rollback of 155_testops_pipeline_run_status_counts.sql. It is NOT part of the chain (the migrator reads sql/ only); an
-- operator runs it by hand, and only after the worker and stream-runner images are back on a build without 155 (a build
-- with 155 refuses to start on a schema that lacks it, and its worker names the four columns in its INSERT).
--
-- The four columns are dropped. The counts the newer build wrote are lost; the rows and their other columns stay.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS unfinished_stale_count SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS other_count SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS unfinished_count SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS skipped_count SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '155_testops_pipeline_run_status_counts.sql' SETTINGS mutations_sync = 2;
