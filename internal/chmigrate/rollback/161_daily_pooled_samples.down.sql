-- Rollback of 161_daily_pooled_samples.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs
-- it by hand, and only after the worker, stream-runner, query-api and report images are back on a build without 161 (a
-- build with 161 refuses to start on a schema that lacks it, and its writers name the new columns in their INSERTs).
--
-- The columns are dropped. The samples the newer build wrote are lost; the rows and their other columns stay, and the
-- older build reads the raw rows as before. The version row goes last: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it as applied and never applies it again.
ALTER TABLE incident_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE incident_metrics_daily DROP COLUMN IF EXISTS pooled_repair_hours SETTINGS mutations_sync = 2;
ALTER TABLE repo_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE repo_metrics_daily DROP COLUMN IF EXISTS pooled_bug_repair_hours SETTINGS mutations_sync = 2;
ALTER TABLE repo_metrics_daily DROP COLUMN IF EXISTS pooled_pr_size_loc SETTINGS mutations_sync = 2;
ALTER TABLE deploy_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE deploy_metrics_daily DROP COLUMN IF EXISTS pooled_deploy_hours SETTINGS mutations_sync = 2;
ALTER TABLE cicd_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE cicd_metrics_daily DROP COLUMN IF EXISTS pooled_queue_minutes SETTINGS mutations_sync = 2;
ALTER TABLE cicd_metrics_daily DROP COLUMN IF EXISTS pooled_run_minutes SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS pooled_durations SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS pooled_service_ids SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS pooled_samples_stored SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS pooled_queues SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS pooled_durations SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS pooled_service_ids SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '161_daily_pooled_samples.sql' SETTINGS mutations_sync = 2;
