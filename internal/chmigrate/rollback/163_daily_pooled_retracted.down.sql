-- Rollback of 163_daily_pooled_retracted.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs it by
-- hand, and only after query-api and the worker image are back on a build without 163 (that build neither writes nor reads the
-- column).
--
-- The column is dropped from the four tables. A row that retired a repository-day stays and then reads as a stored row with the
-- column defaults (counts 0, values NULL, pooled_samples_stored 0, so the pooled readers read its raw rows).
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE incident_metrics_daily DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE deploy_metrics_daily DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE repo_metrics_daily DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_metrics_daily DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '163_daily_pooled_retracted.sql' SETTINGS mutations_sync = 2;
