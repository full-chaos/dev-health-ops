-- Rollback of 163_testops_risk_retracted.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs it
-- by hand, and only after the worker and query-api images are back on a build without 163 (a build with 163 reads the column).
--
-- The columns are dropped. A row that retired a day stays and then reads as a stored row of zeros to the older readers, until
-- the day is recomputed by the older build (which writes the day's record or nothing).
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_release_confidence DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_quality_drag DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_pipeline_stability DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '163_testops_risk_retracted.sql' SETTINGS mutations_sync = 2;
