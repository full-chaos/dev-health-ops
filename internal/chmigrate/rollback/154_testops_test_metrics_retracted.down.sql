-- Rollback of 154_testops_test_metrics_retracted.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs
-- it by hand, and only after the worker, stream-runner and query-api images are back on a build without 154 (a build with 154
-- refuses to start on a schema that lacks it).
--
-- The column is dropped. A row that retired a day stays and then reads as a stored row of zeros to the older readers, until
-- the day is recomputed by the older build (which writes the day's record or nothing).
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_test_metrics_daily DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '154_testops_test_metrics_retracted.sql' SETTINGS mutations_sync = 2;
