-- Rollback of 151_testops_report_slot_attempt.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs it
-- by hand, and only after the worker, stream-runner and query-api images are back on a build without 151 (a build with 151 refuses
-- to start on a schema that lacks it).
--
-- The four columns are dropped. The slot and attempt of the rows the newer build wrote are lost; those rows stay, and the older build
-- counts every suite row of a run again, as it did before 151, until the days are recomputed.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE test_suite_results
    DROP COLUMN IF EXISTS report_slot,
    DROP COLUMN IF EXISTS run_attempt
SETTINGS mutations_sync = 2;
ALTER TABLE coverage_snapshots
    DROP COLUMN IF EXISTS report_slot,
    DROP COLUMN IF EXISTS run_attempt
SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '151_testops_report_slot_attempt.sql' SETTINGS mutations_sync = 2;
