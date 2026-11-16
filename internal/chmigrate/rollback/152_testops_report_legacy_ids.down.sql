-- Rollback of 152_testops_report_legacy_ids.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs it
-- by hand, and only after the worker, stream-runner and query-api images are back on a build without 152 (a build with 152 refuses
-- to start on a schema that lacks it).
--
-- The two columns are dropped. The legacy keys the newer build wrote are lost; the rows stay. A reader without 152 does not read them.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE test_suite_results DROP COLUMN IF EXISTS legacy_suite_ids SETTINGS mutations_sync = 2;
ALTER TABLE coverage_snapshots DROP COLUMN IF EXISTS legacy_snapshot_ids SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '152_testops_report_legacy_ids.sql' SETTINGS mutations_sync = 2;
