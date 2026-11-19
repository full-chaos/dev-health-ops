-- Rollback of 169_testops_coverage_branch_counts.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator
-- runs it by hand, and only after the worker and benchmarking images are back on a build that does not name the columns.
--
-- The columns are dropped and the stored branch counts are lost; branch_coverage_pct is not touched.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_coverage_metrics_daily DROP COLUMN IF EXISTS branches_covered SETTINGS mutations_sync = 2;
ALTER TABLE testops_coverage_metrics_daily DROP COLUMN IF EXISTS branches_total SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '169_testops_coverage_branch_counts.sql' SETTINGS mutations_sync = 2;
