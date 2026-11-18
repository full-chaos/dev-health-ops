-- Rollback of 156_testops_benchmarking_retracted.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs
-- it by hand, and only after the worker image is back on a build without 156 (that build neither writes nor reads the column).
--
-- The column is dropped from the six tables. A row that retired a result stays and then reads as a stored result with the column
-- defaults; no reader reads these tables.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the
-- migration reads it as applied and never applies it again, so the schema would stay rolled back under a build that expects it.
ALTER TABLE testops_metric_baselines DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_maturity_bands DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_metric_anomalies DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_period_comparisons DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_metric_correlations DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE testops_benchmark_insights DROP COLUMN IF EXISTS retracted SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '156_testops_benchmarking_retracted.sql' SETTINGS mutations_sync = 2;
