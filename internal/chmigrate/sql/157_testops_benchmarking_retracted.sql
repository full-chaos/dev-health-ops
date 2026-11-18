-- A marker for the row that retires a benchmarking result of a day, on the six benchmarking output tables.
--
--   testops_metric_baselines / testops_maturity_bands / testops_metric_anomalies / testops_period_comparisons /
--   testops_metric_correlations / testops_benchmark_insights  .retracted  UInt8 DEFAULT 0
--
-- The tables are plain MergeTree, written append only by the benchmarking finalize family. A daily run of the whole
-- organization whose discovery found no repository has no benchmark target for its day, but the results an earlier compute
-- wrote for that day stayed. The family now writes, on such a run, one row over each key of the day with 1 here: the key,
-- org_id, the day, computed_at strictly newer than the stored row, the column default in every other column. A reader takes
-- the NEWEST row of each key first and leaves the key out when that row holds 1. No reader reads these tables today.
--
-- The column is not in the sorting key, the partition key or a version column. ADD COLUMN with a DEFAULT changes the table
-- metadata only: no part is rewritten and no mutation is queued. Every stored row reads 0, a row the compute made. A build
-- without this migration names its columns in every INSERT, so it writes 0. Every statement can run again.
ALTER TABLE testops_metric_baselines ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_maturity_bands ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_metric_anomalies ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_period_comparisons ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_metric_correlations ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_benchmark_insights ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
