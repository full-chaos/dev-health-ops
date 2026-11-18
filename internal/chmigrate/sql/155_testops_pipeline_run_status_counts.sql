-- A daily pipeline row counts every run it holds in exactly one bucket.
--
--   testops_pipeline_metrics_daily.skipped_count           Nullable(UInt32)
--   testops_pipeline_metrics_daily.unfinished_count        Nullable(UInt32)
--   testops_pipeline_metrics_daily.other_count             Nullable(UInt32)
--   testops_pipeline_metrics_daily.unfinished_stale_count  Nullable(UInt32)
--
-- pipelines_count = success_count + failure_count + cancelled_count + skipped_count + unfinished_count + other_count.
-- unfinished_stale_count is the part of unfinished_count whose run row was last synced more than the open-run lookback
-- before computed_at, so it may already be finished.
--
-- No column has a default: a row stored before this migration reads NULL (not reported), never 0, until a recompute of
-- its day writes the counts. None of the four is in the sorting key, the partition key or the version column. ADD COLUMN
-- changes the table metadata only: no part is rewritten and no mutation is queued. A build without this migration names
-- its columns in every statement on the table, so it reads and writes as before. Every statement can run again.
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS skipped_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS other_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_stale_count Nullable(UInt32);
