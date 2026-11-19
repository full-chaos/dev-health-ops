-- The daily rows of the pooled report charts keep the samples their values were taken over, so a chart, a tile and the
-- stored row of one repository-day read one sample set.
--
--   testops_pipeline_metrics_daily  pooled_service_ids Array(Nullable(String)),
--                                   pooled_durations Array(Nullable(Float64)),
--                                   pooled_queues Array(Nullable(Float64))   one element per counted run
--   testops_test_metrics_daily      pooled_service_ids Array(Nullable(String)),
--                                   pooled_durations Array(Nullable(Float64)) one element per counted suite
--   cicd_metrics_daily              pooled_run_minutes Array(Nullable(Float64)),
--                                   pooled_queue_minutes Array(Nullable(Float64))    one element per counted run
--   deploy_metrics_daily            pooled_deploy_hours Array(Float64)
--   repo_metrics_daily              pooled_pr_size_loc Array(Float64), pooled_bug_repair_hours Array(Float64)
--   incident_metrics_daily          pooled_repair_hours Array(Float64)
--   all six                         pooled_samples_stored UInt8 DEFAULT 0
--
-- The arrays of one table are parallel: the writer gives each the same length. pooled_samples_stored is 1 on a row
-- written with its samples, 2 on a row whose samples are over the writers' cap (100,000 per array): it stores empty
-- arrays, and its values stay over all samples. A row stored before this migration reads 0 and empty arrays. The readers
-- read the raw rows of every repository-day whose newest row is not 1, as before. No column is in the sorting key, the
-- partition key or the version column. ADD COLUMN changes the table metadata only: no part is rewritten and no mutation
-- is queued. Every statement can run again.
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS pooled_service_ids Array(Nullable(String));
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS pooled_durations Array(Nullable(Float64));
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS pooled_queues Array(Nullable(Float64));
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS pooled_service_ids Array(Nullable(String));
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS pooled_durations Array(Nullable(Float64));
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
ALTER TABLE cicd_metrics_daily ADD COLUMN IF NOT EXISTS pooled_run_minutes Array(Nullable(Float64));
ALTER TABLE cicd_metrics_daily ADD COLUMN IF NOT EXISTS pooled_queue_minutes Array(Nullable(Float64));
ALTER TABLE cicd_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
ALTER TABLE deploy_metrics_daily ADD COLUMN IF NOT EXISTS pooled_deploy_hours Array(Float64);
ALTER TABLE deploy_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pooled_pr_size_loc Array(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pooled_bug_repair_hours Array(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
ALTER TABLE incident_metrics_daily ADD COLUMN IF NOT EXISTS pooled_repair_hours Array(Float64);
ALTER TABLE incident_metrics_daily ADD COLUMN IF NOT EXISTS pooled_samples_stored UInt8 DEFAULT 0;
