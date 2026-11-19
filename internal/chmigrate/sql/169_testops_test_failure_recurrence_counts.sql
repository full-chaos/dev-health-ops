-- The two counts of failure_recurrence_score of a TestOps test day (testops_test_metrics_daily).
--
--   testops_test_metrics_daily.recurrent_failed_names  Nullable(UInt32)
--   testops_test_metrics_daily.distinct_failed_names   Nullable(UInt32)
--
-- failure_recurrence_score is recurrent_failed_names / distinct_failed_names: the failed test names of the day that also
-- failed in the 29 days before it, over the distinct failed test names of the day. A window of days takes the sum of the
-- first over the sum of the second, never the mean of the stored scores. failed_count counts failed executions, retries
-- included, so it is not the denominator.
--
-- The writer stores both counts on every row it makes (0 and 0 on a day with no failed name). A row stored before this
-- migration reads NULL in both: it has no counts and is in neither sum.
--
-- The columns are not in the sorting key, the partition key or the version column. ADD COLUMN of a Nullable column
-- changes the table metadata only: no part is rewritten and no mutation is queued. Every statement can run again.
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS recurrent_failed_names Nullable(UInt32);
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS distinct_failed_names Nullable(UInt32);
