-- The TestOps test rates of a repo-day that measured no case are unknown, not 0.
--
--   testops_test_metrics_daily.flake_rate                Float64 -> Nullable(Float64)
--   testops_test_metrics_daily.retry_dependency_rate     Float64 -> Nullable(Float64)
--   testops_test_metrics_daily.failure_recurrence_score  Float64 -> Nullable(Float64)
--
-- The three are rates over the day's measured cases. A day whose case rows are all of runs left out of the flake
-- measures, or a day with no case row, measured none: the writer stores NULL there, and every reader skips NULL.
--
-- A row stored before this migration keeps its value: the type change converts each stored value and makes no NULL.
-- A stored 0 cannot be told apart from a measured 0, so no row is rewritten to NULL; a recompute of the day writes it.
-- None of the three is in the sorting key, the partition key or the version column. The change is a mutation that
-- rewrites the three columns of every part; mutations_sync = 2 makes each statement return when it is done. A second
-- run changes no type and queues no mutation. Every statement can run again.
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN flake_rate Nullable(Float64) SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN retry_dependency_rate Nullable(Float64) SETTINGS mutations_sync = 2;
ALTER TABLE testops_test_metrics_daily MODIFY COLUMN failure_recurrence_score Nullable(Float64) SETTINGS mutations_sync = 2;
