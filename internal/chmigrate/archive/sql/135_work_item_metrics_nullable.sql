-- CHAOS-9920: lead_p50_hours and churn_loc can hold NULL for "no data" (platform rule: a stored 0 is data, no row is
-- no data).
--
--   issue_type_metrics_daily.lead_p50_hours  Float64 -> Nullable(Float64): NULL when no completed item of the key
--                                            has a usable creation time.
--   work_item_completion_daily.churn_loc     UInt64 -> Nullable(UInt64): NULL when no pull request linked to the
--                                            key's completed items has a size.
--
-- Both columns change type. Only lead_p50_hours has its rows rewritten: a stored lead time of 0 is set to NULL. That
-- is exact, because no compute ever stored a real lead time of 0 hours (a lead of 0 or less is left out of the median,
-- see workitemengine ComputeIssueTypeMetricsDaily), so every stored 0 meant "no lead time", and the report mean no
-- longer counts those rows as a lead time of 0 hours. churn_loc is NOT rewritten: a stored churn of 0 can be a real
-- 0-line change or "no sized pull request", and the two cannot be told apart in a stored row. Its old rows keep their 0
-- until the day is recomputed. Every statement can run again.
ALTER TABLE issue_type_metrics_daily MODIFY COLUMN lead_p50_hours Nullable(Float64);
ALTER TABLE work_item_completion_daily MODIFY COLUMN churn_loc Nullable(UInt64);
ALTER TABLE issue_type_metrics_daily UPDATE lead_p50_hours = NULL WHERE lead_p50_hours = 0 SETTINGS mutations_sync = 2;
