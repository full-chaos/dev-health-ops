-- CHAOS-9920: lead_p50_hours and churn_loc can hold NULL for "no data" (platform rule: a stored 0 is data, no row is
-- no data).
--
--   issue_type_metrics_daily.lead_p50_hours  Float64 -> Nullable(Float64): NULL when no completed item of the key
--                                            has a usable creation time.
--   work_item_completion_daily.churn_loc     UInt64 -> Nullable(UInt64): NULL when no pull request linked to the
--                                            key's completed items has a size.
--
-- The change is the type only. No stored row is rewritten: the rows already written keep the 0 they hold, and a
-- reader cannot tell, in an old row, a measured 0 from the 0 the old writers stored for "no data". A day reads
-- the new rule once it is recomputed. New writes store NULL. Every statement can run again.
ALTER TABLE issue_type_metrics_daily MODIFY COLUMN lead_p50_hours Nullable(Float64);
ALTER TABLE work_item_completion_daily MODIFY COLUMN churn_loc Nullable(UInt64);
