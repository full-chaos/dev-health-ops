-- compounding_risk_daily: the 30-day review window behind review_latency_p90h.
--
-- review_latency_p90h is now the p90 hours from creation to first review of the
-- pull requests merged in the 30 days ending on the row's day (it was the one-day
-- repo_metrics_daily.pr_first_review_p90_hours). These columns store the window:
-- merged_prs_30d pull requests merged, reviewed_prs_30d of them with a first
-- review, review_share_30d = reviewed / merged. They are stored, not scored.
--
-- NULL = the window was not read (a row written before this migration, or a row
-- of zeros of the stale-key rule). review_share_30d is also NULL when nothing
-- merged in the window: no merge is not a share of zero. A team row adds the
-- counts of its repositories.
ALTER TABLE compounding_risk_daily ADD COLUMN IF NOT EXISTS merged_prs_30d Nullable(UInt32);
ALTER TABLE compounding_risk_daily ADD COLUMN IF NOT EXISTS reviewed_prs_30d Nullable(UInt32);
ALTER TABLE compounding_risk_daily ADD COLUMN IF NOT EXISTS review_share_30d Nullable(Float64);
