-- The pull request rework ratio counts reviewed pull requests only
-- (CHAOS-9015).
--
-- repo_metrics_daily.pr_rework_ratio holds merged pull requests with a
-- changes-requested review / ALL merged pull requests, and 0 for a day with
-- no merged pull request. A pull request whose reviews were never read has
-- no changes-requested count, so it was in the denominator with a numerator
-- of 0: a repository with no review data at all read a measured 0%.
--
-- The inputs of the ratio are now stored as counts on the same row, so a
-- reader sums them over the view's window and subject (a team = its owned
-- repositories) and applies one rule (internal/jobs/metrics/prrework.Rate):
--   no merged pull request                          -> NULL, not applicable
--   no reviewed pull request, every merged pull
--   request of a provider with no such event        -> NULL, not applicable
--   no reviewed pull request                        -> NULL, unknown
--   else                                            -> prs_merged_rework / prs_merged_reviewed
-- A window is the ratio of the sums, never a mean of daily ratios.
--
-- This migration only ADDS columns: old pods and a rollback keep reading the
-- schema they know, with the values they know. A row written before this
-- change holds NULL in every new column: its counts were not stored, which is
-- "not measured", never a measured 0. No stored row is rewritten.
--
-- prs_merged_reviewed: merged pull requests of the day that have review
-- evidence (a stored review) and are of a provider that has a
-- changes-requested event.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS prs_merged_reviewed Nullable(UInt32);
-- prs_merged_rework: of those, the pull requests with a changes-requested
-- review.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS prs_merged_rework Nullable(UInt32);
-- prs_merged_no_rework_signal: merged pull requests of a provider that has no
-- changes-requested event. The ratio is not applicable to them.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS prs_merged_no_rework_signal Nullable(UInt32);
-- pr_rework_ratio_reviewed: the one-day value for a reader of one row,
-- prs_merged_rework / prs_merged_reviewed. NULL when the day is not measured.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_rework_ratio_reviewed Nullable(Float64);

-- DEPRECATED: repo_metrics_daily.pr_rework_ratio. It stays Float64, not
-- Nullable, and keeps its old meaning. The writer keeps writing that value
-- there for older readers. No reader of this release reads it.
