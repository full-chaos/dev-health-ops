-- A marker for the row that retires a repository-day of four pooled daily tables.
--
--   incident_metrics_daily / deploy_metrics_daily / repo_metrics_daily / testops_pipeline_metrics_daily  .retracted  UInt8 DEFAULT 0
--
-- Each table holds one row for each (org, repo, day) and a reader takes the newest row of a key. A recompute of a day that
-- finds no sample any more (an expired incident mapping, a cancelled deployment, a moved merge, a deleted run) produces no
-- record, so the row written before stayed the newest row of its key, with its stored samples (migration 161). The family
-- writes a row over such a key with 1 here: the key, computed_at, and the column default in every other column. A reader
-- takes the NEWEST row of each key first and leaves the key out when that row holds 1, so the day reads as absent, never 0.
--
-- The column is not in the sorting key, the partition key or the version column. ADD COLUMN with a DEFAULT changes the table
-- metadata only: no part is rewritten and no mutation is queued. Every stored row reads 0, a row the compute made. A build
-- without this migration names its columns in every INSERT, so it writes 0. Every statement can run again.
ALTER TABLE incident_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE deploy_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
