-- A marker for the row that retires a TestOps risk day (testops_release_confidence, testops_quality_drag,
-- testops_pipeline_stability).
--
--   <table>.retracted  UInt8 DEFAULT 0
--
-- Each table holds one row for each (org, repo, day) and a reader takes the newest row of a key. A recompute of a day that
-- gives the testops_risk family no record for a table (the day's only run moved to a later day, or no run of the day
-- completed) produces no row, so the row written before stayed the newest row of its key for ever. The family now writes a row
-- over such a key with 1 here: the key, computed_at and the team and service of the row it retires, the column default in every
-- measure. A reader takes the NEWEST row of each key first and leaves the key out when that row holds 1, so the day reads as
-- absent, never as 0.
--
-- The column is not in the sorting key, the partition key or the version column. ADD COLUMN with a DEFAULT changes the table
-- metadata only: no part is rewritten and no mutation is queued. Every stored row reads 0, a row the compute made. A build
-- without this migration names its columns in every INSERT, so it writes 0. Every statement can run again.
ALTER TABLE testops_release_confidence ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_quality_drag ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
ALTER TABLE testops_pipeline_stability ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;
