-- The GitHub test-report rows of one run are keyed on the report slot (the artifact name) and the run attempt that made it.
--
--   test_suite_results.report_slot   LowCardinality(String) DEFAULT ''
--   test_suite_results.run_attempt   UInt32 DEFAULT 0
--   coverage_snapshots.report_slot   LowCardinality(String) DEFAULT ''
--   coverage_snapshots.run_attempt   UInt32 DEFAULT 0
--
-- A re-run attempt uploads a new artifact under the same name. Its suite and coverage rows carry the slot and the attempt, so a
-- reader takes the totals of a run from the newest attempt of each slot, while the case rows of every attempt stay for the flake
-- measures. A row stored before this migration, and every GitLab row, reads '' and 0: a legacy row, counted as before.
--
-- None of the four is in a sorting key, the partition key or the version column. ADD COLUMN with a DEFAULT changes the table
-- metadata only: no part is rewritten and no mutation is queued. A build without this migration names its columns in every
-- statement on the two tables, so it reads and writes as before. Every statement can run again.
ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS report_slot LowCardinality(String) DEFAULT '';
ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS run_attempt UInt32 DEFAULT 0;
ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS report_slot LowCardinality(String) DEFAULT '';
ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS run_attempt UInt32 DEFAULT 0;
