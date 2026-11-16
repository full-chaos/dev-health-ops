-- A GitHub test-report row keyed on its report slot names the pre-slot rows it replaces (CHAOS-13925).
--
--   test_suite_results.legacy_suite_ids     Array(String) DEFAULT []
--   coverage_snapshots.legacy_snapshot_ids  Array(String) DEFAULT []
--
-- Since 151 a GitHub suite or coverage row hashes the report slot and the run attempt into its key; a row stored before it hashes
-- the artifact id. A slotted row lists the keys the older writer gives the same report: the artifact it was read from, and the
-- older artifacts of the same name the sync passed over. A reader drops a legacy row only when a kept slotted row lists its key.
-- A row stored before this migration, and every GitLab row, reads []: it replaces nothing.
--
-- Neither column is in a sorting key, the partition key or the version column. ADD COLUMN with a DEFAULT changes the table
-- metadata only: no part is rewritten and no mutation is queued. A build without this migration names its columns in every
-- statement on the two tables, so it reads and writes as before. Every statement can run again.
ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS legacy_suite_ids Array(String) DEFAULT [];
ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS legacy_snapshot_ids Array(String) DEFAULT [];
