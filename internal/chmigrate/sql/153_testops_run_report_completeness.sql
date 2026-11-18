-- A run's test reports are read whole, or the run is named partial with the reasons.
--
--   testops_run_report_completeness   one row per (org_id, repo_id, run_id, run_attempt)
--   testops_test_metrics_daily.report_runs / partial_report_runs / unmarked_report_runs   Nullable(UInt32)
--
-- The sync writes one completeness row for each attempt of a run whose report sources it walked: complete = 1 when every
-- report was read, else complete = 0 and reasons names each loss (closed vocabulary, sorted). The provider keeps one run_id
-- over all attempts, and the readers count the newest attempt of each report slot, so the attempt is in the key: a re-run that
-- loses a report names the run partial although an earlier attempt was complete.
--
-- The version is verdict_version = complete * 2^42 + the walk's last_synced in milliseconds. FINAL therefore keeps, per key, a
-- complete row over any partial row and, between rows of the same verdict, the newest walk: a later walk of the same attempt
-- that could not read a report it read before never turns a complete row partial; a partial row becomes complete when a walk
-- reads every report. A reader reads the table FINAL and takes the row of max(run_attempt) per run.
--
-- The three daily columns count, per repo-day, the runs with a kept suite (report_runs), of those the runs whose newest attempt
-- is partial (partial_report_runs), and the runs with no completeness row at all (unmarked_report_runs: stored before the
-- writer, or a provider that writes none). NULL is a row written before the columns: not measured, never 0.
--
-- CREATE TABLE IF NOT EXISTS and ADD COLUMN IF NOT EXISTS: every statement can run again. ADD COLUMN with no DEFAULT on a
-- Nullable column changes the table metadata only: no part is rewritten and no mutation is queued. None of the three columns is
-- in a sorting key, the partition key or the version column.
CREATE TABLE IF NOT EXISTS testops_run_report_completeness
(
    `org_id` LowCardinality(String),
    `repo_id` UUID,
    `run_id` String,
    `run_attempt` UInt32,
    `provider` LowCardinality(String),
    `complete` UInt8,
    `reasons` Array(LowCardinality(String)),
    `reports_expected` UInt32,
    `reports_admitted` UInt32,
    `last_synced` DateTime64(3, 'UTC'),
    `verdict_version` UInt64
)
ENGINE = ReplacingMergeTree(verdict_version)
ORDER BY (org_id, repo_id, run_id, run_attempt);
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS report_runs Nullable(UInt32);
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS partial_report_runs Nullable(UInt32);
ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS unmarked_report_runs Nullable(UInt32);
