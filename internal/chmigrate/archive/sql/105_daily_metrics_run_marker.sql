-- A day with no repository row is either a day with no activity or a day that
-- was never computed. The daily metrics run state lives only in Postgres
-- (daily_metrics_runs), so a reader on ClickHouse cannot tell the two apart.
-- This table is the ClickHouse record of that run state, one event per row.
--
-- Append-only. The marker may say 'succeeded' for an (org_id, target_day) only
-- when, at the moment of the append, committed Postgres says the latest run of
-- that day that computes the whole organization is succeeded. Whether a run
-- computes the whole organization is recorded at its creation in
-- daily_metrics_runs.full_org (true when no explicit repository list was
-- given). A run started with a repository list never certifies the day.
-- 'succeeded' is appended by one sync function after the finalize commit and by
-- the backfill verb. 'reopened' is appended when such a run is claimed for
-- dispatch and when a redrive or partition recompute reopens a succeeded run,
-- inside the transaction that does it, before its commit. Every writer of one
-- (org, day) holds one Postgres advisory lock. Nothing is updated or deleted.
--
-- Version is a Postgres clock reading in milliseconds taken in the transaction
-- that changed the run state, never the writing host's clock. Generation is
-- informational and is not ordered.
--
-- Reader contract: for each (org_id, target_day) take the row with the greatest
-- version over all generations, and let 'reopened' win a tie. That is
-- argMax(state, (version, state = 'reopened')). The day is certified only when
-- that state is 'succeeded'. No row, or a state of 'reopened', means unknown. It
-- never means zero activity. A failed append leaves the day without a newer
-- row, so the reader errs on the unknown side. The dho workers metrics
-- daily-marker-backfill verb rebuilds the rows from Postgres and is safe to run
-- again.
CREATE TABLE IF NOT EXISTS daily_metrics_run_marker (
    org_id String,
    target_day Date,
    generation String,
    state LowCardinality(String),
    finalized_at DateTime64(3, 'UTC'),
    version UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(target_day)
ORDER BY (org_id, target_day, version);
