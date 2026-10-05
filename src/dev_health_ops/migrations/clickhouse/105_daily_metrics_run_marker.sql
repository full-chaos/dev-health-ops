-- A day with no repository row is either a day with no activity or a day that
-- was never computed. The daily metrics run state lives only in Postgres
-- (daily_metrics_runs), so a reader on ClickHouse cannot tell the two apart.
-- This table is the ClickHouse record of that run state, one event per row.
--
-- Append-only. The worker appends state 'succeeded' after the Postgres commit
-- that moves the org-day run to status and finalization_status 'succeeded'.
-- A redrive or partition recompute that reopens a succeeded run appends state
-- 'reopened' before it commits the reopen. Nothing is updated or deleted.
--
-- Reader contract: for each (org_id, target_day) take argMax(state, version)
-- over every generation. The day is certified only when that state is
-- 'succeeded'. No row, or a latest state of 'reopened', means unknown. It never
-- means zero activity. A failed append leaves the day without a newer row, so
-- the reader errs on the unknown side. The dho metrics daily-run-marker-backfill
-- verb rebuilds the rows from Postgres and is safe to run again.
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
