-- Migration 047: run_id / completion-marker protocol for work_unit_membership
-- (CHAOS-2433)
--
-- PROTOCOL
--   Every membership write (from the LLM materializer or the no-LLM backfill)
--   stamps a single run_id on ALL membership rows of the run, then writes ONE
--   row to work_unit_membership_runs as the LAST step.  A run that has rows in
--   work_unit_membership but no matching row in work_unit_membership_runs is
--   INCOMPLETE (in-flight or crashed) and is NEVER visible to readers.
--
-- READ-SIDE PROTOCOL (replaces per-node max(computed_at))
--   Readers select the latest COMPLETE run for the org by querying
--   work_unit_membership_runs with argMax(run_id, completed_at) scoped to
--   org_id.  They then scope membership reads to rows whose run_id matches.
--
-- BENEFITS
--   1. Concurrency race: a partial materializer write in-flight is invisible
--      because its marker has not been written yet.
--   2. Split/merge stale: nodes absent from the latest complete run are simply
--      absent -- no stale rows are selected.
--   3. Partial-write divergence: runs without a marker are never selected.
--   4. Tombstones are unnecessary -- a churned/uncategorised node is absent
--      from the new complete run rather than needing an explicit sentinel.
--
-- ADD run_id to work_unit_membership
-- NOTE: The migration runner strips line comments and splits on semicolon before
-- executing each statement, so each statement must end with a semicolon and
-- comments must not contain semicolons (this is the known runner footgun)
ALTER TABLE work_unit_membership ADD COLUMN IF NOT EXISTS run_id String DEFAULT '';

-- CREATE the completion-marker table
-- One row per (org_id, run_id).  ReplacingMergeTree(completed_at) so an
-- idempotent re-write of the same run_id updates the completed_at stamp
-- rather than creating a duplicate.  ORDER BY (org_id, run_id) ensures each
-- org+run pair is a unique deduplicate key.  The ver column (completed_at)
-- means the row with the greatest completed_at value wins during merges
CREATE TABLE IF NOT EXISTS work_unit_membership_runs (
    org_id       String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, run_id)
