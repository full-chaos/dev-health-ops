-- 0009 index for the outbox reaper's selection. ADDITIVE ONLY. See 0001's header.
--
-- audit.Reap selects "published before the cutoff, oldest first"
-- (published_at IS NOT NULL AND published_at < $1 ORDER BY published_at, id).
-- 0005's pending_idx covers only UNPUBLISHED rows, so without this the sweep
-- scans and sorts every published row, and the published backlog only grows.
--
-- Plain CREATE INDEX, not CONCURRENTLY: each migration runs in one transaction
-- with its version row, and CONCURRENTLY cannot run in a transaction. The
-- table holds no published rows until an outbox sink exists, so the write lock
-- the build takes is brief.
CREATE INDEX auth_outbox_events_published_idx
    ON auth_outbox_events (published_at, id) WHERE published_at IS NOT NULL;
