-- 0006 an index serving the outbox reaper (CHAOS-4885).
-- ADDITIVE ONLY. See 0001's header.

-- 0005 indexed the PUBLISHER's hot query -- unpublished, due now, oldest first
-- -- with a partial index over published_at IS NULL. The reaper's predicate is
-- that index's exact complement:
--
--     published_at IS NOT NULL AND published_at < $1  ORDER BY published_at, id
--
-- so no index created in 0005 can serve it and the reclaimer scans the table.
--
-- The DIRECTION of that failure is why this is a migration and not a note.
-- 0005's own comment observes that the published backlog grows "forever"; the
-- reaper is the thing that stops it growing. An unindexed reap therefore gets
-- slower exactly as the backlog it exists to reclaim gets larger -- it is
-- slowest at the moment it matters most. It also holds its transaction open
-- longer over a table the relay is concurrently reading, which is the very
-- interaction the reaper's FOR UPDATE SKIP LOCKED was chosen to avoid.
--
-- Partial rather than total, mirroring the predicate: the unpublished rows are
-- already served by auth_outbox_events_pending_idx, and excluding them keeps
-- this index proportional to the reclaimable backlog instead of the table.
CREATE INDEX auth_outbox_events_reapable_idx
    ON auth_outbox_events (published_at, id) WHERE published_at IS NOT NULL;
