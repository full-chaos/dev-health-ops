-- work_graph_edges retires an edge by appending a tombstone instead of running
-- ALTER TABLE ... DELETE: a copy of the identity's latest version with
-- is_deleted = 1 and a newer last_synced. Readers drop an identity whose
-- latest version is a tombstone. Same shape as the operational_* tables
-- (a plain is_deleted UInt8 beside the ReplacingMergeTree version column).
--
-- ADD COLUMN is a metadata change: no part is rewritten and no mutation is
-- queued. Existing rows read the DEFAULT, so every stored edge stays live.
ALTER TABLE work_graph_edges ADD COLUMN IF NOT EXISTS is_deleted UInt8 DEFAULT 0;
