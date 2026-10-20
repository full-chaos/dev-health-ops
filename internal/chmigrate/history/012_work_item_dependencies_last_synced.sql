-- Backfill last_synced for legacy work_item_dependencies tables.
ALTER TABLE work_item_dependencies
    ADD COLUMN IF NOT EXISTS last_synced DateTime64(3);
