-- Backfill last_synced for legacy work item extras tables.
ALTER TABLE work_item_reopen_events
    ADD COLUMN IF NOT EXISTS last_synced DateTime64(3);
ALTER TABLE work_item_interactions
    ADD COLUMN IF NOT EXISTS last_synced DateTime64(3);
ALTER TABLE sprints
    ADD COLUMN IF NOT EXISTS last_synced DateTime64(3);
