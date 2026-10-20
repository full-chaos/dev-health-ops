-- Migration 065: add source_id provenance column to the 9 external-ingest
-- record-kind tables (CHAOS-2698). NULL for every existing native-sync row.
-- Set to the resolved customer-push source's UUID (external_ingest_sources.id,
-- CHAOS-2696 Postgres migration 0032) by the external-ingest sink-write layer.
-- Not part of any ORDER BY key -- purely a queryable attribution column.
ALTER TABLE repos                    ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_commits               ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_pull_requests         ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_pull_request_reviews  ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE teams                     ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE identities                ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_items                ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_item_transitions     ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_item_dependencies    ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
