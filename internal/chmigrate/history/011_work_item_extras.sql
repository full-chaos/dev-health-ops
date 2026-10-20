ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS priority_raw Nullable(String),
    ADD COLUMN IF NOT EXISTS service_class Nullable(String),
    ADD COLUMN IF NOT EXISTS due_at Nullable(DateTime64(3));

-- work_item_dependencies: Tracks relationships between work items (blocks, relates, etc.).
-- Using ReplacingMergeTree for deduplication on sync retries.
-- Version column last_synced tracks when the row was last synced.
CREATE TABLE IF NOT EXISTS work_item_dependencies (
    source_work_item_id String,
    target_work_item_id String,
    relationship_type String,
    relationship_type_raw String,
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (source_work_item_id, target_work_item_id, relationship_type);

-- work_item_reopen_events: Records when work items are reopened from done/canceled states.
-- Using ReplacingMergeTree for deduplication on sync retries.
-- Version column last_synced tracks when the row was last synced.
CREATE TABLE IF NOT EXISTS work_item_reopen_events (
    work_item_id String,
    occurred_at DateTime64(3),
    from_status String,
    to_status String,
    from_status_raw Nullable(String),
    to_status_raw Nullable(String),
    actor Nullable(String),
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (work_item_id, occurred_at);

-- work_item_interactions: Records comments and other interaction events on work items.
-- Using ReplacingMergeTree for deduplication on sync retries.
-- ORDER BY includes interaction_type to allow efficient deduplication when the same
-- interaction event might be recorded multiple times at the same timestamp.
-- Version column last_synced tracks when the row was last synced.
CREATE TABLE IF NOT EXISTS work_item_interactions (
    work_item_id String,
    provider String,
    interaction_type String,
    occurred_at DateTime64(3),
    actor Nullable(String),
    body_length UInt32,
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (work_item_id, occurred_at, interaction_type);

-- sprints: Stores sprint metadata from work tracking systems.
-- Using ReplacingMergeTree for deduplication on sync retries.
-- Version column last_synced tracks when the row was last synced.
CREATE TABLE IF NOT EXISTS sprints (
    provider String,
    sprint_id String,
    name Nullable(String),
    state Nullable(String),
    started_at Nullable(DateTime64(3)),
    ended_at Nullable(DateTime64(3)),
    completed_at Nullable(DateTime64(3)),
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (provider, sprint_id);
