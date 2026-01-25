CREATE TABLE IF NOT EXISTS teams (
    id String,
    team_uuid UUID,
    name String,
    description Nullable(String),
    members Array(String),
    updated_at DateTime64(6),
    last_synced DateTime64(6) DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);