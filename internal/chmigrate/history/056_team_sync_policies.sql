CREATE TABLE IF NOT EXISTS team_sync_policies (
  org_id String,
  team_id String,
  sync_policy UInt8 DEFAULT 0,
  managed_fields Array(String) DEFAULT [],
  updated_by Nullable(String),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, team_id);
