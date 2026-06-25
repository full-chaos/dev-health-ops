CREATE TABLE IF NOT EXISTS team_drift_changes (
  org_id String,
  change_id String,
  entity_type LowCardinality(String),
  entity_id String,
  provider LowCardinality(String),
  native_team_key Nullable(String),
  change_type LowCardinality(String),
  field Nullable(String),
  old_value_json String,
  new_value_json String,
  status LowCardinality(String),
  first_seen_at DateTime64(6, 'UTC'),
  last_seen_at DateTime64(6, 'UTC'),
  decided_at Nullable(DateTime64(6, 'UTC')),
  decided_by Nullable(String),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, change_id);
