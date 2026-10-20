CREATE TABLE IF NOT EXISTS team_provider_observations (
  org_id String,
  provider LowCardinality(String),
  native_team_key String,
  team_id String,
  name Nullable(String),
  description Nullable(String),
  members_json String,
  project_keys_json String,
  repo_patterns_json String,
  is_active UInt8,
  parent_team_id Nullable(String),
  discovered_at DateTime64(6, 'UTC'),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, native_team_key);
