CREATE TABLE IF NOT EXISTS jira_project_ops_team_links (
    project_key String,
    ops_team_id String,
    project_name String,
    ops_team_name String,
    updated_at DateTime64(6),
    last_synced DateTime64(6) DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_key, ops_team_id);
