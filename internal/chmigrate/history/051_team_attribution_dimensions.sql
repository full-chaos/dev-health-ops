ALTER TABLE teams ADD COLUMN IF NOT EXISTS provider String DEFAULT '' AFTER org_id;
ALTER TABLE teams ADD COLUMN IF NOT EXISTS native_team_key Nullable(String) AFTER provider;
ALTER TABLE teams ADD COLUMN IF NOT EXISTS parent_team_id Nullable(String) AFTER native_team_key;

CREATE TABLE IF NOT EXISTS projects (
    id String,
    org_id String,
    provider String,
    project_key Nullable(String),
    name String,
    is_active UInt8 DEFAULT 1,
    updated_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, id);

CREATE TABLE IF NOT EXISTS members (
    org_id String,
    member_id String,
    name String,
    email Nullable(String),
    provider_identities String,
    is_active UInt8 DEFAULT 1,
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, member_id);

CREATE TABLE IF NOT EXISTS team_memberships (
    org_id String,
    provider String,
    team_id String,
    member_id String,
    raw_provider_user_id Nullable(String),
    raw_email Nullable(String),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0,
    specificity UInt16 DEFAULT 0,
    priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, team_id, member_id, source, valid_from);

CREATE TABLE IF NOT EXISTS team_project_ownership (
    org_id String,
    provider String,
    team_id String,
    project_id String,
    project_key Nullable(String),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0,
    specificity UInt16 DEFAULT 0,
    priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, project_id, team_id, source, valid_from);

CREATE TABLE IF NOT EXISTS team_repo_ownership (
    org_id String,
    provider String,
    team_id String,
    repo_id Nullable(UUID),
    repo_full_name String,
    match_type Enum8('exact' = 1, 'pattern' = 2),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0,
    specificity UInt16 DEFAULT 0,
    priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from);

CREATE TABLE IF NOT EXISTS work_item_team_attributions (
    org_id String,
    repo_id UUID,
    work_item_id String,
    provider String,
    team_id Nullable(String),
    team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6),
    is_primary UInt8,
    confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3),
    evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source);
