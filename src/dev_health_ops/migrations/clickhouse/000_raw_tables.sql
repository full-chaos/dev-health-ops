CREATE TABLE IF NOT EXISTS repos (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS git_files (
    repo_id UUID,
    path String,
    executable UInt8,
    contents Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, path);

CREATE TABLE IF NOT EXISTS git_commits (
    repo_id UUID,
    hash String,
    message Nullable(String),
    author_name Nullable(String),
    author_email Nullable(String),
    author_when DateTime64(3, 'UTC'),
    committer_name Nullable(String),
    committer_email Nullable(String),
    committer_when DateTime64(3, 'UTC'),
    parents UInt32,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, hash);

CREATE TABLE IF NOT EXISTS git_commit_stats (
    repo_id UUID,
    commit_hash String,
    file_path String,
    additions Int32,
    deletions Int32,
    old_file_mode String,
    new_file_mode String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, commit_hash, file_path);

CREATE TABLE IF NOT EXISTS git_blame (
    repo_id UUID,
    path String,
    line_no UInt32,
    author_email Nullable(String),
    author_name Nullable(String),
    author_when Nullable(DateTime64(3, 'UTC')),
    commit_hash Nullable(String),
    line Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, path, line_no);

CREATE TABLE IF NOT EXISTS git_pull_requests (
    repo_id UUID,
    number UInt32,
    title Nullable(String),
    body Nullable(String),
    state Nullable(String),
    author_name Nullable(String),
    author_email Nullable(String),
    created_at DateTime64(3, 'UTC'),
    merged_at Nullable(DateTime64(3, 'UTC')),
    closed_at Nullable(DateTime64(3, 'UTC')),
    head_branch Nullable(String),
    base_branch Nullable(String),
    additions Nullable(UInt32),
    deletions Nullable(UInt32),
    changed_files Nullable(UInt32),
    first_review_at Nullable(DateTime64(3, 'UTC')),
    first_comment_at Nullable(DateTime64(3, 'UTC')),
    changes_requested_count UInt32 DEFAULT 0,
    reviews_count UInt32 DEFAULT 0,
    comments_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, number);

CREATE TABLE IF NOT EXISTS git_pull_request_reviews (
    repo_id UUID,
    number UInt32,
    review_id String,
    reviewer String,
    state String,
    submitted_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, number, review_id);

CREATE TABLE IF NOT EXISTS ci_pipeline_runs (
    repo_id UUID,
    run_id String,
    status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id);

CREATE TABLE IF NOT EXISTS deployments (
    repo_id UUID,
    deployment_id String,
    status Nullable(String),
    environment Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    deployed_at Nullable(DateTime64(3, 'UTC')),
    merged_at Nullable(DateTime64(3, 'UTC')),
    pull_request_number Nullable(UInt32),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, deployment_id);

CREATE TABLE IF NOT EXISTS incidents (
    repo_id UUID,
    incident_id String,
    status Nullable(String),
    started_at DateTime64(3, 'UTC'),
    resolved_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, incident_id);
