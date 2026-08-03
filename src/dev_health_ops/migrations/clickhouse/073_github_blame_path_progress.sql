-- Durable path-level progress for the bounded GitHub blame crawl.
-- Empty files need a progress marker without fabricating git_blame rows, and
-- retryable per-file failures need attempt history so one permanently
-- unblameable path cannot starve the rest of a repository.
CREATE TABLE IF NOT EXISTS github_blame_path_progress (
    org_id LowCardinality(String),
    repo_id UUID,
    tree_ref String,
    path String,
    generation String,
    outcome LowCardinality(String),
    attempted_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_github_blame_path_outcome CHECK outcome IN ('rows', 'empty', 'retryable_error')
) ENGINE = ReplacingMergeTree(attempted_at)
ORDER BY (org_id, repo_id, tree_ref, path, generation);
