-- Work Graph Tables
-- Stores derived linkage relationships between work items, PRs, commits, and files.

-- Generic edge store with provenance and confidence tracking
-- Supports all node types: issue, pr, commit, file
CREATE TABLE IF NOT EXISTS work_graph_edges (
    edge_id String COMMENT 'Deterministic hash of (source_type, source_id, edge_type, target_type, target_id)',
    source_type String COMMENT 'Type of source node: issue|pr|commit|file',
    source_id String COMMENT 'Source identifier: work_item_id, repo#pr, repo@sha, repo:path',
    target_type String COMMENT 'Type of target node: issue|pr|commit|file',
    target_id String COMMENT 'Target identifier',
    edge_type String COMMENT 'Relationship type: references|implements|contains|touches|blocks|relates|duplicates|etc',
    repo_id Nullable(UUID) COMMENT 'Repository UUID if applicable',
    provider Nullable(String) COMMENT 'Provider: jira|github|gitlab',
    provenance String COMMENT 'How edge was discovered: native|explicit_text|heuristic',
    confidence Float32 COMMENT 'Confidence score 0.0-1.0',
    evidence String COMMENT 'Short token or rule ID explaining discovery',
    discovered_at DateTime64(3, 'UTC') COMMENT 'When edge was first discovered',
    last_synced DateTime64(3, 'UTC') COMMENT 'Last sync timestamp for ReplacingMergeTree'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (source_type, source_id, edge_type, target_type, target_id)
COMMENT 'Generic work graph edge store with provenance tracking';

-- Fast path table for issue <-> PR relationships
CREATE TABLE IF NOT EXISTS work_graph_issue_pr (
    repo_id UUID COMMENT 'Repository UUID',
    work_item_id String COMMENT 'Work item ID (e.g., jira:ABC-123, gh:owner/repo#123)',
    pr_number UInt32 COMMENT 'Pull request number',
    confidence Float32 COMMENT 'Confidence score 0.0-1.0',
    provenance String COMMENT 'How link was discovered: native|explicit_text|heuristic',
    evidence String COMMENT 'Matched token or rule ID',
    last_synced DateTime64(3, 'UTC') COMMENT 'Last sync timestamp'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, work_item_id, pr_number)
COMMENT 'Fast path table for issue to PR relationships';

-- Fast path table for PR <-> commit relationships
CREATE TABLE IF NOT EXISTS work_graph_pr_commit (
    repo_id UUID COMMENT 'Repository UUID',
    pr_number UInt32 COMMENT 'Pull request number',
    commit_hash String COMMENT 'Git commit SHA',
    confidence Float32 COMMENT 'Confidence score 0.0-1.0',
    provenance String COMMENT 'How link was discovered: native|explicit_text|heuristic',
    evidence String COMMENT 'Source of link (e.g., api_pr_commits)',
    last_synced DateTime64(3, 'UTC') COMMENT 'Last sync timestamp'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, pr_number, commit_hash)
COMMENT 'Fast path table for PR to commit relationships';

-- View for commit <-> file relationships (derived from existing git_commit_stats)
-- This avoids duplicating data that already exists in git_commit_stats
CREATE VIEW IF NOT EXISTS work_graph_commit_file AS
SELECT
    repo_id,
    commit_hash,
    file_path,
    additions,
    deletions
FROM git_commit_stats;
