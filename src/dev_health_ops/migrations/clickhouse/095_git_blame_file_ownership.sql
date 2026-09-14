-- Per-file blame ownership for the file_risk_hotspots family.
--
-- The hotspot reader needs two numbers per file: the line count of the file's
-- dominant author and the count of lines with any author. Computing them from
-- git_blame on every call aggregates every blamed line of the repository.
-- git_blame_file_ownership stores those two numbers per file instead, and the
-- reader keeps it current by recomputing only the files written since their
-- last recompute.
--
-- git_blame_dirty_paths is that change set. Its materialized view stores
-- keys only -- never a count or a sum -- so a re-synced file that arrives in
-- several INSERT blocks marks the same key again rather than adding a second
-- aggregate (the per-block double count that removed the daily rollups in
-- 093). Every git_blame writer feeds it without any writer change.
--
-- The view is created before the backfill so no row written during the
-- migration is missed, and a key both paths mark collapses in the
-- ReplacingMergeTree.

CREATE TABLE IF NOT EXISTS git_blame_file_ownership (
    org_id String,
    repo_id UUID,
    path String,
    owner_lines UInt64,
    attributed_lines UInt64,
    settled_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(settled_at)
ORDER BY (org_id, repo_id, path);

CREATE TABLE IF NOT EXISTS git_blame_dirty_paths (
    org_id String,
    repo_id UUID,
    path String,
    marked_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(marked_at)
ORDER BY (org_id, repo_id, path);

CREATE MATERIALIZED VIEW IF NOT EXISTS git_blame_dirty_paths_mv
TO git_blame_dirty_paths
AS SELECT
    org_id,
    repo_id,
    path,
    now64(3, 'UTC') AS marked_at
FROM git_blame
GROUP BY org_id, repo_id, path;

-- Backfill: every file already in git_blame starts dirty, so the first
-- hotspot call per repository settles it once.
INSERT INTO git_blame_dirty_paths (org_id, repo_id, path, marked_at)
SELECT
    org_id,
    repo_id,
    path,
    now64(3, 'UTC') AS marked_at
FROM git_blame
GROUP BY org_id, repo_id, path;
