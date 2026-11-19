-- The input evidence of the derived team_repo_ownership rows (source inferred).
--
--   team_repo_ownership_evidence   one row per (org_id, provider, repo_full_name, team_id, input, input_key)
--
-- The team_repo_ownership derivation records, for every claim it derives, the key of each input row of the chains that gave
-- the claim a link: input names the table (work_item, dependency, issue_pr, project_link, linear_team) and input_key is the
-- row's key, a JSON array of its key columns without org_id. A claim the derivation no longer derives is closed only when every
-- recorded key of it is in the run: a missing input row holds the close.
--
-- seen_at is the version: the newest version of a key wins, and is_deleted = 1 retires the key. A reader takes the newest
-- version of each key first (argMax over (seen_at, is_deleted), or FINAL), then filters is_deleted = 0 on that row, never on the
-- stored rows.
--
-- CREATE TABLE IF NOT EXISTS: the statement can run again.
CREATE TABLE IF NOT EXISTS team_repo_ownership_evidence
(
    `org_id` String,
    `provider` String,
    `repo_full_name` String,
    `team_id` String,
    `input` LowCardinality(String),
    `input_key` String,
    `seen_at` DateTime64(3, 'UTC'),
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(seen_at)
ORDER BY (org_id, provider, repo_full_name, team_id, input, input_key);
