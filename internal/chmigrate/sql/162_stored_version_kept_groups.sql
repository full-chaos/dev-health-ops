-- A value a writer stated is not lost when another writer of the same key inserts an older-read row at a newer version.
--
--   git_pull_request_kept   one state row per insert block per (org_id, repo_id, number), merged by AggregatingMergeTree
--   deployment_kept         one state row per insert block per (org_id, repo_id, deployment_id)
--
-- Each kept group is a set of columns a writer states together, led by a terminal column (it cannot un-happen):
--   git_pull_requests.merged = (merged_at)
--   git_pull_requests.review = (first_review_at, changes_requested_count, reviews_count)
--   deployments.merged       = (merged_at, pull_request_number, pull_request_tier)
-- The state is argMaxIf(group, last_synced, lead IS NOT NULL): the group of the newest version that states the lead. The
-- view sees every block inserted into the base table, before a merge removes an older version, so a statement that a newer
-- version carried over is still in the state. Read it with argMaxIfMerge(...) GROUP BY the key, never FINAL alone.
--
-- After each write, the writer repairs its keys (internal/storedversion): where FINAL holds the lead NULL and the state holds
-- it non-null, it inserts the FINAL row with that group from the state, at last_synced + 1 ms. A non-null lead is never
-- replaced. The last statement is the same repair over every key, once, so the state and FINAL agree from the start.
--
-- The base tables do not change. Every statement can run again: CREATE ... IF NOT EXISTS; the backfill reads the base table
-- without FINAL, so a version not merged yet still feeds the state, and a second state of the same version is the same
-- argMax; the repair finds nothing to do the second time.
CREATE TABLE IF NOT EXISTS git_pull_request_kept
(
    org_id String,
    repo_id UUID,
    number UInt32,
    merged AggregateFunction(argMaxIf, Tuple(Nullable(DateTime64(3, 'UTC'))), DateTime64(3, 'UTC'), UInt8),
    review AggregateFunction(argMaxIf, Tuple(Nullable(DateTime64(3, 'UTC')), UInt32, UInt32), DateTime64(3, 'UTC'), UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (org_id, repo_id, number);

CREATE TABLE IF NOT EXISTS deployment_kept
(
    org_id String,
    repo_id UUID,
    deployment_id String,
    merged AggregateFunction(argMaxIf, Tuple(Nullable(DateTime64(3, 'UTC')), Nullable(UInt32), String), DateTime64(3, 'UTC'), UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (org_id, repo_id, deployment_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS git_pull_request_kept_mv TO git_pull_request_kept
AS SELECT org_id, repo_id, number,
    argMaxIfState(tuple(merged_at), last_synced, merged_at IS NOT NULL) AS merged,
    argMaxIfState(tuple(first_review_at, changes_requested_count, reviews_count), last_synced, first_review_at IS NOT NULL) AS review
FROM git_pull_requests
GROUP BY org_id, repo_id, number;

CREATE MATERIALIZED VIEW IF NOT EXISTS deployment_kept_mv TO deployment_kept
AS SELECT org_id, repo_id, deployment_id,
    argMaxIfState(tuple(merged_at, pull_request_number, CAST(pull_request_tier, 'String')), last_synced, merged_at IS NOT NULL) AS merged
FROM deployments
GROUP BY org_id, repo_id, deployment_id;

INSERT INTO git_pull_request_kept
SELECT org_id, repo_id, number,
    argMaxIfState(tuple(merged_at), last_synced, merged_at IS NOT NULL) AS merged,
    argMaxIfState(tuple(first_review_at, changes_requested_count, reviews_count), last_synced, first_review_at IS NOT NULL) AS review
FROM git_pull_requests
GROUP BY org_id, repo_id, number;

INSERT INTO deployment_kept
SELECT org_id, repo_id, deployment_id,
    argMaxIfState(tuple(merged_at, pull_request_number, CAST(pull_request_tier, 'String')), last_synced, merged_at IS NOT NULL) AS merged
FROM deployments
GROUP BY org_id, repo_id, deployment_id;

INSERT INTO git_pull_requests
SELECT * EXCEPT (kept_fill_merged, kept_merged, kept_fill_review, kept_review) REPLACE (
    if(kept_fill_merged, kept_merged.1, merged_at) AS merged_at,
    if(kept_fill_review, kept_review.1, first_review_at) AS first_review_at,
    if(kept_fill_review, kept_review.2, changes_requested_count) AS changes_requested_count,
    if(kept_fill_review, kept_review.3, reviews_count) AS reviews_count,
    last_synced + toIntervalMillisecond(1) AS last_synced)
FROM (
    SELECT base.*, kept.kept_merged, kept.kept_review,
        base.merged_at IS NULL AND kept.kept_merged.1 IS NOT NULL AS kept_fill_merged,
        base.first_review_at IS NULL AND kept.kept_review.1 IS NOT NULL AS kept_fill_review
    FROM (SELECT * FROM git_pull_requests FINAL) AS base
    INNER JOIN (
        SELECT org_id, repo_id, number, argMaxIfMerge(merged) AS kept_merged, argMaxIfMerge(review) AS kept_review
        FROM git_pull_request_kept
        GROUP BY org_id, repo_id, number
    ) AS kept ON base.org_id = kept.org_id AND base.repo_id = kept.repo_id AND base.number = kept.number
    WHERE kept_fill_merged OR kept_fill_review
);

INSERT INTO deployments
SELECT * EXCEPT (kept_fill_merged, kept_merged) REPLACE (
    if(kept_fill_merged, kept_merged.1, merged_at) AS merged_at,
    if(kept_fill_merged, kept_merged.2, pull_request_number) AS pull_request_number,
    if(kept_fill_merged, kept_merged.3, pull_request_tier) AS pull_request_tier,
    last_synced + toIntervalMillisecond(1) AS last_synced)
FROM (
    SELECT base.*, kept.kept_merged,
        base.merged_at IS NULL AND kept.kept_merged.1 IS NOT NULL AS kept_fill_merged
    FROM (SELECT * FROM deployments FINAL) AS base
    INNER JOIN (
        SELECT org_id, repo_id, deployment_id, argMaxIfMerge(merged) AS kept_merged
        FROM deployment_kept
        GROUP BY org_id, repo_id, deployment_id
    ) AS kept ON base.org_id = kept.org_id AND base.repo_id = kept.repo_id AND base.deployment_id = kept.deployment_id
    WHERE kept_fill_merged
);
