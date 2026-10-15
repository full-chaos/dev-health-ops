-- work_graph_pr_commit keeps the link of the highest tier for a pair, whatever
-- the sync order (a sync race).
--
-- Limit: a native row the old engine already merged away is gone, and a
-- migration cannot restore it. A provider re-sync re-emits it (see the PR).
--
-- The table was ReplacingMergeTree(last_synced). The native writer (provider
-- sync) and the text tier (prcommit.ProduceLinks) both stamp their rows with
-- the time of their own run, so a text row written after a native row for the
-- same pair won the replace, and the served link read explicit_text instead of
-- native. The row with the higher link tier is now kept, whatever its time
-- (version_rank, the same rule as work_graph_issue_pr and
-- work_graph_deployment_incident_edges: native, then explicit_text, then
-- heuristic, and within a tier the newer last_synced). Native wins by the ruling of
-- the deployment-incident edges work: inferred links are a fallback only.
--
-- The sort key does not change. ClickHouse cannot change the version column of
-- a replacing merge tree in place, so the table is rebuilt as one
-- INSERT ... SELECT and swapped. The copy reads every physical row, not FINAL,
-- so the new table's own version_rank picks the survivor of a pair. After the
-- swap the old table is copied into the new one once more, so a row an older
-- build wrote between the first copy and the swap is kept. Every statement can
-- run again: the first drops a half-built copy, and a run after the swap
-- rebuilds the table from itself.
DROP TABLE IF EXISTS work_graph_pr_commit_new;

CREATE TABLE work_graph_pr_commit_new
(
    `repo_id` UUID COMMENT 'Repository UUID',
    `pr_number` UInt32 COMMENT 'Pull request number',
    `commit_hash` String COMMENT 'Git commit SHA',
    `confidence` Float32 COMMENT 'Confidence score 0.0-1.0',
    `provenance` String COMMENT 'How link was discovered: native|explicit_text|heuristic',
    `evidence` String COMMENT 'Source of link (e.g., api_pr_commits)',
    `last_synced` DateTime64(3, 'UTC') COMMENT 'Last sync timestamp',
    `org_id` String DEFAULT 'default',
    `version_rank` UInt64 MATERIALIZED ((multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2, provenance = 'heuristic', 1, 0) + 1) * 1125899906842624) + toUnixTimestamp64Milli(last_synced)
)
ENGINE = ReplacingMergeTree(version_rank)
ORDER BY (org_id, repo_id, pr_number, commit_hash)
SETTINGS index_granularity = 8192
COMMENT 'Fast path table for PR to commit relationships';

INSERT INTO work_graph_pr_commit_new
    (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)
SELECT repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id
FROM work_graph_pr_commit;

EXCHANGE TABLES work_graph_pr_commit AND work_graph_pr_commit_new;

INSERT INTO work_graph_pr_commit
    (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)
SELECT repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id
FROM work_graph_pr_commit_new;

DROP TABLE IF EXISTS work_graph_pr_commit_new;
