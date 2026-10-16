-- Rollback of 132_pr_commit_native_wins.sql (CHAOS-9572). It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not rely on version_rank.
--
-- The table goes back to ReplacingMergeTree(last_synced). Every surviving row
-- is copied. The version row goes too: with it left in schema_migrations, a
-- later `dho migrate clickhouse upgrade` on a build that has the migration
-- reads it as applied and never rebuilds the table.
DROP TABLE IF EXISTS work_graph_pr_commit_old;

CREATE TABLE work_graph_pr_commit_old
(
    `repo_id` UUID COMMENT 'Repository UUID',
    `pr_number` UInt32 COMMENT 'Pull request number',
    `commit_hash` String COMMENT 'Git commit SHA',
    `confidence` Float32 COMMENT 'Confidence score 0.0-1.0',
    `provenance` String COMMENT 'How link was discovered: native|explicit_text|heuristic',
    `evidence` String COMMENT 'Source of link (e.g., api_pr_commits)',
    `last_synced` DateTime64(3, 'UTC') COMMENT 'Last sync timestamp',
    `org_id` String DEFAULT 'default'
)
ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, pr_number, commit_hash)
SETTINGS index_granularity = 8192
COMMENT 'Fast path table for PR to commit relationships';

INSERT INTO work_graph_pr_commit_old
    (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)
SELECT repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id
FROM work_graph_pr_commit FINAL;

EXCHANGE TABLES work_graph_pr_commit AND work_graph_pr_commit_old;

DROP TABLE IF EXISTS work_graph_pr_commit_old;

ALTER TABLE schema_migrations DELETE WHERE version = '132_pr_commit_native_wins.sql' SETTINGS mutations_sync = 2;
