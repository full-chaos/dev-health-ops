-- work_graph_deployment_incident_edges is keyed by the repository and has one
-- row for each (repository, deployment, incident).
--
-- The old key (org_id, deployment_id, incident_id, source) left the repository
-- out. deployment_id is not unique across repositories, so two repositories that
-- run one deployment id and share an incident wrote one key: the later row
-- replaced the earlier one, and a native row and a heuristic row for one pair
-- stood side by side. The new key is (org_id, ifNull(repo_id, zero uuid),
-- deployment_id, incident_id), with no source: a pair is one row. The row with the higher link
-- tier is kept, whatever its time (version_rank, the same rule as
-- work_graph_issue_pr: native, then explicit_text, then heuristic; within a tier
-- the newer computed_at). Inferred links are a fallback only.
--
-- repo_id stays nullable: a row without one keeps one key (the zero uuid in the
-- sort key through ifNull), as before. PARTITION BY is dropped: a pair whose
-- observed_at moved to another month sat in another partition, where a
-- replacing merge tree cannot replace it.
--
-- ClickHouse cannot change a sort key in place, so the table is rebuilt and
-- swapped. Every statement can run again: the first drops a half-built copy, and
-- a run after the swap rebuilds the table from itself. Rows an older build wrote
-- between the copy and the swap are not copied; the daily job writes them again.
DROP TABLE IF EXISTS work_graph_deployment_incident_edges_new;

CREATE TABLE work_graph_deployment_incident_edges_new
(
    edge_id        String,
    org_id         UUID,
    deployment_id  String,
    incident_id    String,
    provider       LowCardinality(String),
    repo_id        Nullable(UUID),
    confidence     Float32,
    source         LowCardinality(String),
    evidence       String,
    observed_at    DateTime64(3, 'UTC'),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64(),
    version_rank   UInt64 MATERIALIZED ((multiIf(source = 'native', 3, source = 'explicit_text', 2, source = 'heuristic', 1, 0) + 1) * 1125899906842624) + toUnixTimestamp64Milli(computed_at)
)
ENGINE = ReplacingMergeTree(version_rank)
ORDER BY (org_id, ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), deployment_id, incident_id)
SETTINGS index_granularity = 8192;

INSERT INTO work_graph_deployment_incident_edges_new
    (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
SELECT edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at
FROM work_graph_deployment_incident_edges FINAL;

EXCHANGE TABLES work_graph_deployment_incident_edges AND work_graph_deployment_incident_edges_new;

DROP TABLE IF EXISTS work_graph_deployment_incident_edges_new;
