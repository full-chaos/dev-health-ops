-- Rollback of 131_deployment_incident_edges_repo_key.sql (CHAOS-9793). It is NOT
-- part of the chain (the migrator reads sql/ only); an operator runs it by hand
-- after the writers and the readers are back on a build that reads the old key.
--
-- The table goes back to the key (org_id, deployment_id, incident_id, source),
-- with repo_id nullable. Every row is copied; a pair that has one row now keeps
-- that row. The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it as
-- applied and never rebuilds the key.
DROP TABLE IF EXISTS work_graph_deployment_incident_edges_old;

CREATE TABLE work_graph_deployment_incident_edges_old
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
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, deployment_id, incident_id, source)
SETTINGS index_granularity = 8192;

INSERT INTO work_graph_deployment_incident_edges_old
    (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
SELECT edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at
FROM work_graph_deployment_incident_edges FINAL;

EXCHANGE TABLES work_graph_deployment_incident_edges AND work_graph_deployment_incident_edges_old;

DROP TABLE IF EXISTS work_graph_deployment_incident_edges_old;

ALTER TABLE schema_migrations DELETE WHERE version = '131_deployment_incident_edges_repo_key.sql' SETTINGS mutations_sync = 2;
