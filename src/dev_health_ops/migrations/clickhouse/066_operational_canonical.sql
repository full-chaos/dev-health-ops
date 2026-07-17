CREATE TABLE IF NOT EXISTS operational_services (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    name String, description Nullable(String), service_type Nullable(String), owning_team_id Nullable(String),
    escalation_policy_id Nullable(String), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_incidents (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    service_id Nullable(String), service_external_id Nullable(String), escalation_policy_id Nullable(String),
    title String, description Nullable(String), started_at Nullable(DateTime64(6, 'UTC')),
    resolved_at Nullable(DateTime64(6, 'UTC')), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_alerts (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    service_id Nullable(String), incident_id Nullable(String), title String, description Nullable(String),
    triggered_at Nullable(DateTime64(6, 'UTC')), acknowledged_at Nullable(DateTime64(6, 'UTC')),
    resolved_at Nullable(DateTime64(6, 'UTC')), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_incident_timeline_events (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    incident_id String, event_type String, body Nullable(String), actor_type Nullable(String), actor_id Nullable(String),
    occurred_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_incident_notes (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    incident_id String, body String, author_user_id Nullable(String), created_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_incident_responders (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    incident_id String, user_id Nullable(String), responder_name Nullable(String), role Nullable(String),
    responder_assignment_id Nullable(String), requested_at Nullable(DateTime64(6, 'UTC')),
    assigned_at Nullable(DateTime64(6, 'UTC')), acknowledged_at Nullable(DateTime64(6, 'UTC')),
    completed_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_escalation_policies (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    name String, description Nullable(String), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_on_call_schedules (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    name String, description Nullable(String), timezone Nullable(String), is_deleted UInt8,
    deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_on_call_assignments (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    schedule_id Nullable(String), user_id Nullable(String), escalation_policy_id Nullable(String),
    escalation_level Nullable(Int32), starts_at Nullable(DateTime64(6, 'UTC')), ends_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_teams (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    name String, description Nullable(String), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_users (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    display_name String, email Nullable(String), is_deleted UInt8, deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);

CREATE TABLE IF NOT EXISTS operational_service_repository_mappings (
    org_id String,
    provider LowCardinality(String),
    provider_instance_id String,
    source_entity_type LowCardinality(String),
    external_id String,
    source_version_at DateTime64(6, 'UTC'),
    id String,
    source_id Nullable(UUID),
    source_url Nullable(String),
    source_event_at Nullable(DateTime64(6, 'UTC')),
    source_event_id Nullable(String),
    observed_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC'),
    raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
    normalized_status Nullable(String), normalized_severity Nullable(String), normalized_priority Nullable(String),
    relationship_provenance Nullable(String), relationship_confidence Nullable(Float64),
    service_id String, repo_id Nullable(UUID), repo_full_name Nullable(String), repo_provider Nullable(String),
    mapping_kind Nullable(String), rule_id Nullable(String), valid_from Nullable(DateTime64(6, 'UTC')),
    valid_to Nullable(DateTime64(6, 'UTC')), is_active UInt8
) ENGINE = ReplacingMergeTree(source_version_at)
ORDER BY (org_id, id);
