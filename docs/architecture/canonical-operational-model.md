# Canonical Operational Model

## Purpose

The canonical operational model is the ClickHouse-only contract for provider-neutral
services, incidents, alerts, on-call data, and their evidence. It is the durable seam
between provider ingestion and operational metrics. It does not fetch provider APIs,
normalize provider payloads, or replace legacy producers.

## Entity contract

The foundation defines these entities:

- `OperationalService`
- `OperationalIncident`
- `OperationalAlert`
- `IncidentTimelineEvent`
- `IncidentNote`
- `IncidentResponder`
- `EscalationPolicy`
- `OnCallSchedule`
- `OnCallAssignment`
- `OperationalTeam`
- `OperationalUser`
- `ServiceRepositoryMapping`

Every row carries its tenant and source identity: `org_id`, `provider`,
`provider_instance_id`, `source_entity_type`, and `external_id`. `source_url` is
optional. The source event, observation, and sync timestamps are represented by
`source_event_at`, `observed_at`, and `last_synced`.

Every entity preserves `raw_status`, `raw_severity`, and `raw_priority` alongside
`normalized_status`, `normalized_severity`, and `normalized_priority`. Relationships
may carry `relationship_provenance` and `relationship_confidence`; mappings must use
these fields to make a derived service-to-repository edge auditable. Timeline and note
text are untrusted evidence, never executable instructions.

## Identity and idempotency

`id` is the deterministic SHA-256 digest of this fixed seed:

```text
(org_id, provider_instance_id, source_entity_type, external_id)
```

This identifier deliberately excludes mutable labels, status, and URLs. Every table
uses `ReplacingMergeTree(last_synced)` with `(org_id, id)` as its sorting and
idempotency key. Rewrites of the same source identity converge to the latest sync
version. Services and incidents retain tombstones through `is_deleted` and
`deleted_at`; compatibility reads hide deleted incidents.

## Service and repository scope

Incidents are organization and service scoped. They never require a `repo_id`.
Repository association is optional and is represented only by
`ServiceRepositoryMapping` or a repository-derived service. This prevents a source
without a repository model from being distorted to fit repository-scoped legacy data.

## Storage

Migration `066_operational_canonical.sql` creates one ClickHouse table per entity:

| Entity | Table |
| --- | --- |
| `OperationalService` | `operational_services` |
| `OperationalIncident` | `operational_incidents` |
| `OperationalAlert` | `operational_alerts` |
| `IncidentTimelineEvent` | `operational_incident_timeline_events` |
| `IncidentNote` | `operational_incident_notes` |
| `IncidentResponder` | `operational_incident_responders` |
| `EscalationPolicy` | `operational_escalation_policies` |
| `OnCallSchedule` | `operational_on_call_schedules` |
| `OnCallAssignment` | `operational_on_call_assignments` |
| `OperationalTeam` | `operational_teams` |
| `OperationalUser` | `operational_users` |
| `ServiceRepositoryMapping` | `operational_service_repository_mappings` |

`ClickHouseStore` exposes one `insert_operational_*` method per table. It writes
dataclass columns without an adapter. Canonical entity constructors require `org_id`
so the stored tenant boundary always matches the deterministic identity seed.
`OperationalBatch` is the separate ingestion envelope for these entities; the
work-item `ProviderBatch` remains unchanged.

## Compatibility seam

Later incident-metrics cutover work calls:

```python
await store.load_operational_incidents(org_id, start, end)
```

The reader queries `operational_incidents FINAL`, scopes by `org_id`, applies the
event-or-observation window, and excludes tombstones. It intentionally coexists with
the legacy `incidents` and `atlassian_ops_*` readers. CHAOS-2963 migrates producers
to this contract and changes metric consumers; that work must not alter the identity,
table, or compatibility guarantees documented here.
