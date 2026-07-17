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
optional. `source_id` identifies external-push provenance when present. The source
event, source version, observation, and sync timestamps are represented by
`source_event_at`, `source_version_at`, `observed_at`, and `last_synced`.

Every entity preserves `raw_status`, `raw_severity`, and `raw_priority` alongside
`normalized_status`, `normalized_severity`, and `normalized_priority`. Relationships
may carry `relationship_provenance` and `relationship_confidence`; mappings must use
these fields to make a derived service-to-repository edge auditable. Timeline and note
text are untrusted evidence, never executable instructions.

## Identity and idempotency

`id` is the deterministic SHA-256 digest of this fixed seed:

```text
(org_id, provider, provider_instance_id, entity_family, external_id)
```

`entity_family` is a table-derived constant such as `operational_incident`; the raw
`source_entity_type` is descriptive only and never changes identity. Every seed part
is required and case-sensitive. This identifier deliberately excludes mutable labels,
status, URLs, and external-push `source_id`.

Every table uses `ReplacingMergeTree(source_version_at)` with `(org_id, id)` as its
sorting and idempotency key. Providers set `source_version_at` from their update or
webhook event time, or from a snapshot start time when no source update time exists.
`last_synced` is ingestion observability only. Equal source versions may be duplicated
only when their semantic payload is identical; conflicting equal-version payloads must
be quarantined by the producer rather than relying on engine tie selection. Mutable
resources retain tombstones through `is_deleted` and `deleted_at`.

## Service and repository scope

Incidents are organization and service scoped. They never require a `repo_id`.
Repository association is optional and is represented only by
`ServiceRepositoryMapping` or a repository-derived service. This prevents a source
without a repository model from being distorted to fit repository-scoped legacy data.
Resolved mappings store `repo_id` as a UUID. Unresolved mappings require both
`repo_provider` and `repo_full_name`, plus a stable `rule_id` describing the matching
heuristic.

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
dataclass columns without an adapter and rejects rows whose `org_id` conflicts with
the store context. `OperationalBatch` rejects any row whose organization, provider, or
provider instance differs from its envelope. The work-item `ProviderBatch` remains
unchanged.

## Compatibility seam

Later incident-metrics cutover work chooses an explicit domain-time reader:

```python
await store.load_operational_incidents_resolved_between(org_id, start, end)
await store.load_operational_incidents_started_between(org_id, start, end)
await store.load_operational_incidents_overlapping(org_id, start, end)
```

All readers query `operational_incidents FINAL`, scope by `org_id`, and exclude
tombstones. Resolved windows are for MTTR and DORA. Started windows are for incident
creation analysis. Overlap windows are for lifecycle and active-incident analysis.
`load_operational_incidents()` remains a resolved-window compatibility alias. These
reads intentionally coexist with legacy `incidents` and `atlassian_ops_*` readers.
CHAOS-2963 migrates producers and metric consumers without changing these guarantees.
