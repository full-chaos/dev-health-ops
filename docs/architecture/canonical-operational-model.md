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

Every entity derives four non-null ordering fields at construction and validates them
again at the shared typed ClickHouse writer:

- `source_revision UInt128` is `(timestamp_us << 64) | (operation_rank << 56) |
  tie56`. Rank is create `0`, active/update `1`, or tombstone `2`; `tie56` is the first
  56 bits of `SHA256("operational-source-revision-v1" || source_conflict_key_bytes)`.
- `source_conflict_key String` is lowercase hex of the injective
  `operational-conflict-v1` TLV. It includes the family and every persisted source or
  business field in dataclass/DDL order except `id`, the four ordering fields,
  `observed_at`, and `last_synced`. Runtime types have distinct tags, including separate
  tags for lists and tuples.
- `ingest_revision UInt128` is `(last_synced_us << 64) | observed_at_us` and controls
  replay compaction only.
- `ordering_contract UInt8` is exactly `2`.

All timestamps are UTC microseconds in the ClickHouse 25.1 `DateTime64(6)` range from the
Unix epoch through `2299-12-31T23:59:59.999999Z`. Invalid ranks, non-UTC or out-of-range
values, and numeric overflow are terminal typed errors. Each table uses
`ReplacingMergeTree(ingest_revision)`, `PRIMARY KEY (org_id, id)`, and
`ORDER BY (org_id, id, source_revision, source_conflict_key)`. Distinct equal-time
candidates therefore remain separate sorting-key rows; identical replays share a key
and compact by `ingest_revision`. Mutable resources retain tombstones through
`is_deleted` and `deleted_at`.

## Service and repository scope

Incidents are organization and service scoped. They never require a `repo_id`.
Repository association is optional and is represented only by
`ServiceRepositoryMapping` or a repository-derived service. This prevents a source
without a repository model from being distorted to fit repository-scoped legacy data.
Resolved mappings store `repo_id` as a UUID. Unresolved mappings require both
`repo_provider` and `repo_full_name`, plus a stable `rule_id` describing the matching
heuristic.

## Storage

Migration `066_operational_canonical.sql` is the immutable legacy baseline. Migration
`067_operational_ordering_contract.py` rebuilds one ClickHouse table per entity:

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

`ClickHouseStore` exposes one `insert_operational_*` method per table. It validates the
derived ordering tuple, table family, and organization before writing dataclass columns.
`OperationalBatch` rejects any row whose organization, provider, or provider instance
differs from its envelope. The work-item `ProviderBatch` remains unchanged.

## Compatibility seam

Later incident-metrics cutover work chooses an explicit domain-time reader:

```python
await store.load_operational_incidents_resolved_between(org_id, start, end)
await store.load_operational_incidents_started_between(org_id, start, end)
await store.load_operational_incidents_overlapping(org_id, start, end)
```

All canonical current-row readers use `current_operational_rows_sql()`. In contract 2 it
scopes the candidate set by `org_id`, then orders by `(org_id, id, source_revision DESC,
source_conflict_key DESC, ingest_revision DESC)` and applies `LIMIT 1 BY org_id, id`.
During the contract-1 bridge, the same seam reads migration-066 tables with legacy
`FINAL` selection, omits the absent v2 columns, and derives their values only after typed
hydration. Tombstone, active, source, and domain-time filters run only after winner
selection in both modes, so an older active row cannot reappear behind a tombstone.
Resolved windows are for MTTR and DORA. Started windows are for incident creation
analysis. Overlap windows are for lifecycle and active-incident analysis.
`load_operational_incidents()` remains a resolved-window compatibility alias. These reads
intentionally coexist with legacy `incidents` and `atlassian_ops_*` readers.

## Ordering-contract rollout and recovery

Omitted `OPERATIONAL_ORDERING_CONTRACT` is rollout-safe contract 1 and keeps migration-066
writers, `FINAL` readers, and schema unchanged. Explicit `1` has the same bridge behavior.
Only an explicit `OPERATIONAL_ORDERING_CONTRACT=2`, set after the maintenance boundary,
admits candidate-preserving writers and makes migration 067 eligible. Any other configured
value fails startup. Omitted or explicit contract 1 defers migration 067 without recording
it as applied.

Before applying migration 067 in a populated environment, quiesce ingress, stop every
write-capable and canonical-reader replica, and drain queued work. The migration streams
every surviving raw row in candidate-grouping order without a collapsing read, derives the
v2 tuple through the same builder, and retains only scalar candidate/logical counts plus one
maximum tuple in Python memory. ClickHouse-side aggregates verify those values before the
atomic shadow exchange. The migration resumes safely from an exchanged leftover shadow.
Restart only replicas whose configured contract matches the stored table contract. An
explicit contract-1 bridge presented with a v2 table fails admission with
`operational_old_writer_rejected`; its bounded log fields are only `table`, `service`,
and `version`.

The raw migration cannot recreate conflicting facts already removed by a legacy
`ReplacingMergeTree(source_version_at)` merge. After the swap, schedule an authoritative
full resync for every provider/source represented in the canonical tables before source
cutover. Record that resync with the migration evidence. Rollback uses the same bridge
binary in contract-2 mode; never lower the insert constraint or restart an original
contract-1 binary.
## Producer migration rollout

CHAOS-2963 adds an opt-in, additive producer seam for GitHub and GitLab
issue-derived incidents and for historical Atlassian Ops rows. Set
`OPERATIONAL_INCIDENT_DUAL_WRITE=true` to write canonical rows alongside the
legacy `incidents` write; this flag never suppresses the legacy write.

`providers.operational_migration` maps GitHub and GitLab records with
`source_entity_type="issue"`, a repository-derived `OperationalService`, and a
`ServiceRepositoryMapping`. The incident has no `repo_id`; repository linkage is
the explicit mapping edge. Atlassian Ops backfill maps its legacy incidents,
alerts, and schedules with `provider="atlassian"` and their native source entity
types. Deterministic canonical ids make repeated source snapshots idempotent.

The migration does not change incident-correlation, Sankey, DORA, MTTR, or
deployment-edge consumers. Those consumers continue reading legacy tables until
their explicit cutover gate. Historical migration runs through
`dev-health-ops backfill operational --org <org-id>` and joins `incidents` to
`repos` on `repo_id` before mapping GitHub/GitLab issue incidents. The CLI accepts
explicit provider-instance ids because the legacy incident row does not carry
instance provenance. Atlassian Ops incidents, alerts, and schedules are read from
their legacy tables and mapped through the same canonical writer.

Historical GitHub/GitLab rows retain status and lifecycle timestamps, but legacy
`incidents` has no labels, issue URL, number, title, or description; those canonical
fields are null or empty after backfill. Enabled live dual-write supplies that richer
issue metadata going forward. The backfill's deterministic canonical ids make
repeated runs idempotent under the centralized current-row selector.
