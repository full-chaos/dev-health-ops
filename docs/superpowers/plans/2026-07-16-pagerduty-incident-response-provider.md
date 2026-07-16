# PagerDuty Incident-Response Provider Integration

**Linear source of truth:** [CHAOS-2954](https://linear.app/fullchaos/issue/CHAOS-2954/pagerduty-incident-response-provider-integration)  
**Project:** Dev Health Ops  
**Milestone:** PagerDuty Incident Response Integration  
**Status:** Planning / implementation-ready

## Executive summary

Add PagerDuty as the first non-Atlassian incident-response provider in Dev Health Ops.

The implementation must not create a PagerDuty-specific analytics silo. PagerDuty should normalize into a provider-neutral operational model shared by Atlassian/JSM Operations and future providers such as incident.io, FireHydrant, Rootly, and ServiceNow.

V1 is read-only:

- REST API backfill and incremental reconciliation;
- PagerDuty V3 webhook updates;
- services, business services, incidents, alerts, timeline/log entries, notes, escalation policies, schedules, on-call assignments, users, and teams;
- explicit service-to-repository mappings;
- existing work-graph and incident-correlation integration;
- canonical provider setup, credential preflight, sync/backfill state, tests, docs, and observability.

Do not add PagerDuty incident mutation, acknowledgement, reassignment, resolution, schedule mutation, Event Orchestration management, or status-page publishing in V1.

## Grounded current state

### Atlassian coverage already exists

The custom [`full-chaos/atlassian`](https://github.com/full-chaos/atlassian) library already includes:

- Jira REST and Atlassian GraphQL Gateway clients;
- generated API models from live schema introspection;
- canonical Jira, Compass, Teams, and Teamwork Graph models;
- Compass components, relationships, and scorecards;
- Atlassian Teams and memberships;
- Teamwork Graph generated API support;
- Opsgenie/JSM-oriented project and team relationships.

`dev-health-ops` already imports that library through:

```text
src/dev_health_ops/providers/jira/atlassian_compat.py
src/dev_health_ops/providers/jira/provider.py
```

This plan therefore does **not** add or replace Atlassian Teamwork Graph coverage.

### Existing operational storage is provider-specific

Current Atlassian operational entities live in:

```text
src/dev_health_ops/models/atlassian_ops.py
src/dev_health_ops/storage/mixins/atlassian_ops.py
src/dev_health_ops/migrations/clickhouse/021_atlassian_ops_tables.sql
```

Current tables:

```text
atlassian_ops_incidents
atlassian_ops_alerts
atlassian_ops_schedules
```

These tables are insufficient as the permanent cross-provider contract because they:

- are named for one provider family;
- do not express the full service, escalation, responder, timeline, and on-call graph;
- do not provide a provider-neutral identity contract;
- are not the appropriate destination for PagerDuty data.

### The existing generic incident row is too narrow

`src/dev_health_ops/models/git.py` defines a repo-scoped `Incident` with:

```text
repo_id
incident_id
status
started_at
resolved_at
last_synced
```

This works for GitHub/GitLab incident-like issues, but PagerDuty incidents belong to operational services and may map to zero, one, or many repositories.

PagerDuty incidents must not be forced into a synthetic repository merely to satisfy the current primary key.

### Current provider contract is work-item oriented

`src/dev_health_ops/providers/base.py` defines `ProviderBatch` around work items, transitions, dependencies, comments, sprints, worklogs, and AI attribution.

PagerDuty needs a separate operational ingestion envelope or a broader canonical provider contract. Do not overload the current work-item batch with loosely typed incident fields.

### Existing incident correlation should be reused

GitHub and GitLab processors already create repo-scoped incidents from labeled issues. Existing work-graph and incident-correlation paths include deployment-to-incident relationships.

PagerDuty must feed those canonical analytics and graph surfaces through compatibility queries/edges rather than create a new PagerDuty-only dashboard.

## Product and architecture decisions

### 1. Provider-neutral operational ontology

Define the following canonical entities:

```text
OperationalService
OperationalIncident
OperationalAlert
IncidentTimelineEvent
IncidentNote
IncidentResponder
EscalationPolicy
OnCallSchedule
OnCallAssignment
OperationalTeam
OperationalUser
ServiceRepositoryMapping
```

All canonical entities must include:

- `org_id`;
- provider name;
- provider instance or integration-source identity;
- stable source external ID;
- deterministic internal identity;
- source URL where available;
- source event timestamp, observed timestamp, and `last_synced`;
- raw source status/severity/priority values;
- canonical normalized status/severity/priority;
- provenance and confidence for derived relationships.

### 2. Repository scope is a relationship

Repositories are connected to operational services using `ServiceRepositoryMapping`.

Supported mapping sources:

1. explicit admin configuration;
2. exact repository URL or slug from service metadata/integrations;
3. Compass/service-catalog relationships;
4. bounded heuristics, clearly marked with lower confidence.

A service may map to multiple repositories. A repository may map to multiple services.

### 3. REST reconciles; webhooks reduce latency

```text
PagerDuty REST API -----------┐
                              ├──> provider normalization
PagerDuty V3 webhooks --------┘              │
                                             ▼
                              canonical operational entities
```

REST sync is authoritative for reconciliation and backfill. V3 webhooks provide low-latency updates but must not be the only source of truth.

### 4. No direct metric-table writes

Provider ingestion writes source entities through canonical sinks. Existing metric jobs and work-graph builders derive analytics and relationships.

### 5. Read-only V1

No PagerDuty write/mutation methods are added to the client or UI in V1.

## PagerDuty REST API coverage

Initial read coverage:

```text
GET /incidents
GET /incidents/{id}
GET /incidents/{id}/alerts
GET /incidents/{id}/log_entries
GET /incidents/{id}/notes
GET /services
GET /business_services
GET /escalation_policies
GET /schedules
GET /oncalls
GET /users
GET /teams
```

Required request behavior:

- `Accept: application/vnd.pagerduty+json;version=2`;
- API token and scoped OAuth support;
- offset/limit pagination and `more` handling;
- bounded request concurrency and timeouts;
- route-family provider usage observations;
- parse `ratelimit-limit`, `ratelimit-remaining`, and `ratelimit-reset`;
- bounded retry after `429` using PagerDuty reset guidance;
- no retry for permanent 4xx failures;
- dataset-specific degradation for missing plan/permission coverage.

Official references:

- [PagerDuty REST API rate limits](https://support.pagerduty.com/main/docs/rest-api-rate-limits)
- [PagerDuty Webhooks](https://support.pagerduty.com/main/docs/webhooks)
- [PagerDuty public API collection](https://www.postman.com/pagerduty/pagerduty-public-api-collection/overview)

## PagerDuty V3 webhook coverage

Incident events:

```text
incident.triggered
incident.acknowledged
incident.unacknowledged
incident.escalated
incident.reassigned
incident.delegated
incident.priority_updated
incident.resolved
incident.reopened
incident.annotated
incident.responder.added
incident.responder.replied
incident.service_updated
incident.status_update_published
```

Service events:

```text
service.created
service.updated
service.deleted
```

Webhook requirements:

- verify `x-pagerduty-signature`;
- store secrets through the encrypted credential/configuration boundary;
- validate event type and V3 payload shape;
- deduplicate by provider instance plus PagerDuty event ID;
- protect against replay with a bounded event-ID retention window;
- preserve `occurred_at`, received time, and processed time separately;
- durable asynchronous processing;
- targeted REST hydration when payloads omit required state;
- out-of-order events must not regress a newer canonical state;
- REST reconciliation repairs dropped events;
- service deletion uses tombstone/disabled semantics.

Do not support V1/V2 webhook extensions.

## Canonical mapping direction

### Operational service

PagerDuty sources:

- service;
- business service;
- teams;
- escalation policy;
- integration metadata;
- service dependencies/impact relationships when available.

Canonical fields should retain:

- service kind (`technical`, `business`, or source-specific unknown);
- name/summary/description;
- lifecycle state;
- escalation-policy reference;
- team references;
- source URL;
- provider metadata needed for mapping without exposing credentials.

### Operational incident

PagerDuty sources:

- incident object;
- service reference;
- assignments;
- escalation policy;
- urgency and priority;
- status and lifecycle timestamps;
- acknowledgement and resolution data;
- title/summary/description;
- incident type/custom fields when available and permitted.

Canonical state should distinguish at minimum:

```text
triggered
acknowledged
resolved
reopened
unknown
```

Retain the PagerDuty raw status and urgency separately.

### Alert

Alerts remain distinct from incidents. Preserve:

- alert ID and deduplication key;
- status;
- severity/priority;
- creation and resolution timestamps;
- service and incident references;
- source summary/details where safe.

### Timeline event

PagerDuty log entries normalize into typed timeline events:

- trigger;
- acknowledgement;
- escalation;
- assignment/reassignment;
- responder action;
- notification/channel action;
- resolution/reopen;
- note/status update;
- workflow start/completion;
- source-specific unknown.

Store actor/reference/channel metadata as structured data, with raw event type retained.

### Notes and retrieved content

Incident notes are source evidence and untrusted text. They are never interpreted as system/tool instructions.

## Storage and migration strategy

### Preferred direction

Create provider-neutral tables using current ClickHouse/PostgreSQL ownership conventions.

Candidate table family:

```text
operational_services
operational_incidents
operational_alerts
incident_timeline_events
incident_notes
incident_responders
operational_escalation_policies
operational_schedules
operational_on_call_assignments
operational_users
operational_teams
service_repository_mappings
```

Exact names are decided in CHAOS-2955.

### Existing data compatibility

Implement one of:

1. dual-write existing Atlassian ingestion into legacy and canonical tables until parity;
2. migrate/backfill Atlassian rows into canonical tables plus compatibility views;
3. canonical views over provider-specific raw tables as an interim step.

Do not remove `atlassian_ops_*` tables until parity and rollback are proven.

### Existing generic incidents

GitHub/GitLab issue-derived incidents should remain supported. A compatibility adapter or migration may expose them as canonical operational incidents with an explicit repository-derived service/mapping source.

Do not silently rewrite historical IDs or break current incident-correlation queries.

## Sync and backfill design

Reference collections such as services, policies, teams, users, and schedules can be refreshed as bounded complete snapshots when practical.

Incident history is windowed and paginated.

Per-incident enrichment for alerts, log entries, and notes must be controlled through:

- dataset toggles;
- history limits;
- per-sync incident caps;
- concurrency caps;
- rate-limit budgets;
- partial/degraded completion reporting.

The provider must integrate with canonical SyncRun, JobRun, backfill, credential preflight, provider usage, and queue telemetry paths.

## Work graph and correlation

Canonical nodes/edges:

```text
operational_service -> repository
operational_service -> incident
incident -> alert
incident -> timeline_event
incident -> responder
incident -> escalation_policy
incident -> work_item
incident -> pull_request
incident -> deployment
incident -> remediation_work_item
```

Correlation order:

1. explicit source link;
2. explicit service-to-repository mapping;
3. provider/service-catalog relationship;
4. bounded time/service/environment heuristic.

Rules:

- every edge carries provenance, confidence, evidence, provider, org, and timestamps;
- temporal proximity alone never proves root cause;
- heuristic edges use stable rule IDs and lower confidence;
- duplicate service display names cannot collide;
- cross-org and unmapped-service data cannot leak into repo-scoped queries.

Existing incident Sankey, deployment→incident graph, DORA/MTTR, AI workflow linkage, and ACR evidence should read the canonical operational model.

## Provider setup workflow

Use the existing Provider workflow:

1. Add Provider → PagerDuty
2. Enter API/OAuth credential
3. Run credential and permission preflight
4. Select datasets and history window
5. Discover PagerDuty services
6. Map services to repositories
7. Configure V3 webhooks or select REST-only mode
8. Review and start initial sync
9. Display REST sync/backfill health and webhook health separately

Required states:

- invalid credential;
- insufficient dataset scope;
- no accessible services;
- optional feature unavailable by plan/permission;
- unmapped services;
- REST-only;
- webhook pending/success/failure;
- rate limited;
- initial sync partial/failed/complete.

Do not add a PagerDuty-specific dashboard or integration-card design.

## Observability

Required telemetry:

- requests, latency, failures, and rate-limit state by route family;
- entities fetched/normalized/persisted by dataset;
- sync/backfill duration and partial-failure reasons;
- webhook accepted/rejected/duplicate/replayed/out-of-order counts;
- queue depth/age and processing latency;
- hydration call count;
- unmapped service count;
- mapped-service and correlation coverage;
- credential preflight results without secret values.

## Test plan

### Deterministic fixtures

Sanitized fixtures for:

- services and business services;
- incidents and incident lifecycle variants;
- alerts;
- log entries/timeline events;
- notes;
- escalation policies;
- schedules and on-call entries;
- users and teams;
- V3 webhook events.

Include equivalent Atlassian/JSM fixtures to prove canonical parity.

### Adversarial tests

- 401 and 403;
- partial dataset permission;
- 429 and reset behavior;
- pagination boundary and partial-page failure;
- invalid signature;
- duplicate and replayed webhook;
- out-of-order webhook;
- unknown event/status fields;
- cross-org isolation;
- multi-repo service;
- duplicate service names;
- stale webhook versus newer REST state;
- deleted service;
- repeated backfill idempotency.

### Live validation

Env-gated live validation must prove:

- credential preflight;
- at least one service;
- escalation policy;
- schedule and on-call assignment;
- incident lifecycle with alerts/log entries/notes where available;
- signed V3 test event;
- REST reconciliation after a missed webhook;
- service-to-repository mapping;
- at least one provenance-backed incident work-graph edge.

## Delivery sequence and Linear mapping

### Wave 1 — contracts and client in parallel

- [CHAOS-2955](https://linear.app/fullchaos/issue/CHAOS-2955/pagerduty-foundation-define-canonical-operational-contracts-and): canonical operational contracts and storage migration
- [CHAOS-2956](https://linear.app/fullchaos/issue/CHAOS-2956/pagerduty-client-implement-auth-pagination-rate-limits-and-typed-api): PagerDuty client, auth, models, pagination, and rate limits

### Wave 2 — REST ingestion

- [CHAOS-2957](https://linear.app/fullchaos/issue/CHAOS-2957/pagerduty-sync-normalize-and-persist-rest-entities-through-canonical): normalization, sync, backfill, and canonical sinks

### Wave 3 — low-latency updates and graph integration

- [CHAOS-2958](https://linear.app/fullchaos/issue/CHAOS-2958/pagerduty-webhooks-ingest-v3-incident-and-service-events-with): V3 webhooks
- [CHAOS-2959](https://linear.app/fullchaos/issue/CHAOS-2959/pagerduty-correlation-map-services-to-repositories-and-extend-incident): service mapping and work graph

### Wave 4 — product setup

- [CHAOS-2960](https://linear.app/fullchaos/issue/CHAOS-2960/pagerduty-provider-setup-add-canonical-credential-dataset-mapping-and): provider setup workflow

### Wave 5 — release gate

- [CHAOS-2961](https://linear.app/fullchaos/issue/CHAOS-2961/pagerduty-validation-add-fixtures-live-e2e-observability-and-provider): E2E, live validation, observability, and docs

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Provider-specific storage proliferates | Canonical operational contract is a hard prerequisite |
| PagerDuty data is service-scoped while current incidents are repo-scoped | Explicit service↔repository mapping; repository remains optional |
| Per-incident enrichment creates N+1 and rate-limit pressure | Dataset toggles, caps, concurrency controls, usage telemetry |
| Webhooks are missed or arrive out of order | REST reconciliation and event-time ordering |
| Paid/plan-specific fields cause sync failure | Dataset-specific capability/preflight and graceful degradation |
| Existing Atlassian incident surfaces regress | Dual-write/compatibility path and shared fixtures |
| Temporal correlation is mistaken for root cause | Provenance/confidence and explicit non-causal semantics |
| Sensitive incident text leaks | Sanitized fixtures, secure credentials, bounded raw payload retention, no secret/raw incident logging |

## Definition of done

- Canonical operational contracts support PagerDuty and Atlassian/JSM equivalents.
- PagerDuty REST client is typed, instrumented, rate-limit safe, and read-only.
- REST sync/backfill persists all selected V1 datasets idempotently.
- V3 webhook updates are signed, durable, idempotent, replay-safe, and reconcilable.
- Services map to repositories without synthetic repo requirements.
- Existing incident correlation and work-graph surfaces consume PagerDuty entities.
- Provider setup supports credential preflight, dataset selection, mappings, REST-only, and webhook modes.
- Deterministic, adversarial, and env-gated live tests pass.
- Provider telemetry and runbooks make failures diagnosable.
- Connector inventory and stale Atlassian/Jira coverage statements are corrected.
- No PagerDuty mutation API or provider-specific analytics silo is introduced.
