# Canonical Operations Addendum: OAuth, Source Migration, and External Push

**Parent plan:** `2026-07-16-pagerduty-incident-response-provider.md`  
**Linear epic:** [CHAOS-2954](https://linear.app/fullchaos/issue/CHAOS-2954/canonical-operations-model-and-pagerduty-integration)  
**Milestone:** Canonical Incident Response and PagerDuty

## Purpose

This addendum corrects and expands the original PagerDuty plan in three ways:

1. PagerDuty authentication is OAuth-first rather than API-token-first.
2. The canonical operations model must replace or adapt every existing incident producer—not only accept PagerDuty data.
3. The External Push API must expose the same canonical operational contracts so customer ETL and unsupported incident systems do not create a parallel path.

## 1. PagerDuty authentication

PagerDuty REST access supports scoped OAuth. API keys remain supported, but they are not the preferred hosted connection experience.

### 1.1 Hosted Dev Health

Preferred user-connected flow:

```text
Dev Health -> PagerDuty authorization endpoint
           -> user login and consent
           -> authorization code callback
           -> short-lived bearer token
           -> encrypted token lifecycle storage
```

Implementation requirements:

- register a Dev Health PagerDuty app explicitly;
- use authorization code and PKCE where supported by the registered app type;
- protect callback state, PKCE verifier, redirect URI, and account binding;
- persist access token, refresh/token-lifecycle metadata where returned, granted scopes, account subdomain, and region through the encrypted credential store;
- rotate/refresh tokens atomically;
- support disconnect/revocation and expired authorization;
- request read-only scopes derived from enabled datasets;
- never log codes, tokens, client secrets, or authorization headers.

PagerDuty does not currently expose Dynamic Client Registration. App/client registration is an explicit setup prerequisite.

### 1.2 Private and self-hosted deployments

Preferred non-interactive flow:

```text
customer-managed private scoped app
        -> client ID + client secret
        -> identity.pagerduty.com OAuth token exchange
        -> client_credentials
        -> short-lived scoped bearer token
```

The requested scope must include the account qualifier and only the enabled read permissions, for example:

```text
as_account-us.<subdomain>
incidents.read
services.read
schedules.read
oncalls.read
users.read
teams.read
escalation_policies.read
```

Exact scope names must be derived from PagerDuty endpoint documentation and frozen in provider capability tests.

Store the client secret encrypted, cache the resulting access token only for its lifetime, and renew before expiry. Region and PagerDuty account subdomain are part of the credential identity.

### 1.3 API-token fallback

Support general/user REST API tokens for compatibility and bootstrap cases:

```http
Authorization: Token token=<API_KEY>
```

The provider workflow must display this behind **Use API token instead**. It must not require users to create a long-lived token for the normal hosted flow.

### 1.4 OAuth validation matrix

Tests must cover:

- consent success and denial;
- invalid callback state and PKCE mismatch;
- account/region mismatch;
- token expiry and renewal;
- atomic token rotation;
- concurrent refresh attempts;
- revoked authorization;
- client-credentials exchange and renewal;
- insufficient dataset scopes;
- API-token fallback;
- secret-safe errors and telemetry.

## 2. Canonical migration of existing incident producers

PagerDuty is not complete until all current incident producers use or migrate into the canonical operational model.

### 2.1 Atlassian / JSM Operations / Opsgenie

Current provider-specific persistence:

```text
atlassian_ops_incidents
atlassian_ops_alerts
atlassian_ops_schedules
```

Required migration:

- map existing rows into `OperationalIncident`, `OperationalAlert`, and `OnCallSchedule`;
- populate `OperationalService`, responder, team, escalation, timeline, and on-call relationships when source data exists;
- preserve external IDs, source URLs, provider identity, raw status/severity, and timestamps;
- dual-write or backfill plus compatibility views until parity is proven;
- keep provider-specific storage available for rollback until the removal gate is accepted;
- move downstream queries to canonical reads only after row-count and lifecycle parity.

### 2.2 GitHub issue-derived incidents

Current path collects issues with configured incident labels and writes the narrow repo-scoped `Incident` model.

Required migration:

- normalize the issue into `OperationalIncident` with `provider=github` and explicit `source_entity_type=issue`;
- preserve issue ID/number, URL, labels, status, creation, closure/resolution, and repository identity;
- create or resolve a repository-derived `OperationalService`;
- persist an explicit `ServiceRepositoryMapping` rather than requiring `repo_id` in the incident primary key;
- dual-write during parity validation;
- migrate historical `incidents` rows idempotently;
- preserve existing incident-correlation, Sankey, DORA/MTTR, and deployment edges through compatibility reads.

### 2.3 GitLab issue-derived incidents

Apply the same contract to GitLab:

- `provider=gitlab`;
- source issue ID/IID and project instance retained;
- configured incident-label behavior preserved;
- repository/project represented as an explicit operational service mapping;
- historical rows backfilled with stable identities;
- direct writes to the legacy repo-scoped `Incident` model removed only after consumer cutover.

### 2.4 Stable identity

Canonical identity seed:

```text
org_id
+ provider_instance_id
+ source_entity_type
+ external_id
```

Repository or service mappings are relationships and must not alter incident identity.

Migration must not create duplicate work-graph nodes or edges. Every migrated entity retains source provenance and the migration version.

### 2.5 Cutover sequence

```text
canonical contracts and tables
    -> compatibility reads
    -> native dual-write
    -> historical backfill
    -> parity checks
    -> consumer cutover
    -> stop legacy writes
    -> rollback window
    -> legacy removal issue
```

Removal is never implicit in the initial migration.

## 3. External Push API operational contracts

The External Push API was intentionally designed as a connector-equivalent boundary:

```text
customer payload
    -> external-ingest validation
    -> provider-neutral normalization
    -> existing sinks
    -> metrics and graph
```

Its current `external-ingest.v1` implementation exposes nine work/code record kinds and does not yet support operational incidents. The canonical operations model is now the correct contract for that deferred coverage.

### 3.1 Additive record kinds

Add the following record schemas without creating push-only internal models:

```text
operational_service.v1
operational_incident.v1
operational_alert.v1
incident_timeline_event.v1
incident_note.v1
incident_responder.v1
escalation_policy.v1
on_call_schedule.v1
on_call_assignment.v1
operational_team.v1
operational_user.v1
service_repository_mapping.v1
```

Whether these remain additive kinds under `external-ingest.v1` or require an envelope version increment is decided through compatibility tests. The preference is additive record-kind versioning when existing clients remain unaffected.

### 3.2 Source systems

Operational records must support at least:

```text
pagerduty
atlassian
github
gitlab
custom
```

`custom` supports incident-response systems without a native connector, but customers must still provide stable source system/instance identity and canonical record fields.

### 3.3 One contract, multiple transports

Equivalent native and pushed facts must normalize to identical canonical entities:

```text
PagerDuty REST incident --------┐
PagerDuty webhook incident -----┤
Customer-pushed incident -------┼-> OperationalIncident
GitHub incident issue ----------┤
GitLab incident issue ----------┤
JSM/Opsgenie incident ----------┘
```

The transport must not be visible to analytics except as provenance/ownership metadata.

### 3.4 Dataset-level ownership

The existing External Push policy assigns one owner to a complete `source_system + source_instance`. Canonical operations require ownership to be explicit by entity family:

```text
org_id
+ source_system
+ source_instance
+ entity_family/dataset
```

Rules:

- one active owner per entity family;
- native PagerDuty sync and pushed PagerDuty incidents cannot both own incidents for the same provider instance;
- native code sync may coexist with pushed incident data only when dataset ownership is explicit;
- registration and preflight reject ambiguous ownership;
- every canonical row records native-sync versus customer-push provenance;
- ownership changes require a controlled cutover and watermark reset/reconciliation.

### 3.5 Wire-schema requirements

Operational Push schemas must:

- use strict versioned validation (`extra=forbid`);
- require stable external IDs and timestamps;
- preserve raw status/severity/priority beside canonical values;
- represent services and repositories by references, not implicit scope;
- include tombstone/deletion semantics where relevant;
- bound strings, arrays, notes, and structured metadata;
- treat notes/timeline content as untrusted evidence;
- allow per-record validation failures without rejecting unrelated valid records;
- preserve batch and record idempotency.

### 3.6 Processing and recomputation

The External Push worker must:

1. validate the operational record kind;
2. derive org/provider/source identity;
3. normalize through shared canonical operational mappers;
4. write through canonical operational sinks;
5. update bounded rejected-record diagnostics;
6. record source ownership and provenance;
7. enqueue bounded graph/metric recomputation for affected incidents, services, repositories, and time windows.

It must never write final metric tables directly.

### 3.7 Push credentials

PagerDuty OAuth is unrelated to Push API authentication.

- Native PagerDuty connector: PagerDuty OAuth or API-token fallback.
- External Push producer: Dev Health External Push credential.

Push credentials retain:

```text
schema:read
ingest:write
ingest:status
```

Optional least-privilege scopes may be added:

```text
ingest:operations
ingest:incidents
```

### 3.8 CLI and schema discovery

Update:

```text
GET /api/v1/external-ingest/schemas
GET /api/v1/external-ingest/schemas/{version}
POST /api/v1/external-ingest/validate
POST /api/v1/external-ingest/batches
dev-hops push sample
dev-hops push validate
dev-hops push batch
```

Provide sanitized samples for every operational kind and a multi-record incident lifecycle batch.

## 4. Revised work breakdown

### Wave 1: Canonical contract

- CHAOS-2955 — canonical contracts, identity, storage, and compatibility
- CHAOS-2956 — OAuth-first PagerDuty client and fallback token support

### Wave 2: Existing source migration and Push API

- CHAOS-2963 — migrate Atlassian, GitHub, and GitLab incident producers
- CHAOS-2964 — add canonical operational record kinds to External Push

These may proceed in parallel once CHAOS-2955 freezes identity and storage contracts.

### Wave 3: PagerDuty ingestion

- CHAOS-2957 — REST sync, normalization, reconciliation, and sinks
- CHAOS-2958 — V3 webhooks

### Wave 4: Correlation and setup

- CHAOS-2959 — service/repository mapping and work graph
- CHAOS-2960 — OAuth-first provider setup workflow

### Wave 5: Cross-provider release gate

- CHAOS-2961 — OAuth, migration, Push, PagerDuty, observability, docs, and live validation

## 5. Updated definition of done

- PagerDuty can be connected through scoped OAuth without requiring a copied API token.
- Private/self-hosted deployments can obtain scoped app bearer tokens.
- API-token fallback is supported but secondary.
- Atlassian/JSM, GitHub, and GitLab incident producers use or migrate into canonical operational entities.
- Native and pushed operational records normalize into identical canonical entities.
- Source ownership prevents double-ingestion and ambiguous fact authority.
- Existing incident analytics and work-graph behavior retain parity during cutover.
- Push schema discovery, validation, worker processing, CLI samples, and idempotency cover operational records.
- PagerDuty REST and V3 webhooks share the same canonical sinks.
- Legacy incident storage has an explicit parity, rollback, and removal gate.
- No transport- or provider-specific analytics silo is introduced.
