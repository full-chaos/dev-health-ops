---
page_id: con-contracts
summary: Rules for canonical provider, identity, API, schema, job, route, metric, taxonomy, feature, and documentation contracts.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/contributing/platform-contract.md
  - docs/architecture/pagerduty-contract.md
  - docs/decisions/chaos-3034-river-compatibility.md
  - contracts/jobs/v1/
  - contracts/sync-dispatch/v1/
  - current ADRs and canonical registries
applicability: current
lifecycle: active
---

# Stable contracts and source-of-truth rules

A stable contract has one authoritative source, explicit ownership, and defined compatibility behavior. Code, deployment, tests, documentation, and runtime admission must agree before a new provider, job, route, or schema becomes supported.
{: .fc-page-lede }

## Provider and canonical model contracts

- Provider clients own authentication, fetching, pagination, retry, discovery, and provider-specific limits.
- Normalizers map provider records into canonical models; they do not redefine product meaning.
- Canonical IDs include organization and provider/source authority wherever collisions are possible.
- Missing provider transitions or bounded-query absence are unknown unless an authoritative delete/tombstone event exists.
- A provider-native field or relationship is not canonical without an explicit normalization and provenance rule.

PagerDuty REST reads and Webhooks V3 are separate interfaces with distinct authentication and replay behavior, but both use the shared canonical incident identity and ordering model. The webhook payload cannot choose organization, source, credential, or signing authority.

Jira Service Management incident code is not a supported public capability until live tenant proof satisfies its release contract. Do not convert ordinary Jira issues or Opsgenie alerts into incidents by inference.

## Public API and schema contracts

- Public REST, GraphQL, CLI, webhook, and Customer Push schemas are generated or verified from code.
- Incompatible public changes require an explicit version, compatibility bridge, or deprecation path.
- Nullability, pagination, rate limits, errors, and authorization are part of the contract—not implementation details.
- A frontend label is not a backend enum unless the public mapping says so.

## Job contracts

Versioned Go job contracts live under `contracts/jobs/v1/` and define:

- envelope and argument schema;
- stable kind and version;
- registry entry;
- handler capability;
- deployment profile;
- migration and route state;
- compatibility and admission evidence.

A running binary does not admit a job. The route, contract version, compiled handler, schema, deployment profile, and runtime capability must all agree.

## Route ownership

Sync transport routes live under `contracts/sync-dispatch/v1/`; generic job migration state lives with the job contract. Current routes remain Celery-owned.

A route migration must define:

1. source runtime and target runtime;
2. shadow/parity behavior without mutating the baseline;
3. canary admission;
4. idempotency and duplicate prevention;
5. in-flight classification and drain behavior;
6. domain completion evidence;
7. rollback ordering;
8. mixed-version and schema compatibility.

Do not infer route ownership from deployed replicas, health endpoints, queue names, or feature flags alone.

## Storage and outbox contracts

- PostgreSQL semantic state, River queue state, ClickHouse facts, and Valkey/Redis coordination have separate authority.
- Producer-owned outbox intent and relay-owned delivery state use different database privileges.
- Runtime roles cannot inherit migration or cross-domain authority.
- Outbox and replay identifiers preserve tenant and source scope.
- An ambiguous commit outcome is not safe to retry without inspection.

## Metric and taxonomy contracts

Metrics and taxonomies have one registry or computation source. Units, scope, time, weighting, aggregation, evidence quality, and missing-state semantics are part of the public contract.

An unavailable sample is not zero. A model-derived estimate is not a factual category assignment or causal conclusion.

## Feature and deployment contracts

Feature availability comes from the current feature decision and entitlement source. Deployment manifests define process composition and secret ownership but do not override runtime validation or route contracts.

A feature-off path must block new work at every producer, scheduler, reconciler, API, and webhook boundary while preserving necessary inspection and cleanup controls.

## Documentation contracts

The documentation IA owns one canonical public page and URL per reader outcome. Source documents, ADRs, plans, evidence captures, and issue histories feed that page but do not create competing public truth.

ADRs can explain a durable decision. Benchmark captures, migration planning, and rollout evidence stay internal unless a supported reader needs them to operate the current system.
