---
page_id: op-metrics
summary: Monitor API, Celery, Go worker foundations, provider delivery, synchronization, stores, model usage, and data freshness without publishing misleading zeros.
content_type: reference
owner: platform-operations
source_of_truth:
  - docs/architecture/platform-sync-observability.md
  - docs/architecture/sync-usage-actuals.md
  - docs/ops/observability-tooling.md
  - docs/ops/workers.md
  - deploy/grafana/dashboards/go-workers.json
applicability: current
lifecycle: active
---

# Metrics and traces

Observability should let an operator move from a user-visible symptom to the responsible source, queue, process, store, and domain result. Track execution and data progress together; process availability alone does not prove that current product data exists.
{: .fc-page-lede }

## API and request path

Monitor:

- request rate, latency, and errors;
- GraphQL query timeouts and bounded cost;
- authentication and authorization failures;
- PostgreSQL and ClickHouse query latency and pool saturation;
- correlation IDs across API, worker, and provider calls.

## Celery execution

Monitor:

- task throughput and duration by bounded task or queue family;
- queue depth and oldest age;
- active, retrying, failed, and terminal tasks;
- lease expiry and stale-work repair;
- normal and heavy-worker saturation;
- Beat dispatch and duplicate-scheduler indicators;
- broker/result-backend connectivity.

## Go worker coexistence

The Go foundations expose bounded Prometheus signals for:

- queue depth;
- oldest eligible River job age;
- execution saturation;
- domain PostgreSQL pool saturation;
- direct queue-control PostgreSQL pool saturation;
- readiness categories;
- registry, handler, contract-version, and River-schema compatibility;
- scheduler and reconciler shadow/parity observations.

A database sample failure makes the metric unavailable. It must not be emitted as a zero. These metrics describe the coexistence foundation and do not imply that River owns production jobs.

Use the checked-in `deploy/grafana/dashboards/go-workers.json` as the current dashboard example. Keep profile and route state visible so a healthy disabled process is not mistaken for an active production consumer.

## Provider and incident delivery

Monitor:

- authentication and refresh failure;
- dataset permission preflight;
- request count, latency, pagination, and rate-limit or budget deferral;
- source discovery and mapping coverage;
- webhook signature, unknown-event, replay, conflict, and queue-admission outcomes;
- bounded backfill progress;
- latest successful source and processing timestamps.

For PagerDuty, distinguish REST synchronization from Webhooks V3. `pagey.ping` is a subscription health event, not an incident write. Identical event replay is accepted idempotently; same-identity/different-body reuse is a conflict.

## Synchronization and data progress

Track:

- planned, dispatched, running, retrying, completed, and failed sync units;
- oldest active run and stale leases;
- source × dataset × window coverage;
- provider budget reservations and deferrals;
- downstream writes and post-sync processing;
- latest product materialization and freshness.

Manual, scheduled, and backfill paths should converge on the same execution truth. Separate trigger counts from completed domain effects.

## Stores and migrations

Monitor:

- PostgreSQL client and server connection budgets;
- PgBouncer client and server pool saturation;
- direct River queue-control pool saturation;
- ClickHouse write errors, query latency, merges, and storage growth;
- Valkey/Redis memory, eviction, and connectivity;
- pending or failed migrations;
- runtime role and schema readiness.

## Model and external services

Where enabled, monitor model latency, error, circuit-breaker state, token or usage counts, and spend. Monitor email, billing, and other external services at their own trust boundaries without exposing payloads or secrets.

## Label safety

Use low-cardinality, tenant-safe labels. Do not label metrics or traces with raw tokens, DSNs, URLs containing credentials, request bodies, event payloads, arbitrary repository names at unbounded cardinality, or customer-sensitive text. Preserve detailed evidence in authorized logs or domain records, not metric labels.
