---
page_id: op-healthchecks
summary: Use liveness, readiness, dependency, contract, worker, queue, and data-progress checks for distinct operational decisions.
content_type: reference
owner: platform-operations
source_of_truth:
  - current API and worker health implementation
  - docs/ops/workers.md
applicability: current
lifecycle: active
---

# Health checks

Health endpoints answer narrow questions. A successful HTTP response from one process is not proof that the platform can authenticate providers, consume every queue, execute admitted contracts, write downstream state, or produce current product data.
{: .fc-page-lede }

## API checks

Use the deployment's API liveness and readiness endpoints for orchestrator decisions, then verify separately:

- PostgreSQL, ClickHouse, and Valkey/Redis connectivity;
- current application and analytics migrations;
- authentication and encryption configuration;
- GraphQL and REST dependency readiness;
- provider credential validation where required;
- latest successful background work and product freshness.

## Celery checks

For active Celery workers and Beat, verify:

- each configured queue has a live consumer;
- the scheduler is active exactly once;
- queue depth and oldest age are bounded;
- worker heartbeats are current;
- tasks are completing rather than only starting;
- retries and stale leases are not accumulating;
- the latest expected scheduled work was actually created.

## Go foundation endpoints

The additive Go worker exposes:

| Endpoint | Decision |
| --- | --- |
| `/healthz` | The process is alive. It does not prove job readiness. |
| `/readyz` | Required configuration, database roles, job registry, handler coverage, contract versions, queue-control connectivity, and River schema are compatible. |
| `/metrics` | Bounded operational telemetry is available for the foundation. |

Readiness remains closed when:

- the domain or queue database role is over-privileged or mismatched;
- `WORKER_DATABASE_URI` uses an unsupported pooling mode;
- the job registry or deployment profile is invalid;
- an admitted contract version has no compiled handler;
- the River schema is incompatible;
- a required dependency cannot be sampled.

A Go process may be healthy while its deployment profile remains disabled and Celery retains route ownership. That is expected during coexistence.

## Data-progress checks

The platform is useful only when data progresses. Check:

- latest planned and completed synchronization;
- oldest active run or unit;
- latest successful provider read;
- latest successful processing/materialization;
- product freshness for a known source and period;
- representative downstream writes.

A dependency sampling failure must be reported as unavailable, not as zero queue depth, zero saturation, or a healthy empty result.

## Exposure

Expose health endpoints only to the orchestrator and authorized operators where possible. Responses should use stable categories and avoid DSNs, credentials, grant details, payloads, provider tokens, or customer-sensitive identifiers.
