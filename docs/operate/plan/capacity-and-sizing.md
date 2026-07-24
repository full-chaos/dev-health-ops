---
page_id: op-sizing
summary: Size API, Celery, dormant Go profiles, queues, direct and pooled PostgreSQL connections, ClickHouse, and provider capacity from measured workload.
content_type: reference
owner: platform-operations
source_of_truth:
  - docs/architecture/worker-scaling-readiness.md
  - docs/ops/database-connection-pooling.md
  - deploy/go-workers/profiles.json
  - current worker and synchronization implementation
applicability: current
lifecycle: active
---

# Capacity and sizing

Capacity is a system budget across providers, queues, processes, and stores. Increasing workers without enough provider allowance, broker capacity, database connections, or downstream write capacity can increase retries and make recovery slower.
{: .fc-page-lede }

## Workload inputs

Base initial capacity on:

- connected organizations, repositories, projects, and operational services;
- incremental synchronization cadence and historical backfill windows;
- source × dataset × window fan-out;
- provider request, GraphQL cost, and abuse-risk budgets;
- job duration, queue depth, oldest age, retry, lease, and stale-work behavior;
- API request rate and GraphQL query cost;
- ClickHouse ingest, merges, retention, storage, and query load;
- PostgreSQL semantic, queue-control, migration, and operator connections;
- model and external-service rate or spend limits.

## Active Celery sizing

Size normal and heavy-worker classes independently. Use observed execution duration and queue age, not process count alone. Provider-specific and cost-class queues need enough consumers to prevent one expensive source family from starving user-visible work.

Keep Celery Beat singular. Budget scheduled bursts against manual sync, backfill, report, webhook, and materialization work that can overlap.

## PostgreSQL connection budget

Count all server-side connection pools:

- PgBouncer server pools for Python and Go domain roles;
- direct River queue-control connections;
- API and Celery direct pools where used;
- operator CLI invocation;
- migration and administrative reserve.

The current Go profile manifest budgets maximum replicas even while profiles are disabled. Its documented maximum topology includes bounded direct queue-control and pooled domain connections below the checked-in PostgreSQL ceiling. Run the contract checker after changing profile replicas or pool sizes.

Do not size from current zero Go replicas and assume future activation is free. The profile's maximum topology is the admission budget.

## Go coexistence sizing

Current profiles have zero minimum replicas and Celery route ownership. Their maximum replicas and connection limits exist to validate future coexistence safely.

Before enabling a profile, confirm:

- the job route is approved for shadow, canary, or River ownership;
- handler coverage is complete for every admitted contract version;
- direct queue-control and pooled domain connections fit the database budget;
- `/readyz` passes role, schema, registry, and dependency checks;
- queue depth, oldest eligible age, execution saturation, and both pool-saturation metrics are monitored;
- rollback can restore Celery ownership without duplicate effects.

## Provider and webhook capacity

For each provider, include:

- page size and pagination model;
- request and cost budget;
- concurrent dataset families;
- webhook burst and replay behavior;
- bounded backfill chunk size;
- retry and deferral policy;
- reconciliation reads after event delivery.

PagerDuty REST synchronization and Webhooks V3 share canonical downstream state but consume different request and delivery budgets. A webhook burst does not eliminate the need for bounded REST reconciliation.

## Scale from evidence

Scale only after observing:

- queue oldest age and growth rate;
- worker execution saturation;
- provider deferral and rate limits;
- PostgreSQL client/server and pool saturation;
- ClickHouse write/query latency and storage health;
- Valkey/Redis memory and eviction;
- downstream freshness and completion rate;
- error and retry amplification.

Scale the binding constraint first. When a dependency sample fails, treat capacity as unknown rather than reading a missing metric as zero usage.
