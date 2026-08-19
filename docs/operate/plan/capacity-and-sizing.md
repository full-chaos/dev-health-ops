---
page_id: op-sizing
summary: Size API, Celery, deployment-selected Go queue groups, PostgreSQL connections, ClickHouse, and provider capacity from measured workload.
content_type: reference
owner: platform-operations
source_of_truth:
  - docs/architecture/worker-scaling-readiness.md
  - docs/operate/configure/databases-and-storage.md
  - deploy/go-workers/deployment.json
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

The Go deployment manifest describes each worker group selected by deployment.
For each group, calculate the fleet budget from its replica range, one River
client per process, its queue-control pool, and its domain pool. Run the
contract checker after changing group replicas, queue concurrency, or pool
limits. Include every group, even groups that are currently scaled to zero,
when checking the declared upper bound.

Do not assume that a zero current replica count makes future activation free.
The real admission budget is the sum of the actual group maxima and the
operator and migration processes, with PostgreSQL and PgBouncer headroom.

## Deployment-selected Go sizing

Deployment chooses a worker group's queue set, per-queue concurrency, replicas,
resources, and autoscaling policy. The application keeps only the canonical
job-kind-to-queue mapping. A group can consume a disjoint set such as
`sync,sync_provider`; another can consume `investment,metrics,reports`; and a
third can intentionally overlap `metrics` with either group that needs the
same queue. Groups scale independently.

For every group, confirm:

- every selected queue is registered and every emitted queue has a consumer;
- handler coverage is complete for every admitted kind and contract version;
- one River client is constructed for the process;
- per-queue concurrency matches the deployment manifest;
- queue-control and domain connections fit the database budget after replica
  multiplication;
- `/readyz` reports the selected queue set, worker identity, client count, and
  effective connection limits;
- queue depth, oldest eligible age, execution saturation, and both
  pool-saturation metrics are monitored;
- rollback can restore Celery ownership without duplicate effects.

There is no global unique-queue-owner requirement. River claim semantics allow
overlapping groups to share a queue. Budget the overlap as additional consumer
capacity and verify the queue's total concurrency against provider and store
limits.

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
