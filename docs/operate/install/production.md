---
page_id: op-production
summary: Deploy with the supported Helm chart, preserve migration ordering, and verify a production revision before enabling traffic or synchronization.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/deployment-guide.md
  - deploy/helm/dev-health/
  - deploy/go-workers/
applicability: current
lifecycle: active
---

# Install a production environment

Dev Health is deployed as an API plus background workers, persistent databases, a queue, and scheduled work. A production deployment is not complete when the API container starts: migrations must finish, the active worker topology must consume every configured queue, and the selected revision must pass health and data-progress checks before provider synchronization or user traffic is enabled.
{: .fc-page-lede }

## Deploy with the Helm chart

The [Helm chart](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/helm/dev-health) is the one supported production deployment artifact. Use it where Helm is the managed release boundary, and review values, schema validation, migration-hook ordering, secrets, and worker settings before installation.

The repository no longer ships production Docker Compose, Docker Swarm, or Kustomize examples: they duplicated the chart's contract without being the path production runs, so they drifted from it. Do not translate the chart into a new platform during the first installation unless that platform has its own reviewed manifests and runbook.

The root [`compose.yml`](https://github.com/full-chaos/dev-health-ops/blob/main/compose.yml) is for local development and evaluation. It is not a substitute for the chart.

## Current worker ownership

The Go worker fleet owns every production job and schedule. Celery was stopped
in production on 2026-08-19 and its services were removed from the deployed
topology; no Python Celery worker or Beat process runs in production, and no
route is served by one.

What that means when you deploy:

- the Go worker deployment groups are the production worker topology, not a coexistence foundation waiting on approval. Each group renders at the replica count its `goWorkers.groups` entry sets, so an operator sizes only the groups that environment needs;
- routes are served by the Go runtime, and a route's owner is recorded in the routing state rather than assumed from the deployment;
- River queue ownership is established per job kind, and changing it still requires contract, handler, parity, canary, and rollback evidence;
- the reconciler refuses a route that drifts from the checked-in policy, so a
  deployment whose routes disagree with the manifest fails closed rather than
  serving the wrong runtime.

No Compose file or chart defines the Celery services any more (CHAOS-5589): R146 established Celery is not a rollback target, so the fleet is deleted outright, not archived-in-place. Go/River is the unconditional default worker topology.

## Prepare the production inputs

Before installing the chart, decide and record:

- the immutable Dev Health image or reviewed source revision;
- PostgreSQL domain, queue-control, and migration endpoints where the Go foundation is included;
- the ClickHouse database and credentials;
- the Valkey or Redis endpoint used by distributed controls;
- provider credentials or app installations;
- ingress hostname, TLS termination, and trusted proxy ranges;
- worker concurrency, heavy-worker capacity, and scheduled work;
- backup, restore, monitoring, and rollback ownership.

Use an external secret store or the scheduler's secret mechanism. Never commit populated credentials. Checked-in environment and secret files describe names and shape only.

## Database identities and migration ordering

For the Python API runtime, `POSTGRES_URI` may use transaction-mode PgBouncer when `PGBOUNCER_TRANSACTION_MODE=true`. Migrations must bypass the transaction pooler.

When the Go coexistence foundation is deployed, keep these responsibilities distinct:

```dotenv
POSTGRES_URI=postgresql+asyncpg://devhealth_domain:<secret>@pgbouncer:6432/devhealth
WORKER_DATABASE_URI=postgres://devhealth_queue:<secret>@postgres:5432/devhealth
MIGRATION_DATABASE_URI=postgres://devhealth_migrate:<secret>@postgres:5432/devhealth
```

Long-running workers must not receive `MIGRATION_DATABASE_URI`. Provision the domain and queue roles before the one-shot migration applies River grants.

The Go topology requires `max_connections` on that PostgreSQL server to be at least the declared `postgres_budget.server_max_connections` in `deploy/go-workers/deployment.json` — currently **200**. Nothing in the stack checks this for you: the deployment contract validates the topology against its own declaration, but it cannot read your server. A managed instance left at the common default of 100 cannot hold the three PgBouncer pools plus the migration and administrative reserve, and the shortfall only appears under the load that needs every pool at once. The three PgBouncer poolers start unconditionally now (CHAOS-5589: the Go fleet depends on them, not an opt-in), so confirm this before bringing up the stack at all:

```bash
psql "$MIGRATION_DATABASE_URI" -Atc 'SHOW max_connections'
```

## Install the release

Pin a reviewed image tag or digest in your values file, replace the example database hosts and secret references, and install with Helm:

```bash
helm upgrade --install dev-health deploy/helm/dev-health \
  --namespace dev-health --create-namespace \
  -f values.production.yaml
```

The migration hooks run before the application workloads roll: the chart's migrate Job applies the PostgreSQL and ClickHouse schema, the optional `provisionRoles` and `riverMigrate` hooks provision the runtime roles and apply the River schema and grants, and the route-activate hook applies the route table. Inspect them before relying on the release:

```bash
kubectl -n dev-health get jobs
kubectl -n dev-health logs job/<migrate-job-name>
```

If a hook Job fails, correct the database, credential, role, or schema problem and run the upgrade again; the workloads keep running the previous revision until the hooks succeed.

## Verify the deployed revision

Before enabling provider synchronization or directing user traffic, verify:

1. the deployed image digest matches the reviewed revision;
2. PostgreSQL, River where present, and ClickHouse migrations are current;
3. runtime role separation and connection modes pass readiness;
4. the API readiness endpoint succeeds through the intended ingress path;
5. the Go worker groups you scaled report ready, and each configured queue has a consumer;
6. the scheduler is enabled exactly once;
7. the reconciler reports no route drift against the checked-in policy;
8. Valkey or Redis is reachable and persistent where required;
9. TLS, forwarded headers, and trusted proxies match the real network path;
10. logs, metrics, and alerts identify environment and revision;
11. a known-good previous image and configuration remain available for rollback.

Continue with [Verify first health](verify-health.md), [Environment and secrets](../configure/environment-and-secrets.md), [Databases and storage](../configure/databases-and-storage.md), [Workers and schedules](../configure/workers-and-schedules.md), and [Production hardening](../security/hardening.md).

## Do not infer support from defaults alone

The chart and its defaults are maintained for the environments this project runs, not a promise that every default fits every environment. Review image tags, storage classes, resource limits, ingress behavior, secret integration, provider budgets, worker ownership, and observability. When an example and the current runtime contract disagree, the current code, migration state, route contract, and validated configuration win.
