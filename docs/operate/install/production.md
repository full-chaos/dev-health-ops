---
page_id: op-production
summary: Choose a supported deployment artifact, preserve migration ordering, keep Celery ownership explicit, and verify a production revision before enabling traffic or synchronization.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/deployment-guide.md
  - deploy/kubernetes/
  - deploy/docker-compose/
  - deploy/docker-swarm/
  - deploy/helm/dev-health/
  - deploy/go-workers/
applicability: current
lifecycle: active
---

# Install a production environment

Dev Health is deployed as an API plus background workers, persistent databases, a queue, and scheduled work. A production deployment is not complete when the API container starts: migrations must finish, the active worker topology must consume every configured queue, and the selected revision must pass health and data-progress checks before provider synchronization or user traffic is enabled.
{: .fc-page-lede }

## Choose a deployment example

Use the repository artifact that matches the environment you already operate. Do not translate one example into a new platform during the first installation unless that platform has its own reviewed manifests and runbook.

<div class="fc-topic-grid" markdown>

<div class="fc-topic-card" markdown>

### [Docker Compose](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-compose)

Best for a single managed host or small environment where Docker Compose is the operational standard. The example includes one-shot migrations, API, Celery workers, queue routing, health checks, and an environment template.

</div>

<div class="fc-topic-card" markdown>

### [Kubernetes with Kustomize](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/kubernetes)

Best for Kubernetes with ingress, external secret management, and an established rollout process. The base includes namespace, storage, migration Job, API, workers, schedules, and ingress resources.

</div>

<div class="fc-topic-card" markdown>

### [Helm](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/helm/dev-health)

Use the chart where Helm is the managed release boundary. Review values, schema validation, migration-job ordering, secrets, and worker settings before installation.

</div>

<div class="fc-topic-card" markdown>

### [Docker Swarm](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-swarm)

Use only where Swarm is already supported. The example uses Docker secrets and requires explicit verification of the one-shot migration service because Swarm does not provide Compose-style dependency ordering.

</div>

</div>

The root [`compose.yml`](https://github.com/full-chaos/dev-health-ops/blob/main/compose.yml) is for local development and evaluation. It is not a substitute for the production examples.

## Current worker ownership

Celery remains the production owner of all current jobs and schedules. The checked-in Go worker profiles under [`deploy/go-workers/`](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/go-workers) are disabled coexistence foundations:

- minimum replicas are zero;
- current routes remain `celery`;
- a healthy Go binary does not admit production work;
- River queue ownership requires job-specific contract, handler, parity, canary, and rollback evidence;
- Celery workers and Beat remain required until a migration explicitly changes a route.

Do not add enabled Go replicas to a production overlay merely because the image and profile exist. Use the profiles to validate topology, connection budgets, health, and future migration readiness.

## Prepare the production inputs

Before applying any example, decide and record:

- the immutable Dev Health image or reviewed source revision;
- PostgreSQL domain, queue-control, and migration endpoints where the Go foundation is included;
- the ClickHouse database and credentials;
- the Valkey or Redis endpoint used by Celery and distributed controls;
- provider credentials or app installations;
- ingress hostname, TLS termination, and trusted proxy ranges;
- worker concurrency, heavy-worker capacity, and scheduled work;
- backup, restore, monitoring, and rollback ownership.

Use an external secret store or the scheduler's secret mechanism. Never commit populated credentials. Checked-in environment and secret files describe names and shape only.

## Database identities and migration ordering

For the active Python runtime, `POSTGRES_URI` may use transaction-mode PgBouncer when `PGBOUNCER_TRANSACTION_MODE=true`. Migrations must bypass the transaction pooler.

When the Go coexistence foundation is deployed, keep these responsibilities distinct:

```dotenv
POSTGRES_URI=postgresql+asyncpg://devhealth_domain:<secret>@pgbouncer:6432/devhealth
WORKER_DATABASE_URI=postgres://devhealth_queue:<secret>@postgres:5432/devhealth
MIGRATION_DATABASE_URI=postgres://devhealth_migrate:<secret>@postgres:5432/devhealth
```

Long-running workers must not receive `MIGRATION_DATABASE_URI`. Provision the domain and queue roles before the one-shot migration applies River grants.

## Docker Compose example

Start from the checked-in [production Compose file](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/docker-compose/compose.production.yml) and [environment template](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/docker-compose/.env.example).

```bash
cd deploy/docker-compose
cp .env.example .env
```

Replace the example image, database hosts, passwords, application secrets, and provider credentials. Pin a reviewed tag or digest:

```dotenv
DEV_HEALTH_IMAGE=ghcr.io/full-chaos/dev-health-ops@sha256:<reviewed-digest>
POSTGRES_HOST=postgres.internal.example
POSTGRES_USER=devhealth
POSTGRES_PASSWORD=<secret-from-your-store>
POSTGRES_DB=devhealth
POSTGRES_URI=postgresql+asyncpg://devhealth:<secret>@postgres.internal.example:5432/devhealth
CLICKHOUSE_PASSWORD=<secret-from-your-store>
```

Run and inspect the one-shot migration service before starting the rest of the stack:

```bash
docker compose -f compose.production.yml pull
docker compose -f compose.production.yml up migrate
docker compose -f compose.production.yml logs migrate
```

Start the application only after migration exits successfully:

```bash
docker compose -f compose.production.yml up -d
docker compose -f compose.production.yml ps
curl -fsS http://127.0.0.1:${API_PORT:-8000}/ready
```

If migration fails, correct the database, credential, role, or schema problem and rerun only that service before starting API and workers.

## Kubernetes example

The checked-in [Kustomize entry point](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/kubernetes/kustomization.yaml) assembles namespace, configuration, secret template, ClickHouse, Valkey, migration Job, API, Celery workers, schedules, and ingress. Review the [Kubernetes deployment notes](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/kubernetes/README.md).

Create an environment overlay that pins the image and replaces example configuration:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
images:
  - name: ghcr.io/your-org/dev-health-ops
    newName: ghcr.io/full-chaos/dev-health-ops
    digest: sha256:<reviewed-digest>
```

Apply migrations and wait before rolling application workloads:

```bash
kubectl -n dev-health delete job dev-health-migrate --ignore-not-found
kubectl apply -k deploy/kubernetes/
kubectl -n dev-health wait --for=condition=complete --timeout=600s \
  job/dev-health-migrate
kubectl -n dev-health rollout status deployment/dev-health-api
kubectl -n dev-health rollout status deployment/dev-health-worker
```

If the migration Job fails:

```bash
kubectl -n dev-health logs job/dev-health-migrate
kubectl -n dev-health describe job dev-health-migrate
```

The API and worker manifests include a read-only migration wait as a safety net, but explicit Job completion remains the production procedure.

## Docker Swarm example

The [Swarm example](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-swarm) expects Docker secrets and a pre-existing Swarm:

```bash
docker stack deploy -c deploy/docker-swarm/stack.yml dev-health
```

Swarm does not gate services on migration completion. Verify the one-shot task before relying on API or workers:

```bash
docker service logs dev-health_migrate
docker service ps dev-health_migrate
```

A successful migration task should be in `Shutdown` without an error. After correcting a failure:

```bash
docker service update --force dev-health_migrate
```

## Verify the deployed revision

Before enabling provider synchronization or directing user traffic, verify:

1. the deployed image digest matches the reviewed revision;
2. PostgreSQL, River where present, and ClickHouse migrations are current;
3. runtime role separation and connection modes pass readiness;
4. the API readiness endpoint succeeds through the intended ingress path;
5. active Celery workers consume every configured queue;
6. Celery Beat or the current scheduler is enabled exactly once;
7. any Go foundation profile remains disabled unless its route is explicitly approved;
8. Valkey or Redis is reachable and persistent where required;
9. TLS, forwarded headers, and trusted proxies match the real network path;
10. logs, metrics, and alerts identify environment and revision;
11. a known-good previous image and configuration remain available for rollback.

Continue with [Verify first health](verify-health.md), [Environment and secrets](../configure/environment-and-secrets.md), [Databases and storage](../configure/databases-and-storage.md), [Workers and schedules](../configure/workers-and-schedules.md), and [Production hardening](../security/hardening.md).

## Do not infer support from an example alone

Deployment directories are maintained examples, not a promise that every default fits every environment. Review image tags, storage classes, resource limits, ingress behavior, secret integration, provider budgets, worker ownership, and observability. When an example and the current runtime contract disagree, the current code, migration state, route contract, and validated configuration win.
