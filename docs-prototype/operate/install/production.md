---
page_id: op-production
summary: Choose a supported deployment artifact, preserve migration ordering, and verify a production revision before enabling traffic or synchronization.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/deployment-guide.md
  - deploy/kubernetes/
  - deploy/docker-compose/
  - deploy/docker-swarm/
applicability: current
lifecycle: active
---

# Install a production environment

Dev Health is deployed as an API plus background workers, persistent databases, a queue, and scheduled work. A production deployment is not complete when the API container starts: migrations must finish, the worker topology must be consuming the configured queues, and the selected revision must pass health checks before provider synchronization or user traffic is enabled.
{: .fc-page-lede }

## Choose a deployment example

Use the repository artifact that matches the environment you already operate. Do not translate one example into a new platform during the first installation unless that platform has its own reviewed manifests and runbook.

<div class="fc-topic-grid" markdown>

<div class="fc-topic-card" markdown>

### [Docker Compose](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-compose)

Best for a single managed host or a small environment where Docker Compose is already the operational standard. The example includes the production service topology, one-shot migrations, worker queues, health checks, and an environment template.

</div>

<div class="fc-topic-card" markdown>

### [Kubernetes with Kustomize](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/kubernetes)

Best for a Kubernetes environment with an ingress controller, external secret management, and an established rollout process. The base includes namespace, storage, migration Job, API, workers, schedules, and ingress resources.

</div>

<div class="fc-topic-card" markdown>

### [Docker Swarm](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-swarm)

Use only where Swarm is already the supported scheduler. The example uses Docker secrets and requires explicit verification of the one-shot migration service because Swarm does not provide Compose-style dependency ordering.

</div>

</div>

The root [`compose.yml`](https://github.com/full-chaos/dev-health-ops/blob/main/compose.yml) is for local development and evaluation. It is not a substitute for the production examples.

## Prepare the production inputs

Before applying any example, decide and record:

- the immutable Dev Health image or reviewed source revision;
- the external PostgreSQL connection, when PostgreSQL-backed features are enabled;
- the ClickHouse database and credentials;
- the Valkey or Redis endpoint used by Celery and distributed controls;
- the provider credentials or app installations the environment is allowed to use;
- the ingress hostname, TLS termination, and trusted proxy ranges;
- worker concurrency, heavy-worker capacity, and scheduled work;
- backup, restore, monitoring, and rollback ownership.

Use an external secret store or the scheduler's secret mechanism. Never commit populated credentials to a deployment directory. The checked-in `.env.example` and secret manifests describe names and shape only.

## Docker Compose example

Start from the checked-in [production Compose file](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/docker-compose/compose.production.yml) and [environment template](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/docker-compose/.env.example).

```bash
cd deploy/docker-compose
cp .env.example .env
```

At minimum, replace the example image, database hosts, passwords, application secrets, and provider credentials. Pin the image to a reviewed tag or digest rather than leaving `latest` in production:

```dotenv
DEV_HEALTH_IMAGE=ghcr.io/full-chaos/dev-health-ops@sha256:<reviewed-digest>
POSTGRES_HOST=postgres.internal.example
POSTGRES_USER=devhealth
POSTGRES_PASSWORD=<secret-from-your-store>
POSTGRES_DB=devhealth
POSTGRES_URI=postgresql+asyncpg://devhealth:<secret>@postgres.internal.example:5432/devhealth
CLICKHOUSE_PASSWORD=<secret-from-your-store>
```

The production Compose file runs migrations as a one-shot `migrate` service and keeps `AUTO_RUN_MIGRATIONS=false` on API and worker services. Inspect migrations before starting the rest of the stack:

```bash
docker compose -f compose.production.yml pull
docker compose -f compose.production.yml up migrate
docker compose -f compose.production.yml logs migrate
```

Start the application only after the migration service exits successfully:

```bash
docker compose -f compose.production.yml up -d
docker compose -f compose.production.yml ps
curl -fsS http://127.0.0.1:${API_PORT:-8000}/ready
```

If migrations fail, correct the database, credential, or migration problem and rerun only that service before starting API and workers:

```bash
docker compose -f compose.production.yml up migrate
docker compose -f compose.production.yml up -d
```

## Kubernetes example

The checked-in [Kustomize entry point](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/kubernetes/kustomization.yaml) assembles the namespace, configuration, secret template, ClickHouse, Valkey, migration Job, API, workers, schedules, and ingress. Review the [Kubernetes deployment notes](https://github.com/full-chaos/dev-health-ops/blob/main/deploy/kubernetes/README.md) before applying it.

Create an environment overlay that pins the image and replaces example configuration. The base image stanza is intended to be overridden:

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

Apply migrations and wait for them before rolling application workloads:

```bash
kubectl -n dev-health delete job dev-health-migrate --ignore-not-found
kubectl apply -k deploy/kubernetes/
kubectl -n dev-health wait --for=condition=complete --timeout=600s \
  job/dev-health-migrate
kubectl -n dev-health rollout status deployment/dev-health-api
kubectl -n dev-health rollout status deployment/dev-health-worker
```

If the migration Job fails, inspect it before retrying:

```bash
kubectl -n dev-health logs job/dev-health-migrate
kubectl -n dev-health describe job dev-health-migrate
```

The API and worker manifests include a read-only migration wait as a safety net, but the explicit Job completion check remains the production procedure.

## Docker Swarm example

The [Swarm example](https://github.com/full-chaos/dev-health-ops/tree/main/deploy/docker-swarm) expects Docker secrets and a pre-existing Swarm. Create secrets through your approved secret process, then deploy the stack:

```bash
docker stack deploy -c deploy/docker-swarm/stack.yml dev-health
```

Swarm does not gate other services on the migration service. Verify that migrations completed before relying on the API or workers:

```bash
docker service logs dev-health_migrate
docker service ps dev-health_migrate
```

A successful one-shot migration task should be in `Shutdown` without an error. Re-run it deliberately after correcting a failure:

```bash
docker service update --force dev-health_migrate
```

## Verify the deployed revision

Before enabling provider synchronization or directing user traffic to the environment, verify:

1. the deployed image digest matches the reviewed revision;
2. PostgreSQL and ClickHouse migrations are current;
3. the API readiness endpoint succeeds through the intended ingress path;
4. normal and heavy workers are running and consuming the configured queues;
5. Valkey or Redis is reachable and persistent where required;
6. scheduled work is enabled exactly once;
7. TLS, forwarded headers, and trusted proxies reflect the real network path;
8. logs, metrics, and alerts identify the environment and revision;
9. a known-good previous image and configuration remain available for rollback.

Continue with [Verify first health](verify-health.md), then review [Environment and secrets](../configure/environment-and-secrets.md), [Workers and schedules](../configure/workers-and-schedules.md), and [Production hardening](../security/hardening.md).

## Do not infer support from an example alone

The deployment directories are maintained examples, not a promise that every default fits every environment. Review image tags, storage classes, resource limits, ingress behavior, secret integration, provider budgets, and observability before production use. When the example and current runtime contract disagree, the current code, migration status, and validated configuration win.
