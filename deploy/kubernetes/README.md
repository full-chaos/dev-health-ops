# Kubernetes Deployment

Raw manifests managed via Kustomize:

```bash
kubectl apply -k deploy/kubernetes/
```

## Database migrations (required ordering)

Schema migrations run as a one-shot Job (`migrate-job.yaml`), and all app pods
run with `AUTO_RUN_MIGRATIONS=false` (set in `configmap.yaml`) so api/go-worker
groups never ambient-migrate (CHAOS-2304 — shadow-table rebuild migrations are
not safe to run concurrently from workers).

**Kubernetes Jobs do not gate Deployments.** The migrate Job must complete
before rolling out new api/go-worker images:

```bash
# Jobs are immutable — delete the previous run first
kubectl -n dev-health delete job dev-health-migrate --ignore-not-found

kubectl apply -k deploy/kubernetes/

kubectl -n dev-health wait --for=condition=complete --timeout=600s \
  job/dev-health-migrate

# Only then roll the app workloads onto the new image
kubectl -n dev-health rollout restart deployment/dev-health-api
kubectl -n dev-health rollout restart deployment -l app.kubernetes.io/component=go-worker
```

If the Job fails, inspect it before retrying:

```bash
kubectl -n dev-health logs job/dev-health-migrate
```

### Safety net: `wait-for-migrations` initContainers

The explicit `kubectl wait` flow above is the recommended path, but a naive
`kubectl apply -k deploy/kubernetes/` is also safe: the api Deployment carries
a `wait-for-migrations` initContainer that blocks app start until
`dev-hops migrate clickhouse status --check` reports the schema current. The
go-worker groups in `go-workers.yaml` deploy at `replicas: 0` by default (see
below), so they carry no such initContainer of their own.

- The check is strictly **read-only** (it lists applied vs pending migrations
  and exits 1 while any are pending) — it never runs DDL, so multiple replicas
  polling concurrently cannot race. The migrate Job remains the only thing
  that applies schema.
- Each initContainer run polls every 5s for up to ~5 minutes, then exits
  nonzero and relies on the kubelet's restart backoff as the overall timeout.
  Pods stuck in `Init:...` mean the migrate Job has not completed — check
  `kubectl -n dev-health logs job/dev-health-migrate`.
- The check covers **ClickHouse only**. Postgres is external/optional in this
  stack (the Alembic step is skipped when `POSTGRES_URI` is unset) and Alembic
  has no equally cheap read-only pending-check wired into `dev-hops`; if you
  run Postgres, use the explicit `kubectl wait` flow above for full ordering.

Notes:

- `CLICKHOUSE_URI` is intentionally duplicated: the migration Job reads it
  from the database-only `dev-health-migration-secrets`, while application and
  read-only wait containers read it from `dev-health-secrets`. Keep both in
  sync with `DATABASE_URI` (the app connection string). This prevents the
  elevated one-shot pod from receiving provider credentials.
- For the unified Alembic + River path, populate `MIGRATION_DATABASE_URI` in
  the dedicated `dev-health-migration-secrets` Secret. It must point
  **directly** at Postgres (port 5432) and use the migration role; the checked-in
  Secret intentionally contains no placeholder value.
- Existing Alembic-only installations may continue to use the direct
  `POSTGRES_URI` in `dev-health-migration-secrets`. Without the elevated
  migration DSN, the River step is skipped. ClickHouse migrations always run.
  See
  `docs/operate/configure/databases-and-storage.md`.

## Go/River worker topology (CHAOS-3052)

`go-workers.yaml` is the only worker topology this tree renders and is a base
`kustomization.yaml` resource, applied by the same `kubectl apply -k` as
everything else. It replaces the Celery `worker.yaml`/`beat.yaml` manifests
deleted in CHAOS-4195 (production stopped running them on 2026-08-19,
CHAOS-4026). Every group deploys at `replicas: 0` so a fresh apply stays
inert until an operator deliberately scales the groups it needs, after the
migration Job has completed.

**Provision the PostgreSQL roles first.** This tree provisions no PostgreSQL
roles automatically. `dev-health-go-worker-secrets` (`secrets.yaml`)
authenticates as `devhealth_domain`/`devhealth_queue`/`devhealth_coordinator`
-- matching `RIVER_DOMAIN_DATABASE_ROLE`/`RIVER_QUEUE_DATABASE_ROLE`/
`RIVER_COORDINATOR_DATABASE_ROLE` in `configmap.yaml` -- and
`RuntimeConfig.Validate` (`internal/storage/postgres/runtime.go`) rejects any
DSN whose login doesn't equal the configured role, so a worker fails
readiness on first scale-up without these roles existing. Create them once
against your PostgreSQL server (the compose topology automates this as
`go-river-provision`; there is no Kubernetes equivalent yet):

```bash
PGPASSWORD=<admin password> psql --host=<postgres host> --username=<admin user> \
  --dbname=devhealth \
  --set=domain_password=<match POSTGRES_URI's password in secrets.yaml> \
  --set=queue_password=<match WORKER_DATABASE_URI's password> \
  --set=coordinator_password=<match COORDINATOR_DATABASE_URI's password> \
  --file=scripts/worker/provision_river_roles.sql
```

Then scale a group:

```bash
kubectl -n dev-health scale deployment/dev-health-go-worker-heavy --replicas=1
kubectl -n dev-health rollout status deployment/dev-health-go-worker-heavy
```

The workloads are non-root, read-only, start-first rolling Deployments. Their
HPA requires a Prometheus Adapter for `worker_jobs_available`,
`worker_job_oldest_age_seconds`, and `worker_execution_saturation_ratio`;
scrape the shared `dev-health-go-workers` Service before enabling a group's
autoscaler. See `../go-workers/README.md` for group semantics and rollout
guidance.

### Upgrading an existing installation that still runs the Celery fleet

`kubectl apply -k` never deletes an object just because its manifest left
`kustomization.yaml` -- it only stops managing it. If you have a prior
installation of this tree with the Celery Deployments/HPA running (from
before CHAOS-4195), applying this version leaves them running orphaned
alongside the new Go topology: `dev-health-worker`,
`dev-health-worker-ingest`, `dev-health-worker-external-ingest`,
`dev-health-worker-heavy`, `dev-health-beat` (Deployments), and
`dev-health-worker-hpa` (HorizontalPodAutoscaler). Delete them explicitly as
part of the upgrade, after scaling up whichever Go groups replace the
capacity you're removing:

```bash
kubectl -n dev-health delete deployment \
  dev-health-worker dev-health-worker-ingest \
  dev-health-worker-external-ingest dev-health-worker-heavy dev-health-beat \
  --ignore-not-found
kubectl -n dev-health delete hpa dev-health-worker-hpa --ignore-not-found
```

A fresh install has nothing to clean up here.
