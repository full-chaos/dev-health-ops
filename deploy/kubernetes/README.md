# Kubernetes Deployment

Raw manifests managed via Kustomize:

```bash
kubectl apply -k deploy/kubernetes/
```

## Database migrations (required ordering)

Schema migrations run as a one-shot Job (`migrate-job.yaml`), and all app pods
run with `AUTO_RUN_MIGRATIONS=false` (set in `configmap.yaml`) so api/worker
never ambient-migrate (CHAOS-2304 — shadow-table rebuild migrations are not
safe to run concurrently from workers).

**Kubernetes Jobs do not gate Deployments.** The migrate Job must complete
before rolling out new api/worker images:

```bash
# Jobs are immutable — delete the previous run first
kubectl -n dev-health delete job dev-health-migrate --ignore-not-found

kubectl apply -k deploy/kubernetes/

kubectl -n dev-health wait --for=condition=complete --timeout=600s \
  job/dev-health-migrate

# Only then roll the app workloads onto the new image
kubectl -n dev-health rollout restart deployment/dev-health-api deployment/dev-health-worker
```

If the Job fails, inspect it before retrying:

```bash
kubectl -n dev-health logs job/dev-health-migrate
```

### Safety net: `wait-for-migrations` initContainers

The explicit `kubectl wait` flow above is the recommended path, but a naive
`kubectl apply -k deploy/kubernetes/` is also safe: the api and worker
Deployments carry a `wait-for-migrations` initContainer that blocks app start
until `dev-hops migrate clickhouse status --check` reports the schema current.

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
  `docs/ops/database-connection-pooling.md`.
