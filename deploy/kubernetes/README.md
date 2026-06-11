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

Notes:

- `POSTGRES_URI` (in `dev-health-secrets`) must point **directly** at Postgres
  (port 5432), never at a transaction-mode pooler (PgBouncer/RDS Proxy) —
  migrations run raw DDL. See `docs/ops/database-connection-pooling.md`.
- The Alembic step is skipped automatically when `POSTGRES_URI` is unset;
  ClickHouse migrations always run.
