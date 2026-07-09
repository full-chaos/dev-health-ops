# Deployment Guide

This guide covers deploying dev-health-ops across different container orchestration platforms.

---

## Quick Links

| Platform | Config Location | Quick Start |
|----------|-----------------|-------------|
| Kubernetes | `deploy/kubernetes/` | [Jump to section](#kubernetes-deployment) |
| Docker Compose | `deploy/docker-compose/` | [Jump to section](#docker-compose-deployment) |
| Docker Swarm | `deploy/docker-swarm/` | [Jump to section](#docker-swarm-deployment) |
| Local Development | `compose.yml` | [Jump to section](#local-development) |

---

## Prerequisites

### Required

- Docker 20.10+ (for all deployment methods)
- Container registry access (for pulling images)
- Network access to GitHub, GitLab, and/or Jira APIs

### Platform-Specific

| Platform | Requirements |
|----------|--------------|
| Kubernetes | kubectl, Kubernetes 1.25+, Ingress Controller |
| Docker Compose | Docker Compose V2 |
| Docker Swarm | Docker Swarm initialized |

---

## Environment Variables

All deployment methods use the same environment variables:

### Database

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URI` | Primary database connection | Required |
| `SECONDARY_DATABASE_URI` | Secondary sink (optional) | - |

### Provider Credentials

| Variable | Description |
|----------|-------------|
| `GITHUB_TOKEN` | GitHub Personal Access Token |
| `GITHUB_APP_SLUG` | GitHub App URL slug for one-click install/connect |
| `GITHUB_APP_ID` | GitHub App id for app-based auth |
| `GITHUB_APP_CLIENT_ID` | GitHub App OAuth Client ID for installation authorization |
| `GITHUB_APP_CLIENT_SECRET` | GitHub App OAuth Client secret for installation authorization |
| `GITHUB_APP_CALLBACK_URL` | Exact web callback for GitHub App install, for example `https://app.example.com/org/admin/integrations/github-app/callback` |
| `GITHUB_APP_PRIVATE_KEY_PATH` | Path to a mounted GitHub App private key |
| `GITHUB_APP_PRIVATE_KEY` | Inline GitHub App private key for the API GitHub App integration config path; worker sync env fallback uses `GITHUB_APP_PRIVATE_KEY_PATH` |
| `GITHUB_APP_INSTALLATION_ID` | GitHub App installation id for single-installation fallback |
| `GITHUB_BASE_URL` | GitHub API base URL |
| `GITHUB_LINEAR_LINKBACK_BOTS` | Comma-separated GitHub bot actors trusted for Linear linkback comments |
| `SOCIAL_GITHUB_CLIENT_ID` | GitHub OAuth Client ID used by `/auth/social-login`; may be the same value as `GITHUB_APP_CLIENT_ID` |
| `SOCIAL_GITHUB_CLIENT_SECRET` | GitHub OAuth Client secret used by `/auth/social-login`; may be the same value as `GITHUB_APP_CLIENT_SECRET` |
| `GITLAB_TOKEN` | GitLab Private Token |
| `GITLAB_URL` | GitLab instance URL (default: gitlab.com) |
| `JIRA_BASE_URL` | Jira Cloud URL (e.g., your-org.atlassian.net) |
| `JIRA_EMAIL` | Jira account email |
| `JIRA_API_TOKEN` | Jira API token |
| `LINEAR_API_KEY` | Linear API key for Linear work-item sync |
| `LINEAR_TRUSTED_SCM_HOSTS` | Additional self-hosted SCM hosts trusted in Linear issue attachment links |

### Application

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAPHQL_QUERY_TIMEOUT` | GraphQL timeout (seconds) | 30 |
| `LOG_LEVEL` | Logging verbosity | INFO |
| `BATCH_SIZE` | Records per batch | 100 |
| `MAX_WORKERS` | Parallel workers | 4 |

### Sync Routing and Budgets

| Variable | Description | Default |
|----------|-------------|---------|
| `PROVIDER_SYNC_QUEUES_ENABLED` | Route provider sync units to `sync.<provider>` queues. Enable only after workers consume those queues. | true in bundled deploy templates |
| `SYNC_COST_CLASS_QUEUES` | Route eligible sync units to `sync.<provider>.<class>` sub-queues. Requires provider queues. | true in bundled deploy templates |
| `HIDE_MIGRATED_CHILD_CONFIGS` | Hide migrated child sync configs from operator-facing lists. | true |
| `SYNC_RUN_MAX_UNITS` | Maximum units allowed in one planned sync run. | 1000 |
| `SYNC_UNIT_CONCURRENCY_PER_BUCKET` | Concurrent dispatch cap per org/provider/cost-class bucket. | 8 |
| `SYNC_UNIT_DISPATCH_STALE_SECONDS` | Age after which `DISPATCHING` units can be reclaimed. | 900 |
| `SYNC_UNIT_RUNNING_STALE_SECONDS` | Age after which running units are treated as stale for reconciliation/reporting. | 3600 |
| `LINEAR_BACKFILL_MAX_WINDOW_DAYS` | Max window size (days) for a Linear work-item-family backfill chunk. CHAOS-2717 bounds each window's issue crawl to its own slice (`updatedAt` gte/lte), so the size balances a single unit's lease/soft-timeout budget against per-hour request volume; smaller windows re-multiply per-window teams/cycles fetches toward Linear's rate limit. Non-Linear backfills use the 7-day default. | 14 |
| `SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES` | Max times an eligible Linear work-item backfill unit may be retried after an expired lease (or soft-timeout) before it is marked terminal `FAILED` (`worker_lost_retry_exhausted`). Retry is DISABLED on every other surface. | 1 |
| `SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS` | Backoff added to `available_at` when an eligible expired-lease unit is flipped to `RETRYING`, before it is redispatched. | 60 |
| `SYNC_DISPATCH_REDISPATCH_COUNTDOWN` | Delay before redispatching sync-run work. | 60 |
| `SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS` | Dispatch outbox claim lease duration. | 300 |
| `SYNC_WATERMARK_OVERLAP` | Seconds subtracted from incremental watermark reads to intentionally re-read a lookback margin. | 0 |
| `SYNC_BUDGET_BUCKET_LIMITS` | JSON map that enables enforced provider budget deferrals. Values are reservation units, not raw request or GraphQL cost counters. Jira supports route-family keys such as `jira:search:jira_jql`, `jira:rest_core:jira_issue_enrichment`, `jira:rest_core:jira_worklogs`, and `jira:graphql_cost:jira_gql_enrichment`. | `{"github:rest_core":250,"github:graphql_cost":500,"github:contents_blob":100,"github:secondary_abuse_risk":25,"jira:search:jira_jql":250,"jira:rest_core:jira_issue_enrichment":250,"jira:rest_core:jira_worklogs":100,"jira:graphql_cost:jira_gql_enrichment":250,"linear:graphql_cost":500}` |
| `SYNC_BUDGET_DEFAULT_LIMIT` | Fallback enforced budget limit for unnamed buckets. | 1000000 |
| `SYNC_BUDGET_DEFERRAL_SECONDS` | Base countdown when budget enforcement defers a unit. | 60 |
| `SYNC_BUDGET_DEFERRAL_JITTER_SECONDS` | Jitter added to budget deferrals. | 5 |
| `SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS` | Optional observation-only provider budget limits. | unset |
| `SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT` | Fallback dry-run budget limit. | 1000000 |
| `SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS` | Observation-only deferral estimate. | 60 |

### Rate Limiting

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL for distributed rate-limit storage. **Required in non-dev environments** — the API will refuse to start without it when `ENVIRONMENT` is not `development`/`local`/`test`. | — |
| `TRUSTED_PROXIES` | Comma-separated list of trusted proxy IPs or CIDRs (e.g. `10.0.0.1,10.0.0.2`). Only peers in this list are allowed to set the `X-Forwarded-For` header for rate-limit key extraction. When unset, `X-Forwarded-For` is ignored and the TCP peer address is used. | — |

> **Security note:** Never leave `TRUSTED_PROXIES` empty behind a load balancer — rate limits would key on the LB IP rather than the real client. Conversely, setting it when running without a proxy would allow header spoofing by any client.
---

## Kubernetes Deployment
See also: [Worker horizontal-scaling readiness](../architecture/worker-scaling-readiness.md)


### File Structure

```
deploy/kubernetes/
├── kustomization.yaml      # Kustomize entry point
├── namespace.yaml          # Namespace definition
├── configmap.yaml          # Application configuration
├── secrets.yaml            # Credentials (template)
├── clickhouse.yaml         # ClickHouse StatefulSet
├── redis.yaml              # Redis Deployment
├── api.yaml                # API Deployment + HPA
├── worker.yaml             # Celery Worker Deployment + HPA
├── cronjobs.yaml           # Scheduled sync jobs
└── ingress.yaml            # Ingress + NetworkPolicy
```

### Quick Start

```bash
cd deploy/kubernetes

kubectl create namespace dev-health

kubectl create secret generic dev-health-secrets \
  --namespace dev-health \
  --from-literal=GITHUB_TOKEN="$GITHUB_TOKEN" \
  --from-literal=DATABASE_URI="clickhouse://ch:ch@clickhouse:8123/default"

kubectl apply -k .
```

### Using Kustomize Overlays

Create environment-specific overlays:

```
deploy/kubernetes/
├── base/
│   └── kustomization.yaml
├── overlays/
│   ├── production/
│   │   └── kustomization.yaml
│   └── staging/
│       └── kustomization.yaml
```

Example production overlay:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
patchesStrategicMerge:
  - api-patch.yaml
images:
  - name: ghcr.io/your-org/dev-health-ops
    newTag: v1.2.3
```

### External Secrets

For production, use a secrets manager:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: dev-health-secrets
  namespace: dev-health
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: dev-health-secrets
  data:
    - secretKey: GITHUB_TOKEN
      remoteRef:
        key: dev-health/github
        property: token
```

### Monitoring

The API exposes `/health` for liveness/readiness probes. For metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: dev-health-api
  namespace: dev-health
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: dev-health-api
  endpoints:
    - port: http
      path: /metrics
```

---

## Docker Compose Deployment

### File Structure

```
deploy/docker-compose/
├── compose.production.yml  # Production stack
└── .env.example            # Environment template
```

### Quick Start

```bash
cd deploy/docker-compose

cp .env.example .env

docker compose -f compose.production.yml up -d
```

### Customization

Override specific services:

```bash
docker compose -f compose.production.yml \
  -f compose.override.yml up -d
```

Example `compose.override.yml`:

```yaml
services:
  api:
    deploy:
      replicas: 4
  worker:
    environment:
      - WORKER_CONCURRENCY=8
```

### Running Sync Commands

```bash
docker compose -f compose.production.yml run --rm api \
  dev-hops sync work-items --provider github --backfill 30

docker compose -f compose.production.yml run --rm api \
  dev-hops metrics daily
```

---

## Docker Swarm Deployment

### File Structure

```
deploy/docker-swarm/
├── stack.yml   # Swarm stack definition
└── README.md   # Setup instructions
```

### Quick Start

```bash
docker swarm init

echo "ch_password" | docker secret create clickhouse_password -
echo "$GITHUB_TOKEN" | docker secret create github_token -
echo "$GITLAB_TOKEN" | docker secret create gitlab_token -

docker stack deploy -c deploy/docker-swarm/stack.yml dev-health
```

### Scaling

```bash
docker service scale dev-health_api=4
docker service scale dev-health_worker=4
```

### Updates

Rolling updates with zero downtime:

```bash
docker service update --image ghcr.io/your-org/dev-health-ops:v1.2.3 \
  dev-health_api
```

---

## Local Development

Use the root `compose.yml` for local development:

```bash
docker compose up -d clickhouse redis

pip install -e .

dev-hops api --reload

celery -A workers.celery_app worker --loglevel=debug
```

---

## Storage Backends

### ClickHouse (Recommended)

Optimized for analytics workloads. Connection string:

```
clickhouse://user:password@host:8123/database
```

### PostgreSQL

For smaller deployments or existing Postgres infrastructure:

```
postgresql+asyncpg://user:password@host:5432/database
```

Requires Alembic migrations:

```bash
alembic upgrade head
```

### MongoDB

Document storage option:

```
mongodb://host:27017
```

---

## Scheduled Sync Jobs

### Kubernetes CronJobs

CronJobs are defined in `deploy/kubernetes/cronjobs.yaml`:

| Job | Schedule | Description |
|-----|----------|-------------|
| daily-metrics | 0 2 * * * | Compute daily metrics |
| sync-github | 0 */6 * * * | Sync GitHub work items |
| sync-gitlab | 30 */6 * * * | Sync GitLab work items |
| sync-jira | 0 */4 * * * | Sync Jira work items |

### Docker Compose / Swarm

Use host cron or a separate scheduler service:

```bash
0 2 * * * docker compose -f compose.production.yml run --rm api dev-hops metrics daily
0 */6 * * * docker compose -f compose.production.yml run --rm api dev-hops sync work-items --provider github --backfill 1
```

### GitHub Actions (Runner Container)

Use the `dev-hops-runner` image from `docker/Dockerfile` and repository secrets for credentials. Create separate workflows for sync and metrics schedules.

`sync-work-items.yml`:

```yaml
name: Dev Health Sync
on:
  schedule:
    - cron: "0 */6 * * *"
  workflow_dispatch:

jobs:
  sync-work-items:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/full-chaos/dev-health-ops/dev-hops-runner:latest
    env:
      DATABASE_URI: ${{ secrets.DATABASE_URI }}
      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    steps:
      - name: Sync work items
        run: dev-hops sync work-items --provider github --backfill 1 --db "$DATABASE_URI"
```

`metrics-daily.yml`:

```yaml
name: Dev Health Metrics
on:
  schedule:
    - cron: "0 2 * * *"
  workflow_dispatch:

jobs:
  metrics-daily:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/full-chaos/dev-health-ops/dev-hops-runner:latest
    env:
      DATABASE_URI: ${{ secrets.DATABASE_URI }}
    steps:
      - name: Compute daily metrics
        run: dev-hops metrics daily --db "$DATABASE_URI"
```

### GitLab CI (Runner Container)

Use pipeline schedules and masked CI/CD variables for credentials. Configure schedules in GitLab UI for the cron timing.

```yaml
stages:
  - sync
  - metrics

sync-work-items:
  stage: sync
  image: ghcr.io/full-chaos/dev-health-ops/dev-hops-runner:latest
  script:
    - dev-hops sync work-items --provider gitlab --backfill 1 --db "$DATABASE_URI"
  rules:
    - if: '$CI_PIPELINE_SOURCE == "schedule"'

metrics-daily:
  stage: metrics
  image: ghcr.io/full-chaos/dev-health-ops/dev-hops-runner:latest
  script:
    - dev-hops metrics daily --db "$DATABASE_URI"
  rules:
    - if: '$CI_PIPELINE_SOURCE == "schedule"'
```

---

## Health Checks

| Endpoint | Purpose |
|----------|---------|
| `/health` | API liveness/readiness |
| `/graphql` | GraphQL playground |

### ClickHouse

```bash
wget -q -O- http://clickhouse:8123/ping
```

### Redis

```bash
redis-cli ping
```

---

## Troubleshooting

### API Won't Start

1. Check database connectivity:
   ```bash
   kubectl logs -l app.kubernetes.io/name=dev-health-api
   ```

2. Verify secrets are mounted:
   ```bash
   kubectl exec -it deploy/dev-health-api -- env | grep DATABASE
   ```

### Workers Not Processing

1. Check Celery connection:
   ```bash
   celery -A workers.celery_app inspect ping
   ```

2. Check Redis:
   ```bash
   redis-cli -h redis INFO replication
   ```

### Sync Failures

1. Check provider credentials:
   ```bash
   curl -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/user
   ```

2. Check rate limits:
   ```bash
   curl -I -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/rate_limit
   ```

---

## Security Recommendations

1. **Use secret managers** (Vault, AWS Secrets Manager) instead of plain secrets
2. **Enable TLS** on all endpoints
3. **Restrict network access** using NetworkPolicies
4. **Rotate credentials** regularly
5. **Use read-only tokens** where possible (GitHub, GitLab)
6. **Audit API access** via ingress logs
