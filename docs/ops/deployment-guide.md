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
| `GITLAB_TOKEN` | GitLab Private Token |
| `GITLAB_URL` | GitLab instance URL (default: gitlab.com) |
| `JIRA_BASE_URL` | Jira Cloud URL (e.g., your-org.atlassian.net) |
| `JIRA_EMAIL` | Jira account email |
| `JIRA_API_TOKEN` | Jira API token |

### Application

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAPHQL_QUERY_TIMEOUT` | GraphQL timeout (seconds) | 30 |
| `LOG_LEVEL` | Logging verbosity | INFO |
| `BATCH_SIZE` | Records per batch | 100 |
| `MAX_WORKERS` | Parallel workers | 4 |

---

## Kubernetes Deployment

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
