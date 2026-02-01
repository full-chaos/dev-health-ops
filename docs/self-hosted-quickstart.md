# Self-Hosted Quickstart

Deploy Dev Health Platform on your own infrastructure in under 30 minutes.

## License Requirements

Before deploying, ensure you comply with the [Business Source License](../LICENSE.md):

| Organization Size | License Required |
|-------------------|------------------|
| <10 employees AND <$1M revenue | Free (Community) |
| ≥10 employees OR ≥$1M revenue | Commercial License |

For commercial licensing: [fullchaos.studio/pricing](https://fullchaos.studio/pricing)

---

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   dev-health-   │────▶│   dev-health-   │────▶│   ClickHouse    │
│      web        │     │      ops        │     │   (Analytics)   │
│   (Frontend)    │     │   (API/CLI)     │     └─────────────────┘
└─────────────────┘     └─────────────────┘              │
        │                       │                        │
        │                       ▼                        ▼
        │               ┌─────────────────┐     ┌─────────────────┐
        └──────────────▶│    Postgres     │     │    Grafana      │
                        │  (App State)    │     │  (Dashboards)   │
                        └─────────────────┘     └─────────────────┘
```

**Components:**
- **dev-health-web**: Next.js frontend (port 3000)
- **dev-health-ops**: FastAPI backend + CLI (port 8000)
- **ClickHouse**: Analytics database (port 8123)
- **PostgreSQL**: Application state, users, teams (port 5432)
- **Grafana**: Optional dashboards (port 3001)

---

## Option 1: Docker Compose (Recommended)

### Prerequisites

- Docker 24+ with Compose v2
- 4GB RAM minimum (8GB recommended)
- 20GB disk space

### Step 1: Clone the repositories

```bash
git clone https://github.com/full-chaos/dev-health-ops.git
git clone https://github.com/full-chaos/dev-health-web.git
cd dev-health-ops
```

### Step 2: Start the stack

```bash
docker compose up -d
```

This starts:
- ClickHouse on `localhost:8123`
- PostgreSQL on `localhost:5432`
- Grafana on `localhost:3001`

### Step 3: Run database migrations

```bash
# Set up the database schema
export DATABASE_URI="postgresql+asyncpg://postgres:postgres@localhost:5432/devhealth"
alembic upgrade head
```

### Step 4: Start the API

```bash
# Using Docker
docker run --rm -p 8000:8000 \
  --network dev-health-ops_default \
  -e DATABASE_URI="clickhouse://default:@clickhouse:8123/default" \
  -e POSTGRES_URI="postgresql+asyncpg://postgres:postgres@postgres:5432/devhealth" \
  ghcr.io/full-chaos/dev-health-ops/api:latest

# Or locally with pip
pip install dev-health-ops
dev-hops api --db "clickhouse://localhost:8123/default" --reload
```

### Step 5: Start the web frontend

```bash
cd ../dev-health-web
npm install
BACKEND_URL="http://127.0.0.1:8000" npm run dev
```

### Step 6: Access the platform

- **Web App**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs
- **Grafana**: http://localhost:3001 (admin/admin)

---

## Option 2: Kubernetes (Helm)

### Prerequisites

- Kubernetes 1.25+
- Helm 3.10+
- kubectl configured

### Step 1: Add the Helm repository

```bash
helm repo add fullchaos https://charts.fullchaos.studio
helm repo update
```

### Step 2: Create values file

```yaml
# values.yaml
global:
  domain: devhealth.your-company.com

clickhouse:
  persistence:
    size: 50Gi

postgresql:
  auth:
    password: your-secure-password
  persistence:
    size: 10Gi

api:
  replicas: 2
  resources:
    requests:
      memory: 512Mi
      cpu: 250m

web:
  replicas: 2
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: devhealth.your-company.com
        paths:
          - path: /
            pathType: Prefix
```

### Step 3: Install

```bash
helm install dev-health fullchaos/dev-health-platform \
  -f values.yaml \
  --namespace dev-health \
  --create-namespace
```

### Step 4: Verify deployment

```bash
kubectl get pods -n dev-health
kubectl get ingress -n dev-health
```

---

## Option 3: Manual Installation

### Prerequisites

- Python 3.11+
- Node.js 20+
- ClickHouse server
- PostgreSQL 15+

### Step 1: Install the CLI/API

```bash
pip install dev-health-ops
```

### Step 2: Configure environment

```bash
# ~/.bashrc or ~/.zshrc
export DATABASE_URI="clickhouse://localhost:8123/default"
export POSTGRES_URI="postgresql+asyncpg://user:pass@localhost:5432/devhealth"
export AUTH_SECRET="$(openssl rand -base64 32)"
```

### Step 3: Initialize databases

```bash
# ClickHouse tables are auto-created
# PostgreSQL requires migrations
cd dev-health-ops
alembic upgrade head
```

### Step 4: Start services

```bash
# Terminal 1: API
dev-hops api --db "$DATABASE_URI" --host 0.0.0.0 --port 8000

# Terminal 2: Web
cd dev-health-web
npm install && npm run build
BACKEND_URL="http://localhost:8000" npm start
```

---

## Initial Setup

### 1. Create admin user and organization

Users must belong to an organization to log in. Create both in sequence:

```bash
# Create admin user
python -m dev_health_ops.cli admin users create \
  --email admin@your-company.com \
  --password "secure-password" \
  --superuser

# Create organization with admin as owner (REQUIRED for login)
python -m dev_health_ops.cli admin orgs create \
  --name "Your Company" \
  --owner-email admin@your-company.com \
  --tier free
```

> **Note:** The `--owner-email` flag automatically adds the user as an organization owner, which is required for authentication.

### 2. Sync your first repository

```bash
# GitHub
export GITHUB_TOKEN="ghp_xxxx"
dev-hops sync git --provider github \
  --db "$DATABASE_URI" \
  --owner your-org \
  --repo your-repo

# Or local Git
dev-hops sync git --provider local \
  --db "$DATABASE_URI" \
  --repo-path /path/to/repo
```

### 3. Compute metrics

```bash
dev-hops metrics daily \
  --db "$DATABASE_URI" \
  --date $(date +%Y-%m-%d) \
  --backfill 30
```

### 4. (Optional) Sync work items

```bash
# Jira
export JIRA_EMAIL="you@company.com"
export JIRA_API_TOKEN="xxx"
export JIRA_URL="https://your-company.atlassian.net"

dev-hops sync work-items --provider jira \
  --db "$DATABASE_URI" \
  --backfill 30

# GitHub Issues
dev-hops sync work-items --provider github \
  --db "$DATABASE_URI" \
  -s "your-org/*"
```

---

## Environment Variables Reference

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URI` | ClickHouse connection | `clickhouse://localhost:8123/default` |
| `AUTH_SECRET` | JWT signing key | `openssl rand -base64 32` |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_URI` | PostgreSQL for app state | SQLite fallback |
| `BACKEND_URL` | API URL for web frontend | `http://127.0.0.1:8000` |
| `GITHUB_TOKEN` | GitHub API access | — |
| `GITLAB_TOKEN` | GitLab API access | — |
| `JIRA_EMAIL` | Jira authentication | — |
| `JIRA_API_TOKEN` | Jira API token | — |
| `JIRA_URL` | Jira instance URL | — |

---

## Upgrading

### Docker Compose

```bash
docker compose pull
docker compose up -d
```

### Helm

```bash
helm repo update
helm upgrade dev-health fullchaos/dev-health-platform -n dev-health
```

### pip

```bash
pip install --upgrade dev-health-ops
alembic upgrade head  # Run migrations
```

---

## Troubleshooting

### API won't start

```bash
# Check ClickHouse connectivity
curl http://localhost:8123/ping

# Check PostgreSQL
psql "$POSTGRES_URI" -c "SELECT 1"
```

### Web shows "Auth Error"

```bash
# Ensure AUTH_SECRET is set
echo $AUTH_SECRET

# Check API is reachable from web container
curl http://localhost:8000/health
```

### Metrics not appearing

```bash
# Verify data was synced
dev-hops sync git --provider local --db "$DATABASE_URI" --repo-path . --dry-run

# Check metrics computation
dev-hops metrics daily --db "$DATABASE_URI" --date $(date +%Y-%m-%d) --verbose
```

---

## Support

- **Community**: [GitHub Discussions](https://github.com/full-chaos/dev-health-ops/discussions)
- **Issues**: [GitHub Issues](https://github.com/full-chaos/dev-health-ops/issues)
- **Commercial Support**: support@fullchaos.studio

---

## Next Steps

1. [Configure team mappings](./team-configuration.md)
2. [Set up SSO/SAML](./sso-setup.md) (Enterprise)
3. [Configure alerting](./alerting.md)
4. [API documentation](https://api.fullchaos.studio/docs)
