# Getting started

## Install

If you are using the project as a package:

```bash
pip install dev-health-ops
```

If you are working from source:

```bash
pip install -r requirements.txt
```

## Docs site

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

## Quick start

### Sync a local repository

```bash
dev-hops sync git --provider local --db "<DB_CONN>" --repo-path /path/to/repo
```

### Sync work items from GitHub

```bash
dev-hops sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*" --db "<DB_CONN>"
```

### Compute daily metrics

```bash
dev-hops metrics daily --db "<DB_CONN>"
```

### Bring up Grafana dashboards

```bash
docker compose -f compose.yml up -d
```

## Database Architecture

Dev Health Ops uses a **dual-database architecture**:

| Database | Purpose | Env Var |
|----------|---------|---------|
| **PostgreSQL** | Users, orgs, settings, credentials | `POSTGRES_URI` |
| **ClickHouse** | Commits, PRs, work items, metrics | `CLICKHOUSE_URI` |

```bash
# Start databases
docker compose up -d postgres clickhouse redis

# Set environment
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"

# Run PostgreSQL migrations (for user management)
cd src/dev_health_ops && alembic upgrade head
```

See [Database Architecture](architecture/database-architecture.md) for details.

## Environment notes

CLI flags override environment variables.

| Variable | Status | Purpose |
|----------|--------|---------|
| `POSTGRES_URI` | Required | Semantic data (users, settings, credentials) |
| `CLICKHOUSE_URI` | Required | Analytics data (sync + metrics) |
| `DATABASE_URI` / `DATABASE_URL` | Deprecated fallback | Legacy resolver paths; keep only for compatibility |
| `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_*`, `ATLASSIAN_*`, `LINEAR_API_KEY` | Optional | Provider auth for sync commands |
| `EMAIL_PROVIDER`, `EMAIL_API_KEY`, `EMAIL_FROM_ADDRESS` | Optional (`console` default) | Email delivery via [Resend](./email-setup.md) |
| `APP_BASE_URL`, `JWT_SECRET_KEY`, `SETTINGS_ENCRYPTION_KEY` | Optional in dev, required in production | API callback/auth/encryption settings |
