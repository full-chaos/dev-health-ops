# Database Architecture

Dev Health Ops uses a dual-database architecture separating **semantic** (operational) data from **analytics** data.

## Overview

```
┌────────────────────────────────────────────────────────────────────┐
│                         Dev Health Ops                              │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────────────┐       ┌─────────────────────┐            │
│   │   PostgreSQL        │       │   ClickHouse        │            │
│   │   (Semantic Layer)  │       │   (Analytics Layer) │            │
│   ├─────────────────────┤       ├─────────────────────┤            │
│   │ • Users             │       │ • Commits           │            │
│   │ • Organizations     │       │ • Pull Requests     │            │
│   │ • Memberships       │       │ • Work Items        │            │
│   │ • Settings          │       │ • CI/CD Pipelines   │            │
│   │ • Credentials       │       │ • Deployments       │            │
│   │ • Sync Configs      │       │ • Incidents         │            │
│   │ • Identity Mappings │       │ • Daily Metrics     │            │
│   │ • Team Mappings     │       │ • DORA Metrics      │            │
│   │ • Alembic Migrations│       │ • Complexity Data   │            │
│   └─────────────────────┘       └─────────────────────┘            │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

## Analytics Backend Requirement

> **ClickHouse is required for all analytics features.** MongoDB, PostgreSQL, and SQLite are deprecated as analytics backends and will be removed in a future release.

Analytics queries use ClickHouse-specific features (ARRAY JOIN, JSONExtract, argMax) that have no equivalent in other backends. Attempting to use a non-ClickHouse backend for analytics endpoints returns a clear validation error (ValueError) directing users to configure `CLICKHOUSE_URI`.

### What requires ClickHouse

| Feature | Requires ClickHouse | Notes |
|---------|---------------------|-------|
| Investment breakdown/sunburst | Yes | Uses ARRAY JOIN on distribution JSON |
| Sankey flow diagrams | Yes | Uses JSONExtract for structural evidence |
| GraphQL analytics API | Yes | All queries compile to ClickHouse SQL |
| Work unit detail (investment) | Yes | Uses argMax for deduplication |
| Metrics daily computation | Yes | Primary sink target |
| Data sync (git, PRs, work items) | Yes | ClickHouse is the ingest target |

### What does NOT require ClickHouse

| Feature | Backend | Notes |
|---------|---------|-------|
| User management | PostgreSQL | Semantic layer |
| Organization settings | PostgreSQL | Semantic layer |
| Team configuration | PostgreSQL | Semantic layer |
| Authentication | PostgreSQL | Semantic layer |

## Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `POSTGRES_URI` | Semantic layer (users, settings, config) | `postgresql+asyncpg://user:pass@localhost:5432/devhealth` |
| `CLICKHOUSE_URI` | Analytics layer (commits, metrics) | `clickhouse://ch:ch@localhost:8123/default` |
| `DATABASE_URI` | Legacy fallback → `CLICKHOUSE_URI` | (deprecated, use specific vars) |

### Resolution Order

1. Specific variable (`POSTGRES_URI` or `CLICKHOUSE_URI`)
2. Fallback to `DATABASE_URI` (for backward compatibility)
3. Fallback to `DATABASE_URL` (common convention)

## Database Responsibilities

### PostgreSQL (Semantic Layer)

Stores operational data that requires:

- **ACID transactions** (user creation, membership changes)
- **Relational integrity** (foreign keys between users/orgs/memberships)
- **Schema migrations** (Alembic)
- **Row-level security** (future: RBAC enforcement)

**Tables:**

| Table | Purpose |
|-------|---------|
| `users` | User accounts and authentication |
| `organizations` | Multi-tenant organizations |
| `memberships` | User-org relationships and roles |
| `settings` | Org-scoped configuration |
| `integration_credentials` | Encrypted provider credentials |
| `sync_configurations` | Data sync job configs |
| `identity_mappings` | Cross-provider identity resolution |
| `team_mappings` | Team definitions and repo patterns |

### ClickHouse (Analytics Layer)

Stores time-series and event data optimized for:

- **High-volume inserts** (commits, PRs, work items)
- **Analytical queries** (aggregations, time-series)
- **Column-oriented storage** (efficient for metrics)
- **No schema migrations** (append-only, evolving schemas)

**Tables:**

| Table | Purpose |
|-------|---------|
| `repos` | Repository metadata |
| `commits` | Git commit data |
| `commit_stats` | Per-file commit statistics |
| `pull_requests` | PR lifecycle data |
| `work_items` | Jira/GitHub/GitLab issues |
| `repo_metrics_daily` | Daily repository metrics |
| `user_metrics_daily` | Daily per-user metrics |
| `team_metrics_daily` | Daily team aggregates |
| `work_item_metrics_daily` | Work item throughput metrics |

## CLI Command Routing

Commands automatically use the appropriate database:

| Command | Database | Notes |
|---------|----------|-------|
| `admin create-user` | PostgreSQL | User management |
| `admin create-org` | PostgreSQL | Organization management |
| `admin list-users` | PostgreSQL | |
| `admin list-orgs` | PostgreSQL | |
| `sync git` | ClickHouse | Git data ingestion |
| `sync prs` | ClickHouse | PR data ingestion |
| `sync work-items` | ClickHouse | Work item ingestion |
| `sync teams` | PostgreSQL | Team config (semantic) |
| `metrics daily` | ClickHouse | Metrics computation |
| `metrics dora` | ClickHouse | DORA metrics |
| `api` | Both | Serves both layers |

## API Service Routing

| Service | Database | Session Factory |
|---------|----------|-----------------|
| `UserService` | PostgreSQL | `get_postgres_session()` |
| `OrganizationService` | PostgreSQL | `get_postgres_session()` |
| `MembershipService` | PostgreSQL | `get_postgres_session()` |
| `SettingsService` | PostgreSQL | `get_postgres_session()` |
| `AuthService` | PostgreSQL | `get_postgres_session()` |
| GraphQL Resolvers | ClickHouse | `get_clickhouse_session()` |
| Metrics API | ClickHouse | `get_clickhouse_session()` |

## Connection Strings

### PostgreSQL

```bash
# Async (for FastAPI/SQLAlchemy async)
postgresql+asyncpg://user:password@host:5432/database

# Sync (for Alembic migrations)
postgresql://user:password@host:5432/database
```

### ClickHouse

```bash
# HTTP interface (default)
clickhouse://user:password@host:8123/database

# Native interface
clickhouse+native://user:password@host:9000/database
```

## Local Development

```bash
# Start databases
docker compose up -d postgres clickhouse redis

# Set environment
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"

# Run PostgreSQL migrations
cd src/dev_health_ops && alembic upgrade head

# Create initial admin user
python -m dev_health_ops.cli admin create-user \
  --email admin@example.com \
  --password secretpass \
  --superuser

# Create organization
python -m dev_health_ops.cli admin create-org \
  --name "My Org" \
  --owner-email admin@example.com

# Sync analytics data
python -m dev_health_ops.cli sync git --provider local --repo-path /path/to/repo
```

## Migration Strategy

### From Single DATABASE_URI

If migrating from a single-database setup:

1. **Analytics data stays in ClickHouse** - No migration needed
2. **Semantic data moves to PostgreSQL**:
   - Run Alembic migrations to create tables
   - Re-create users/orgs via CLI or API
   - Update environment variables

### Alembic (PostgreSQL Only)

Alembic migrations only apply to PostgreSQL:

```bash
export DATABASE_URI="postgresql+asyncpg://..."  # Alembic reads this
cd src/dev_health_ops
alembic upgrade head
```

## Security Considerations

- **Credentials encryption**: `integration_credentials` stores encrypted secrets (Fernet)
- **Password hashing**: User passwords use PBKDF2-SHA256 with random salt
- **Connection pooling**: Both databases use connection pools with health checks
- **Separate access**: PostgreSQL and ClickHouse can have different network rules
