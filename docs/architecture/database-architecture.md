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
│   │ • Alembic Migrations│       │ • Teams + Identities│            │
│   │                     │       │ • Team ownership    │            │
│   │                     │       │   dimensions        │            │
│   │                     │       │ • Daily/DORA Metrics│            │
│   │                     │       │ • Complexity Data   │            │
│   └─────────────────────┘       └─────────────────────┘            │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

> **Teams & identities live in ClickHouse (CHAOS-2600 CS5).** The team catalog
> (`teams`), team→repo/project ownership dimensions, and identity→team
> membership (`identities`) are ClickHouse-resolved at query time. The Postgres
> `team_mappings` / `identity_mappings` tables were **dropped in CS6** (CHAOS-2607);
> ClickHouse is the sole system of record.

## Analytics Backend Requirement

> **ClickHouse is required for all analytics features.** MongoDB, PostgreSQL, and SQLite are deprecated as analytics backends and will be removed in a future release.

Analytics queries use ClickHouse-specific features (ARRAY JOIN, JSONExtract, argMax) that have no equivalent in other backends. Attempting to use a non-ClickHouse backend for analytics endpoints returns a clear validation error (ValueError) directing users to configure `CLICKHOUSE_URI`.

SQLite via `aiosqlite` remains allowed only for test fixtures and local-only ephemeral development. It must not be used for production semantic data, analytics, CI long-run pipelines, or durable environments; the `sqlite+aiosqlite` URL normalization helpers in `src/dev_health_ops/db.py` and `metrics/db_utils.py` exist for that narrow compatibility scope.

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
| Team & identity mapping (`teams`, `identities`) | ClickHouse | System of record as of CS5 (CHAOS-2606). The Postgres `team_mappings`/`identity_mappings` tables were dropped in CS6 (CHAOS-2607). |
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

### ClickHouse (Analytics Layer)

Stores time-series and event data optimized for:

- **High-volume inserts** (commits, PRs, work items)
- **Analytical queries** (aggregations, time-series)
- **Column-oriented storage** (efficient for metrics)
- **Custom schema migrations** (numbered SQL/Python scripts in `migrations/clickhouse/`)

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
| `teams` | Team entities (members; consumed by metrics & attribution) |
| `work_item_metrics_daily` | Work item throughput metrics |

## CLI Command Routing

Commands automatically use the appropriate database:

| Command | Database | Notes |
|---------|----------|-------|
| `admin create-user` | PostgreSQL | User management |
| `admin create-org` | PostgreSQL | Organization management |
| `admin list-users` | PostgreSQL | |
| `admin list-orgs` | PostgreSQL | |
| `migrate postgres` | PostgreSQL | Schema migrations (Alembic) |
| `migrate clickhouse` | ClickHouse | Schema migrations (custom runner) |
| `sync git` | ClickHouse | Git data ingestion |
| `sync prs` | ClickHouse | PR data ingestion |
| `sync work-items` | ClickHouse | Work item ingestion |
| `sync teams` | ClickHouse | Writes ClickHouse `teams` directly. **Org-scoped** (`--org`): rows tagged with `org_id`; **No-org**: untagged. No Postgres team projection (CHAOS-2600 CS5). |
| `metrics daily` | ClickHouse | Metrics computation |
| `metrics dora` | ClickHouse | DORA metrics |
| `api` | Both | Serves both layers |

### `sync teams` routing detail

As of CHAOS-2600 CS5, **ClickHouse is the system of record for teams**. Both routing paths write the ClickHouse `teams` table directly; the only difference is whether rows are tagged with `org_id`:

| Path | Trigger | Write | ClickHouse writer |
|------|---------|-------|-------------------|
| **Org-scoped** | `--org <id>` present | Direct `insert_teams` to ClickHouse `teams`, each row tagged with `org_id` (plus any Jira ops links). No Postgres projection. | `ClickHouseStore.insert_teams` (org-scoped) |
| **No-org** | `--org` absent | Direct `run_with_store` → ClickHouse `teams` table (untagged) | `run_with_store` |

**Invariants:**

- ClickHouse `teams` is the system of record AND the analytics read source for teams. There is no Postgres team control plane.
- The org-scoped path opens a `ClickHouseStore`, sets `store.org_id`, tags each team with the org id, and calls `insert_teams` — mirroring the no-org path with the org tag added. It does **not** project to PostgreSQL `team_mappings` and does **not** call any Postgres→ClickHouse bridge.
- A ClickHouse write failure is fatal (exit 1) so callers cannot report a successful sync when analytics has not been updated.

> **Removed in CS5 (CHAOS-2606):** `bridge_teams_to_clickhouse` + `providers/team_bridge.py`, `providers/team_reconcile.py` + the `dev-hops teams reconcile` command, and the `sync_teams_to_analytics` task are **deleted**. The org-scoped path no longer projects to Postgres. **Removed in CS6 (CHAOS-2607):** the Postgres `team_mappings` / `identity_mappings` models + tables (Alembic `0020`), the `TeamMappingService` / `IdentityMappingService` / `TeamDriftSyncService` classes, the `sync-team-drift` / `reconcile-team-members` tasks, and the Postgres-backed drift engine are **deleted** (the four admin drift-review endpoints remain as HTTP 501 stubs until CS7 removes them with the web caller — CHAOS-2608; a ClickHouse-backed drift-review rebuild is tracked by CHAOS-2622).

#### Team Sync Architecture Diagram (CS5)

Both paths write ClickHouse directly; identity→team membership is ClickHouse-native:

```mermaid
graph TD
    CLI[CLI: sync teams --org] -->|insert_teams org-tagged| CH[(ClickHouse: teams)]
    CLINO[CLI: sync teams no-org] -->|run_with_store| CH
    Admin[Admin API: team/identity CRUD] -->|ClickHouseTeamAdminService / ClickHouseIdentityStore| CH
    Admin -->|surgical team.members updates| CHI[(ClickHouse: identities)]
    AutoImport[Celery: team auto-import] -->|native CH writers| CH

    subgraph "Removed (CS5 + CS6)"
        Bridge[bridge_teams_to_clickhouse - DELETED CS5]
        Reconcile[CLI teams reconcile - DELETED CS5]
        Drift[sync-team-drift / reconcile-team-members tasks - DELETED CS6]
        PG[(PostgreSQL team_mappings / identity_mappings - DROPPED CS6)]
    end
```
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

# Run migrations (both databases)
dev-hops migrate postgres
dev-hops migrate clickhouse

# Create initial admin user
dev-hops admin users create \
  --email admin@example.com \
  --password secretpass \
  --superuser

# Create organization
dev-hops admin orgs create \
  --name "My Org" \
  --owner-email admin@example.com

# Sync analytics data
dev-hops sync git --provider local --repo-path /path/to/repo
```

## Migration Strategy

### From Single DATABASE_URI

If migrating from a single-database setup:

1. **Analytics data stays in ClickHouse** - No migration needed
2. **Semantic data moves to PostgreSQL**:
   - Run Alembic migrations to create tables
   - Re-create users/orgs via CLI or API
   - Update environment variables

### Running Migrations

Both databases have their own migration systems, managed via the CLI:

```bash
# PostgreSQL (Alembic)
dev-hops migrate postgres              # Apply all pending migrations
dev-hops migrate postgres current      # Show current revision

# ClickHouse (custom runner)
dev-hops migrate clickhouse            # Apply all pending migrations
dev-hops migrate clickhouse status     # Show applied/pending migrations
```

> **Important:** Always run `dev-hops migrate clickhouse` after setting up a fresh ClickHouse instance. Unlike PostgreSQL, ClickHouse tables are not auto-created — queries will fail with `Unknown table` errors until migrations are applied.

## Security Considerations

- **Credentials encryption**: `integration_credentials` stores encrypted secrets (Fernet)
- **Password hashing**: User passwords use PBKDF2-SHA256 with random salt
- **Connection pooling**: Both databases use connection pools with health checks
- **Separate access**: PostgreSQL and ClickHouse can have different network rules
