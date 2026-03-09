# CLI Reference

Complete reference for the dev-health-ops command-line interface.

---

## Overview

The CLI is implemented in `cli.py` and orchestrates:
- Data synchronization from providers
- Metric computation
- Fixture generation
- Team management

---

## Global Arguments

| Argument | Environment Variable | Description |
|----------|---------------------|-------------|
| `--db` | `POSTGRES_URI` | PostgreSQL connection (semantic: users, settings) |
| `--analytics-db` | `CLICKHOUSE_URI` | ClickHouse connection (analytics: metrics, data) |

Subcommands like `metrics daily` also accept `--sink` for output format (`clickhouse`, `mongo`, `sqlite`, `postgres`, `both`, `auto`).

### Dual-Database Architecture

Dev Health Ops uses two databases:

| Layer | Database | Env Var | Purpose |
|-------|----------|---------|---------|
| **Semantic** | PostgreSQL | `POSTGRES_URI` | Users, orgs, settings, credentials |
| **Analytics** | ClickHouse | `CLICKHOUSE_URI` | Commits, PRs, work items, metrics |

See [Database Architecture](../architecture/database-architecture.md) for details.

### Database Connection Strings

| Backend | Format | Example |
|---------|--------|---------|
| PostgreSQL | `postgresql+asyncpg://` | `postgresql+asyncpg://localhost:5555/postgres` |
| ClickHouse | `clickhouse://` | `clickhouse://localhost:8123/default` |

---

## Sync Commands

### `sync git`

Sync git repository data. Uses `CLICKHOUSE_URI` (analytics layer).

```bash
# Local repository
dev-hops sync git --provider local \
  --repo-path /path/to/repo

# GitHub
dev-hops sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# GitLab
dev-hops sync git --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964
```

**Options:**
| Option | Description |
|--------|-------------|
| `--provider` | `local`, `github`, `gitlab` |
| `--repo-path` | Path to local repo |
| `--owner`, `--repo` | GitHub owner/repo |
| `--project-id` | GitLab project ID |
| `--since`, `--date` | Date filter |
| `--backfill N` | Days to backfill |

### `sync prs`

Sync pull request data. Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync prs --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync work-items`

Sync work items from issue trackers. Uses `CLICKHOUSE_URI`.

```bash
# All providers
dev-hops sync work-items --provider all \
  --date 2025-02-01 \
  --backfill 30

# Jira only
dev-hops sync work-items --provider jira

# GitHub with pattern
dev-hops sync work-items --provider github \
  -s "org/*"

# Linear (all teams)
dev-hops sync work-items --provider linear

# Linear (specific team by key)
dev-hops sync work-items --provider linear \
  --repo ENG
```

**Providers:** `jira`, `github`, `gitlab`, `linear`, `synthetic`, `all`

### `sync cicd`

Sync CI/CD pipeline data. Uses `CLICKHOUSE_URI`.

```bash
# GitHub
dev-hops sync cicd --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo

# GitLab
dev-hops sync cicd --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

### `sync deployments`

Sync deployment events. Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync deployments --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync incidents`

Sync incident data. Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync incidents --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync teams`

Sync team definitions.

```bash
# From config file
dev-hops sync teams --path src/dev_health_ops/config/team_mapping.yaml

# From Jira projects
dev-hops sync teams --provider jira

# Synthetic teams
dev-hops sync teams --provider synthetic

# From GitHub org (requires --owner and token)
dev-hops sync teams --provider github \
  --owner my-org \
  --auth "$GITHUB_TOKEN"

# From GitLab group (fetches group + subgroups)
dev-hops sync teams --provider gitlab \
  --owner my-group/path \
  --auth "$GITLAB_TOKEN"
```

---

## Metrics Commands

### `metrics daily`

Compute daily metrics. Uses `CLICKHOUSE_URI`.

```bash
# Single day
dev-hops metrics daily \
  --date 2025-02-01

# With backfill
dev-hops metrics daily \
  --date 2025-02-01 \
  --backfill 7

# Filter to one repo
dev-hops metrics daily \
  --date 2025-02-01 \
  --repo-id <uuid>

# Specify output format
dev-hops metrics daily \
  --date 2025-02-01 \
  --sink clickhouse
```

**Options:**
| Option | Description |
|--------|-------------|
| `--date` | Target date |
| `--backfill N` | Compute N days ending at date |
| `--repo-id` | Filter to specific repository |

---

## Audit Commands

Diagnostic audits for data completeness, schema integrity, provider coverage, and query performance.

### `audit completeness`

Check data freshness and completeness across providers within a time window.

```bash
# Table output (default)
dev-hops audit completeness --db "clickhouse://localhost:8123/default" --days 7

# JSON output
dev-hops audit completeness --db "clickhouse://localhost:8123/default" --days 30 --format json
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | Database connection string (required) |
| `--days N` | Lookback window in days (default: 7) |
| `--format` | Output format: `table` or `json` (default: `table`) |

Checks work items, transitions, git commits, PRs, deployments, incidents, and CI pipeline runs across providers (jira, github, gitlab, synthetic). Reports staleness and missing data.

### `audit schema`

Verify the database schema matches expected migrations (tables, columns, types).

```bash
dev-hops audit schema
```

Supports ClickHouse (compares against SQL migration files) and PostgreSQL/SQLite (compares against SQLAlchemy model definitions). Reports missing tables, missing columns, and type mismatches with migration file hints.

### `audit perf`

Find slow queries in the ClickHouse query log.

```bash
# Default: queries > 1000ms in the last 60 minutes
dev-hops audit perf

# Custom thresholds
dev-hops audit perf --threshold 500 --lookback 120 --limit 50
```

**Options:**
| Option | Description |
|--------|-------------|
| `--threshold` | Slow query threshold in ms (default: 1000) |
| `--lookback` | Lookback window in minutes (default: 60) |
| `--limit` | Max queries to display (default: 20) |

### `audit coverage`

Audit provider implementation coverage -- checks that collectors, config, schema, sinks, and CLI commands are wired up for each provider.

```bash
# All providers
dev-hops audit coverage --db "clickhouse://localhost:8123/default"

# Specific providers
dev-hops audit coverage --db "clickhouse://localhost:8123/default" --provider jira,github

# JSON output
dev-hops audit coverage --db "clickhouse://localhost:8123/default" --format json
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | Database connection string (required) |
| `--provider` | Comma-separated provider list (default: all) |
| `--format` | Output format: `table` or `json` (default: `table`) |

---

## Fixtures Commands

### `fixtures generate`

Generate synthetic test data. Uses `CLICKHOUSE_URI`.

```bash
# Basic generation
dev-hops fixtures generate --days 30

# Full generation with metrics and work graph
dev-hops fixtures generate \
  --sink "$CLICKHOUSE_URI" \
  --repo-name "acme/demo-app" \
  --repo-count 3 \
  --days 60 \
  --commits-per-day 10 \
  --pr-count 40 \
  --seed 42 \
  --with-metrics \
  --with-work-graph \
  --team-count 8
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--sink` | `$CLICKHOUSE_URI` | Analytics sink URI (ClickHouse) |
| `--db-type` | auto-detect | Explicit DB type (`postgres`, `clickhouse`, etc.) |
| `--repo-name` | `acme/demo-app` | Base repository name |
| `--repo-count` | `1` | Number of repos to generate |
| `--days` | `30` | Number of days of historical data |
| `--commits-per-day` | `5` | Average commits per day |
| `--pr-count` | `20` | Total pull requests to generate |
| `--seed` | random | Deterministic seed for repeatable runs |
| `--provider` | `synthetic` | Provider label: `synthetic`, `github`, `gitlab`, `jira` |
| `--with-metrics` | off | Also generate derived metrics (daily, DORA, complexity, investment, etc.) |
| `--with-work-graph` | off | Build work graph edges after generation (ClickHouse only) |
| `--team-count` | `8` | Number of synthetic teams to create |

### `fixtures validate`

Validate that fixture data is sufficient for work graph and investment analysis.

```bash
dev-hops fixtures validate --sink "clickhouse://localhost:8123/default"
```

**Options:**
| Option | Description |
|--------|-------------|
| `--sink` | Analytics sink URI (required, ClickHouse only) |

Checks raw data counts, team mappings, cycle time metrics, work graph edges, connected components, and evidence bundle quality.

---

## Admin Commands

User and organization management commands. These use PostgreSQL (`POSTGRES_URI`).

> **Important:** Users must belong to an organization to log in. Always create an organization after creating a user.

### `admin users create`

Create a new user.

```bash
python -m dev_health_ops.cli admin users create \
  --email admin@example.com \
  --password secretpass123 \
  --full-name "Admin User" \
  --superuser
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | PostgreSQL URI override (or set `POSTGRES_URI`) |
| `--email` | User email (required) |
| `--password` | Password, min 8 chars (required) |
| `--username` | Optional username |
| `--full-name` | User's full name |
| `--superuser` | Grant superuser privileges |

### `admin orgs create`

Create a new organization. Uses `POSTGRES_URI`.

```bash
python -m dev_health_ops.cli admin orgs create \
  --name "My Organization" \
  --owner-email admin@example.com \
  --tier free
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | PostgreSQL URI override (or set `POSTGRES_URI`) |
| `--name` | Organization name (required) |
| `--slug` | URL-safe slug (auto-generated if omitted) |
| `--description` | Organization description |
| `--tier` | Subscription tier (default: `free`) |
| `--owner-email` | Email of initial owner |

### `admin users list`

List all users.

```bash
python -m dev_health_ops.cli admin users list --limit 50
```

### `admin orgs list`

List all organizations.

```bash
python -m dev_health_ops.cli admin orgs list --include-inactive
```

---

## Batch Processing Options

For GitHub/GitLab batch operations:

| Option | Description |
|--------|-------------|
| `-s, --search PATTERN` | Glob pattern for repos |
| `--group NAME` | Organization/group name |
| `--batch-size N` | Records per batch |
| `--max-concurrent N` | Concurrent workers |
| `--max-repos N` | Maximum repos to process |
| `--use-async` | Enable async workers |
| `--rate-limit-delay SECONDS` | Delay between requests |

---

## Environment Variables

### Database

| Variable | Description |
|----------|-------------|
| `POSTGRES_URI` | PostgreSQL connection (semantic layer: users, settings) |
| `CLICKHOUSE_URI` | ClickHouse connection (analytics layer: metrics, data) |
| `DATABASE_URI` | Legacy fallback (deprecated) |
| `DB_ECHO` | Enable SQL logging |

### Provider Auth

| Variable | Provider |
|----------|----------|
| `GITHUB_TOKEN` | GitHub |
| `GITLAB_TOKEN` | GitLab |
| `JIRA_EMAIL` | Jira |
| `JIRA_API_TOKEN` | Jira |
| `JIRA_BASE_URL` | Jira |
| `LINEAR_API_KEY` | Linear |

### Linear Options

| Variable | Default | Description |
|----------|---------|-------------|
| `LINEAR_FETCH_COMMENTS` | `true` | Fetch issue comments |
| `LINEAR_FETCH_HISTORY` | `true` | Fetch status change history |
| `LINEAR_FETCH_CYCLES` | `true` | Fetch cycles as sprints |
| `LINEAR_COMMENTS_LIMIT` | `100` | Max comments per issue |

### Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | 100 | Records per batch |
| `MAX_WORKERS` | 4 | Parallel workers |

---

## Workflow Examples

### Full Sync Pipeline

```bash
# Set environment variables
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"

# 1. Sync git data
dev-hops sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 2. Sync work items
dev-hops sync work-items --provider jira \
  --date 2025-02-01 \
  --backfill 30

# 3. Compute metrics
dev-hops metrics daily \
  --date 2025-02-01 \
  --backfill 30
```

### Local Development

```bash
# Use SQLite for local dev (analytics layer)
export CLICKHOUSE_URI="sqlite+aiosqlite:///./dev.db"

# Generate synthetic data
dev-hops fixtures generate --days 30

# Compute metrics
dev-hops metrics daily --backfill 30
```

### Batch Organization Sync

```bash
# Sync all repos in org
dev-hops sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  -s "myorg/*" \
  --group myorg \
  --max-concurrent 4 \
  --use-async
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Authentication error |
| 4 | Rate limit exceeded |
