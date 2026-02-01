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
python cli.py sync git --provider local \
  --repo-path /path/to/repo

# GitHub
python cli.py sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# GitLab
python cli.py sync git --provider gitlab \
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
python cli.py sync prs --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync work-items`

Sync work items from issue trackers. Uses `CLICKHOUSE_URI`.

```bash
# All providers
python cli.py sync work-items --provider all \
  --date 2025-02-01 \
  --backfill 30

# Jira only
python cli.py sync work-items --provider jira

# GitHub with pattern
python cli.py sync work-items --provider github \
  -s "org/*"

# Linear (all teams)
python cli.py sync work-items --provider linear

# Linear (specific team by key)
python cli.py sync work-items --provider linear \
  --repo ENG
```

**Providers:** `jira`, `github`, `gitlab`, `linear`, `synthetic`, `all`

### `sync cicd`

Sync CI/CD pipeline data. Uses `CLICKHOUSE_URI`.

```bash
# GitHub
python cli.py sync cicd --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo

# GitLab
python cli.py sync cicd --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

### `sync deployments`

Sync deployment events. Uses `CLICKHOUSE_URI`.

```bash
python cli.py sync deployments --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync incidents`

Sync incident data. Uses `CLICKHOUSE_URI`.

```bash
python cli.py sync incidents --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync teams`

Sync team definitions.

```bash
# From config file
python cli.py sync teams --path config/team_mapping.yaml

# From Jira projects
python cli.py sync teams --provider jira

# Synthetic teams
python cli.py sync teams --provider synthetic
```

---

## Metrics Commands

### `metrics daily`

Compute daily metrics. Uses `CLICKHOUSE_URI`.

```bash
# Single day
python cli.py metrics daily \
  --date 2025-02-01

# With backfill
python cli.py metrics daily \
  --date 2025-02-01 \
  --backfill 7

# Filter to one repo
python cli.py metrics daily \
  --date 2025-02-01 \
  --repo-id <uuid>

# Specify output format
python cli.py metrics daily \
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

## Fixtures Commands

### `fixtures generate`

Generate synthetic test data. Uses `CLICKHOUSE_URI`.

```bash
python cli.py fixtures generate --days 30
```

**Options:**
| Option | Description |
|--------|-------------|
| `--days N` | Number of days to generate |
| `--teams N` | Number of teams |
| `--repos-per-team N` | Repos per team |

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
python cli.py sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 2. Sync work items
python cli.py sync work-items --provider jira \
  --date 2025-02-01 \
  --backfill 30

# 3. Compute metrics
python cli.py metrics daily \
  --date 2025-02-01 \
  --backfill 30
```

### Local Development

```bash
# Use SQLite for local dev (analytics layer)
export CLICKHOUSE_URI="sqlite+aiosqlite:///./dev.db"

# Generate synthetic data
python cli.py fixtures generate --days 30

# Compute metrics
python cli.py metrics daily --backfill 30
```

### Batch Organization Sync

```bash
# Sync all repos in org
python cli.py sync git --provider github \
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
