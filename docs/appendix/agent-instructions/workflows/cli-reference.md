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
| `--db` | `DATABASE_URI` | Database connection string |
| `--sink` | — | Output target: `primary`, `secondary`, `both` |

### Database Connection Strings

| Backend | Format | Example |
|---------|--------|---------|
| PostgreSQL | `postgresql+asyncpg://` | `postgresql+asyncpg://localhost:5432/db` |
| ClickHouse | `clickhouse://` | `clickhouse://localhost:8123/default` |
| MongoDB | `mongodb://` | `mongodb://localhost:27017/db` |
| SQLite | `sqlite+aiosqlite://` | `sqlite+aiosqlite:///./data.db` |

---

## Sync Commands

### `sync git`

Sync git repository data.

```bash
# Local repository
dev-hops sync git --provider local \
  --db "$DATABASE_URI" \
  --repo-path /path/to/repo

# GitHub
dev-hops sync git --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# GitLab
dev-hops sync git --provider gitlab \
  --db "$DATABASE_URI" \
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

Sync pull request data.

```bash
dev-hops sync prs --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync work-items`

Sync work items from issue trackers.

```bash
# All providers
dev-hops sync work-items --provider all \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30

# Jira only
dev-hops sync work-items --provider jira \
  --db "$DATABASE_URI"

# GitHub with pattern
dev-hops sync work-items --provider github \
  --db "$DATABASE_URI" \
  -s "org/*"
```

**Providers:** `jira`, `github`, `gitlab`, `synthetic`, `all`

### `sync cicd`

Sync CI/CD pipeline data.

```bash
# GitHub
dev-hops sync cicd --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo

# GitLab
dev-hops sync cicd --provider gitlab \
  --db "$DATABASE_URI" \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

### `sync deployments`

Sync deployment events.

```bash
dev-hops sync deployments --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync incidents`

Sync incident data.

```bash
dev-hops sync incidents --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync teams`

Sync team definitions.

```bash
# From config file
dev-hops sync teams --path config/team_mapping.yaml

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

Compute daily metrics.

```bash
# Single day
dev-hops metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01

# With backfill
dev-hops metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 7

# Filter to one repo
dev-hops metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --repo-id <uuid>
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

Generate synthetic test data.

```bash
dev-hops fixtures generate \
  --db "$DATABASE_URI" \
  --days 30
```

**Options:**
| Option | Description |
|--------|-------------|
| `--days N` | Number of days to generate |
| `--teams N` | Number of teams |
| `--repos-per-team N` | Repos per team |

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
| `DATABASE_URI` | Primary database connection |
| `SECONDARY_DATABASE_URI` | Secondary sink (with `--sink both`) |
| `DB_ECHO` | Enable SQL logging |

### Provider Auth

| Variable | Provider |
|----------|----------|
| `GITHUB_TOKEN` | GitHub |
| `GITLAB_TOKEN` | GitLab |
| `JIRA_EMAIL` | Jira |
| `JIRA_API_TOKEN` | Jira |
| `JIRA_BASE_URL` | Jira |

### Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | 100 | Records per batch |
| `MAX_WORKERS` | 4 | Parallel workers |

---

## Workflow Examples

### Full Sync Pipeline

```bash
# 1. Sync git data
dev-hops sync git --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 2. Sync work items
dev-hops sync work-items --provider jira \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30

# 3. Compute metrics
dev-hops metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30
```

### Local Development

```bash
# Generate synthetic data
dev-hops fixtures generate \
  --db "sqlite+aiosqlite:///./dev.db" \
  --days 30

# Compute metrics
dev-hops metrics daily \
  --db "sqlite+aiosqlite:///./dev.db" \
  --backfill 30
```

### Batch Organization Sync

```bash
# Sync all repos in org
dev-hops sync git --provider github \
  --db "$DATABASE_URI" \
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
