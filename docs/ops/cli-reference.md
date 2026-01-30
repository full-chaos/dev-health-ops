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
python cli.py sync git --provider local \
  --db "$DATABASE_URI" \
  --repo-path /path/to/repo

# GitHub
python cli.py sync git --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner torvalds \
  --repo linux

# GitLab
python cli.py sync git --provider gitlab \
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
python cli.py sync prs --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync work-items`

Sync work items from issue trackers.

```bash
# All providers
python cli.py sync work-items --provider all \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30

# Jira only
python cli.py sync work-items --provider jira \
  --db "$DATABASE_URI"

# GitHub with pattern
python cli.py sync work-items --provider github \
  --db "$DATABASE_URI" \
  -s "org/*"

# Linear (all teams)
python cli.py sync work-items --provider linear \
  --db "$DATABASE_URI"

# Linear (specific team by key)
python cli.py sync work-items --provider linear \
  --db "$DATABASE_URI" \
  --repo ENG
```

**Providers:** `jira`, `github`, `gitlab`, `linear`, `synthetic`, `all`

### `sync cicd`

Sync CI/CD pipeline data.

```bash
# GitHub
python cli.py sync cicd --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo

# GitLab
python cli.py sync cicd --provider gitlab \
  --db "$DATABASE_URI" \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

### `sync deployments`

Sync deployment events.

```bash
python cli.py sync deployments --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner org \
  --repo repo
```

### `sync incidents`

Sync incident data.

```bash
python cli.py sync incidents --provider github \
  --db "$DATABASE_URI" \
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

Compute daily metrics.

```bash
# Single day
python cli.py metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01

# With backfill
python cli.py metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 7

# Filter to one repo
python cli.py metrics daily \
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
python cli.py fixtures generate \
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
# 1. Sync git data
python cli.py sync git --provider github \
  --db "$DATABASE_URI" \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 2. Sync work items
python cli.py sync work-items --provider jira \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30

# 3. Compute metrics
python cli.py metrics daily \
  --db "$DATABASE_URI" \
  --date 2025-02-01 \
  --backfill 30
```

### Local Development

```bash
# Generate synthetic data
python cli.py fixtures generate \
  --db "sqlite+aiosqlite:///./dev.db" \
  --days 30

# Compute metrics
python cli.py metrics daily \
  --db "sqlite+aiosqlite:///./dev.db" \
  --backfill 30
```

### Batch Organization Sync

```bash
# Sync all repos in org
python cli.py sync git --provider github \
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
