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

`--db` and `--analytics-db` are **not** aliases. They point to different databases serving different roles (see Dual-Database Architecture below). If `POSTGRES_URI` is not set, `--db` falls back to `DATABASE_URI`.

Subcommands like `metrics daily` also accept `--sink` to select the output backend. Legacy values (`mongo`, `sqlite`, `postgres`, `both`) are rejected immediately with a migration message. ClickHouse is the only supported analytics backend.

> **Caveat:** Some subcommands (e.g., `audit completeness`, `audit coverage`, `investment materialize`, `work-graph build`) define their own `--db` flag that accepts an **analytics** (ClickHouse) connection string, overriding the global `--db` meaning for that subcommand. Check individual subcommand docs below for the expected connection type.

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

## Input Validation (Preflight)

Before a subcommand runs, the CLI validates that the inputs it actually needs are present. Commands that require a database connection or an organization id **fail fast** with an argparse usage error (**exit code 2**) that names exactly what is missing — instead of failing deep inside the handler with a logged error or a traceback.

These inputs are supplied through global flags or environment variables (`--analytics-db`/`CLICKHOUSE_URI`, `--db`/`POSTGRES_URI`, `--org`/`ORG_ID`), so they cannot be marked `required` on individual subparsers. The preflight closes that gap centrally.

```bash
$ dev-hops metrics compounding-risk        # no CLICKHOUSE_URI / org configured
usage: dev-health-ops metrics compounding-risk [-h] [--day DAY] ...
dev-health-ops metrics compounding-risk: error: missing required input(s):
  - ClickHouse analytics database — pass --analytics-db or set CLICKHOUSE_URI (...)
  - organization id — pass --org or set ORG_ID (could not auto-resolve ...)
```

Each affected command also lists its requirements at the bottom of `--help`:

```bash
$ dev-hops metrics compounding-risk --help
...
Requires: ClickHouse (--analytics-db / CLICKHOUSE_URI), organization (--org / ORG_ID).
```

**Requirement matrix:**

| Requirement | Commands |
|-------------|----------|
| ClickHouse (`--analytics-db` / `CLICKHOUSE_URI`) | `sync git`, `sync prs`, `sync blame`, `sync cicd`, `sync deployments`, `sync incidents`, `sync security`, `sync tests`, `sync work-items`, `sync teams`; `metrics daily`, `metrics dora`, `metrics complexity`, `metrics release-impact`, `metrics validate-flags`, `metrics rebuild`, `metrics compounding-risk` (+org); `audit perf`, `audit schema`; `recommendations compute`; `ai allowlist list/set` (+org); `migrate clickhouse` (bare + `upgrade`/`status`/`repair`) |
| ClickHouse via `--db` (`CLICKHOUSE_URI`) | `investment materialize` |
| PostgreSQL (`--db` / `POSTGRES_URI`) | `billing reconcile`; `migrate postgres` (bare + `upgrade`/`downgrade`/`current`); legacy `migrate upgrade`/`downgrade`/`current` |
| Organization (`--org` / `ORG_ID`) | `metrics compounding-risk`, `backfill run`, `ai allowlist list/set` |

> The org id auto-resolves from the first organization in PostgreSQL when `--org`/`ORG_ID` are omitted; the preflight only fails when no org can be resolved.

> Read-only Alembic commands that do not open a connection (`migrate [postgres] heads`, `migrate [postgres] history`) are intentionally **not** gated. Commands that declare their own `required=True` flag (e.g. `audit completeness`/`coverage` `--db`, `metrics capacity` `--db`, `fixtures validate` `--sink`) keep using argparse's own required-argument error.

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

# GitHub App
dev-hops sync git --provider github \
  --github-app-id "$GITHUB_APP_ID" \
  --github-app-key-path "$GITHUB_APP_PRIVATE_KEY_PATH" \
  --github-app-installation-id "$GITHUB_APP_INSTALLATION_ID" \
  --owner my-org \
  --repo my-repo

# GitLab
dev-hops sync git --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --project-id 278964
```

**Options:**
| Option | Description |
|--------|-------------|
| `--provider` | `local`, `github`, `gitlab` |
| `--auth` | GitHub/GitLab token override (PAT mode for GitHub) |
| `--github-app-id`, `--github-app-key-path`, `--github-app-installation-id` | GitHub App auth flags. Mutually exclusive with PAT auth. |
| `--repo-path` | Path to local repo |
| `--owner`, `--repo` | GitHub owner/repo |
| `--project-id` | GitLab project ID |
| `--since` | Start datetime (ISO 8601). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: tomorrow) |
| `--backfill N` | Backfill N days ending at `--before`. Mutually exclusive with `--since` |
| `--sink` | Analytics backend (`clickhouse` only; default) |

`--date` is a deprecated hidden alias for `--before`.

GitHub authentication precedence is CLI flags > environment variables > stored database credentials. Use either PAT auth (`--auth` or `GITHUB_TOKEN`) or GitHub App auth, not both. See [GitHub App authentication](../user-guide/github-app-auth.md).

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
  --before 2025-02-02 \
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

Sync team definitions. Persists to the analytics store, so `CLICKHOUSE_URI` (or `--analytics-db`) is required regardless of the `--provider` source (see [Input Validation](#input-validation-preflight)).

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
  --before 2025-02-02 \
  --backfill 1

# 7-day backfill
dev-hops metrics daily \
  --before 2025-02-02 \
  --backfill 7

# Filter to one repo
dev-hops metrics daily \
  --before 2025-02-02 \
  --repo-id <uuid>

# Specify output format
dev-hops metrics daily \
  --before 2025-02-02 \
  --sink clickhouse
```

**Options:**
| Option | Description |
|--------|-------------|
| `--since` | Start date. Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: tomorrow) |
| `--backfill N` | Compute N days ending at `--before` (default: 1) |
| `--repo-id` | Filter to specific repository |
| `--sink` | Analytics backend (`clickhouse` only) |

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
  --repo-name "meridian/web-app" \
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
| `--repo-name` | `meridian/web-app` | Base repository name |
| `--repo-count` | `1` | Number of repos to generate |
| `--days` | `30` | Number of days of historical data |
| `--commits-per-day` | `5` | Average commits per day |
| `--pr-count` | `20` | Total pull requests to generate |
| `--seed` | random | Deterministic seed for repeatable runs |
| `--provider` | `synthetic` | Provider label: `synthetic`, `github`, `gitlab`, `jira` |
| `--with-metrics` | off | Also generate derived metrics (daily, DORA, complexity, investment, etc.) |
| `--with-work-graph` | off | Build work graph edges after generation (ClickHouse only) |
| `--team-count` | `8` | Number of synthetic teams to create |

Database type is auto-detected from the sink URI.

Every fixture run also seeds synthetic security alert rows into
`security_alerts` for each generated repo. These rows include Dependabot,
code-scanning, advisory, GitLab vulnerability, and GitLab dependency-style
sources so the security GraphQL resolvers and UI have demo data without a
separate flag. Verify them with:

```sql
SELECT count(), countDistinct(severity) FROM security_alerts;
```

When `--with-metrics` is enabled against ClickHouse, AI workflow intelligence
tables are also seeded: `ai_attribution`, `ai_workflow_runs`,
`ai_workflow_artifact_edges`, and `ai_workflow_issue_edges`. The daily metrics
job then computes `ai_impact_metrics_daily`, `ai_governance_coverage_daily`,
and `ai_policy_events` from those source rows.

### `fixtures validate`

Validate that fixture data is sufficient for work graph and investment analysis.

```bash
dev-hops fixtures validate --sink "clickhouse://localhost:8123/default"
```

**Options:**
| Option | Description |
|--------|-------------|
| `--sink` | Analytics sink URI (required, ClickHouse only) |

Checks raw data counts, team mappings, cycle time metrics, work graph edges,
connected components, security alert fixture coverage, AI fixture/rollup tables,
and evidence bundle quality.

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

## Backfill Commands

### `backfill run`

Run historical data backfill for a sync configuration. Data is synced in chunked 7-day windows. Uses `CLICKHOUSE_URI`.

```bash
dev-hops backfill run \
  --config-id "550e8400-e29b-41d4-a716-446655440000" \
  --since 2024-01-01 \
  --before 2024-03-01
```

**Options:**

| Option | Description |
|--------|-------------|
| `--config-id` | Sync configuration UUID (required) |
| `--since` | Start date (ISO 8601). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: tomorrow) |
| `--backfill N` | Backfill N days ending at `--before`. Mutually exclusive with `--since` |
| `--sink` | Analytics backend (`clickhouse` only; default) |

Backfill depth is limited by organization tier:

| Tier | Max Backfill Depth |
|------|-------------------|
| Community | 30 days |
| Team | 90 days |
| Enterprise | Unlimited |

> **Important:** Backfill never updates SyncWatermarks. Incremental sync state is preserved.

## Reports

AI-generated reports are managed through the GraphQL API and executed as Celery tasks. Reports are not triggered via CLI — they are created, triggered, and scheduled through the Report Center UI or GraphQL mutations.

### How Reports Work

1. **Create** a SavedReport via the Report Center UI or `createSavedReport` mutation
2. **Trigger** execution manually ("Run Now") or via a cron schedule
3. The `execute_saved_report` Celery task runs on the `reports` queue
4. The engine fetches metrics from ClickHouse, generates insights, and renders markdown
5. Results are persisted as a `ReportRun` with rendered content and provenance records

### Report Plan

Each report requires a `ReportPlan` that defines scope, time range, sections, and metrics. If no explicit plan is provided, a default plan is generated from the report's `parameters` at execution time:

- `scope` → team/repo/org scoping
- `dateRange` → time window (`last_7_days`, `last_30_days`, `last_90_days`)
- `metrics` → requested metric names

### Scheduling

Reports can be scheduled with a cron expression (via `scheduleCron` in the create/update mutation). The `dispatch_scheduled_reports` beat task runs every 5 minutes and dispatches any due reports.

### Worker Configuration

Reports require the `reports` queue to be active:

```bash
dev-hops workers start-worker --queues default metrics sync reports
```

### GraphQL Mutations

| Mutation | Description |
|----------|-------------|
| `createSavedReport` | Create a new report definition |
| `updateSavedReport` | Update name, description, parameters, schedule |
| `cloneSavedReport` | Clone a report with optional overrides |
| `deleteSavedReport` | Delete a report and its schedule |
| `triggerReport` | Manually trigger a report execution |

### GraphQL Queries

| Query | Description |
|-------|-------------|
| `savedReports` | List saved reports for an org |
| `savedReport` | Get a single report by ID |
| `reportRuns` | List execution history for a report |

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

## Migrate Commands

Database schema migrations for PostgreSQL (Alembic) and ClickHouse.

### `migrate postgres`

Run PostgreSQL (Alembic) schema migrations. Uses `POSTGRES_URI`.

```bash
# Apply all pending migrations (upgrade to head)
dev-hops migrate postgres
dev-hops migrate postgres upgrade

# Upgrade to a specific revision
dev-hops migrate postgres upgrade abc123

# Revert one migration
dev-hops migrate postgres downgrade -1

# Show current applied revision
dev-hops migrate postgres current

# Show migration history
dev-hops migrate postgres history

# Show available heads
dev-hops migrate postgres heads
```

**Backward-compatible aliases:** `dev-hops migrate upgrade`, `dev-hops migrate downgrade`, etc. still work and target PostgreSQL.

### `migrate clickhouse`

Run ClickHouse schema migrations. Uses `CLICKHOUSE_URI`.

ClickHouse migrations are numbered `.sql` and `.py` files in `migrations/clickhouse/`, tracked via a `schema_migrations` table in ClickHouse.

```bash
# Apply all pending migrations
dev-hops migrate clickhouse
dev-hops migrate clickhouse upgrade

# Show applied and pending migrations
dev-hops migrate clickhouse status
```

> **Important:** Run `dev-hops migrate clickhouse` after setting up a fresh environment, before running any sync or metrics commands. ClickHouse tables are **not** auto-created — they require migrations to be applied first.

---

## Workflow Examples

### Full Sync Pipeline

```bash
# Set environment variables
export CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/default"
export POSTGRES_URI="postgresql+asyncpg://postgres:postgres@localhost:5555/postgres"

# 1. Run migrations
dev-hops migrate postgres
dev-hops migrate clickhouse

# 2. Sync git data
dev-hops sync git --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner myorg \
  --repo myrepo

# 3. Sync work items
dev-hops sync work-items --provider jira \
  --before 2025-02-02 \
  --backfill 30

# 4. Compute metrics
dev-hops metrics daily \
  --backfill 30
```

### Local Development

```bash
# Start databases
docker compose up -d clickhouse postgres

# Run migrations
dev-hops migrate postgres
dev-hops migrate clickhouse

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
| 2 | Configuration error — includes argparse usage errors and missing required inputs surfaced by the [preflight](#input-validation-preflight) (e.g. unset `CLICKHOUSE_URI`/`POSTGRES_URI`/`ORG_ID`) |
| 3 | Authentication error |
| 4 | Rate limit exceeded |
