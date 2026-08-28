# CLI Reference

Complete reference for the dev-health-ops command-line interface.

---

## Overview

The CLI entry point is `dev-hops` (module `dev_health_ops.cli`). Command groups:

- `sync` : ingest provider data (git, prs, blame, cicd, deployments, incidents, security, tests, teams, work-items)
- `teams` : team catalog operations (ClickHouse-backed sync)
- `metrics` — compute analytics (daily, rebuild, dora, complexity, capacity, release-impact, validate-flags, compounding-risk)
- `audit` — diagnostics (completeness, schema, perf, coverage)
- `fixtures` — synthetic/demo data (generate, validate, product-telemetry)
- `work-graph` / `investment` / `recommendations` — graph, investment, and recommendation computation
- `admin` — users, orgs, licenses, feature flags, billing plans, feature bundles
- `billing` — Stripe reconciliation
- `ai` — AI governance allowlist
- `migrate` — PostgreSQL (Alembic) and ClickHouse schema migrations
- `api` — run the REST/GraphQL API server
- `workers` — Celery worker and beat scheduler
- `maintenance` — operational cleanup

### Inline Execution vs. Celery-Backed Operations

Bare CLI commands run inline, executing immediately in your terminal session. However, several commands have argument-enforcement gaps (CHAOS-2475). These operations require credentials or inputs that the CLI doesn't enforce at startup. Running them inline without these inputs can lead to silent failures or incomplete runs.

Until these gaps are fixed, we recommend triggering the equivalent Celery jobs instead of running the commands inline. Celery workers run in a managed environment where credentials and configurations are fully validated. You can find the list of Celery tasks and queue configurations in [Run workers and jobs](../../operate/run/workers-and-jobs.md).

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

See [Data and storage boundaries](../../contribute/architecture/data-and-storage.md) for details.

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
usage: dev-health-ops metrics compounding-risk [-h] [--since SINCE | --backfill BACKFILL] ...
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
| PostgreSQL (`--db` / `POSTGRES_URI`) | `billing reconcile`; `migrate postgres` (bare + `upgrade`/`downgrade`/`current`); `migrate configs-to-integrations` (one-time child-config -> integration data migration; `--dry-run` to preview); legacy `migrate upgrade`/`downgrade`/`current` |
| Organization (`--org` / `ORG_ID`) | `metrics compounding-risk`, `backfill run`, `ai allowlist list/set` |

> The org id auto-resolves from the first organization in PostgreSQL when `--org`/`ORG_ID` are omitted; the preflight only fails when no org can be resolved.

> Read-only Alembic commands that do not open a connection (`migrate [postgres] heads`, `migrate [postgres] history`) are intentionally **not** gated. Commands that declare their own `required=True` flag (e.g. `audit completeness`/`coverage` `--db`, `metrics capacity` `--db`, `fixtures validate` `--sink`) keep using argparse's own required-argument error.

---

## Sync Commands

> ⚠️ **Warning (CHAOS-2475):** Sync commands run inline and require provider credentials (such as `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_EMAIL`, `JIRA_API_TOKEN`, or `LINEAR_API_KEY`) that the CLI doesn't enforce at startup. Running them inline without these inputs can cause silent failures.
>
> **Interim Workaround:** Trigger the sync via `POST /api/v1/admin/sync-configs/{config_id}/trigger`. The API plans the `SyncRun` and commits a durable reference-discovery wakeup; the reconciler publishes it through the active sync-dispatch route.

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
| `--backfill N` | Backfill N days ending before `--before`. Mutually exclusive with `--since` |
| `--sink` | Analytics backend (`clickhouse` only; default) |

`--date` is a deprecated hidden alias for `--before`.

GitHub authentication precedence is CLI flags > environment variables > stored database credentials. Use either PAT auth (`--auth` or `GITHUB_TOKEN`) or GitHub App auth, not both. See [Connect GitHub](../../admin/data-sources/github.md).

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

The sync also writes the versioned, provider-neutral CI acceptance projection
used by Ask Dev status checks. GitHub derives required check names only from the
target branch's required-status-check policy; GitLab derives the required
pipeline outcome only from the project's merge policy. A denied, missing, or
unsupported policy is stored as `unknown`, never optional. Required work that
the provider reports as skipped remains skipped even when the enclosing
pipeline is green.

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

Rows ingested before ClickHouse migration `070_ci_acceptance_checks` remain
explicitly unknown to Ask Dev. Re-run the existing CI sync for only the required
repository and time window to populate them; there is no automatic unbounded
history backfill. Replaying the same range is idempotent. Rolling a producer
back preserves prior projection rows and their freshness timestamps, so stale
rows degrade status instead of silently proving completion.

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
dev-hops sync incidents --provider gitlab \
  --auth "$GITLAB_TOKEN" \
  --gitlab-url "https://gitlab.com" \
  --project-id 123
```

GitLab selects native `issue_type=incident` rows. GitHub is intentionally unsupported:
ordinary GitHub issues, including label-bearing issues, remain work items.

### `sync blame`

Sync git blame data only (line-level authorship). Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync blame --provider local --repo-path /path/to/repo
```

Accepts the same provider, auth, single-repo, batch-mode, and date-range options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

Planner-managed GitHub/GitLab integrations seed the heavy `blame` dataset when
the legacy `git` target is selected. This keeps ownership and bus-factor metrics
reachable from normal code-host onboarding while the per-sync GitHub blame crawl
remains capped (`BLAME_BACKFILL_MAX_FILES=500`) and coverage-aware. Existing dev
or support fixtures created before that seed can enable blame by adding an
`integration_datasets` row for the integration with `dataset_key='blame'`,
`is_enabled=true`, and options mirroring the integration's existing `git` dataset
row (for example `{"legacy_targets":["git"]}`); after the row exists, the admin
dataset endpoint can toggle it like any other dataset.

### `sync security`

Sync security and dependency alerts (Dependabot, code-scanning, advisories, GitLab vulnerability/dependency findings). Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync security --provider github \
  --auth "$GITHUB_TOKEN" --owner org --repo repo
```

Accepts the same provider/auth/batch options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

### `sync tests`

Sync CI test results and coverage (TestOps). Uses `CLICKHOUSE_URI`.

```bash
dev-hops sync tests --provider github \
  --auth "$GITHUB_TOKEN" --owner org --repo repo
```

Accepts the same provider/auth/batch options as [`sync git`](#sync-git). Providers: `local`, `github`, `gitlab`, `synthetic`.

### `sync teams`

Sync team definitions. ClickHouse is the system of record for teams (CHAOS-2600 CS5); both paths write ClickHouse directly. The only difference is org tagging:

- **Org-scoped (`--org ORG`)**: The provider data is written **directly to ClickHouse** via `insert_teams`, with each team row tagged with `org_id` (and any Jira ops links inserted). It does **not** project to PostgreSQL `team_mappings` and does **not** call any Postgres→ClickHouse bridge.
- **No-org (no `--org`)**: The provider data is written directly to ClickHouse (preserving synthetic/local seeding), untagged.

`CLICKHOUSE_URI` (or `--analytics-db`) is required. `POSTGRES_URI` / `--db` is **not** required for `sync teams` (no Postgres team write).

```bash
# From config file
dev-hops sync teams --path src/dev_health_ops/config/team_mapping.yaml --allow-empty

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

The bundled `src/dev_health_ops/config/team_mapping.yaml` is intentionally empty for onboarding. By default, `sync teams` exits non-zero when discovery or persistence results in zero teams; use `--allow-empty` only when an empty/no-op sync is expected.

---

## Teams Commands

> **Removed in CHAOS-2600 CS5:** the `dev-hops teams reconcile` command is **deleted**. It reconciled org-scoped ClickHouse teams back into PostgreSQL `team_mappings` and re-bridged via `bridge_teams_to_clickhouse` — both the Postgres team control plane and the bridge are gone. ClickHouse is now the system of record for teams, written directly by `sync teams` and the admin team surface, so no reconcile step is needed.

---

## Metrics Commands

> ⚠️ **Warning (CHAOS-2475):** Metrics commands run inline and require database connections and configurations that the CLI doesn't enforce at startup. Running them inline can cause silent failures or incomplete computations.
>
> **Interim Workaround:** We recommend triggering the equivalent Celery jobs on the `metrics` queue. See [Run workers and jobs](../../operate/run/workers-and-jobs.md) for details on Celery worker configuration.

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
| `--backfill N` | Compute N days ending before `--before` (default: 1) |
| `--repo-id` | Filter to specific repository |
| `--sink` | Analytics backend (`clickhouse` only) |

### `metrics rebuild`

Recompute daily metrics for one or more repositories (or all repos) over a date range, then run a single partitioned finalize per day. Each repo/day is recomputed with finalize skipped, then the whole day is finalized once. Use after correcting or re-syncing source data for specific repos. Uses `CLICKHOUSE_URI`.

```bash
# Rebuild all repos for the last 7 days
dev-hops metrics rebuild --backfill 7

# Rebuild specific repos (repeatable --repo-id) over an explicit range
dev-hops metrics rebuild \
  --repo-id 550e8400-e29b-41d4-a716-446655440000 \
  --repo-id 550e8400-e29b-41d4-a716-446655440001 \
  --since 2025-01-01 --before 2025-02-01
```

**Options:**
| Option | Description |
|--------|-------------|
| `--repo-id` | Repo UUID to rebuild; repeatable. Omit to rebuild all repos |
| `--since` | Start date (inclusive). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: tomorrow) |
| `--backfill N` | Process N days ending before `--before` (default: 1) |
| `--sink` | Analytics backend (`clickhouse` only) |
| `--provider` | Restrict to a single provider (default: `auto`) |

### `metrics dora`

Compute and persist DORA metrics (deployment frequency, lead time, change failure rate, time to restore) from synced ClickHouse data. Uses `CLICKHOUSE_URI`.

```bash
dev-hops metrics dora --backfill 30

# Compute a subset of metrics
dev-hops metrics dora --backfill 30 --metrics deployment_frequency,lead_time
```

**Options:**
| Option | Description |
|--------|-------------|
| `--since` / `--before` / `--backfill N` | Date range (as in `metrics daily`) |
| `--repo-id` / `--repo-name` | Filter to a specific repository |
| `--metrics` | Comma-separated metric names (default: full DORA set) |
| `--sink` | Analytics backend (`clickhouse` only) |

### `metrics complexity`

Compute file complexity and hotspot metrics from persisted `git_files`/`git_blame` data. Uses `CLICKHOUSE_URI`.

> **Note (CHAOS-2850/CHAOS-2888):** `--backfill N` must not fabricate N days of historical complexity from current file contents. There is no persisted historical file-content snapshot, so the DB complexity path writes complexity only when it has a real target-day input contract; run it daily (or let Go's daily complexity fixed schedule run -- the Celery `dispatch_complexity_job` beat cadence it replaced was deleted under CHAOS-4026 on 2026-08-21) to build a genuine trend. Historical API backfills skip complexity recompute unless a future real historical source of truth is added.

```bash
dev-hops metrics complexity --backfill 30

# Scope to repos matching a glob and limit languages/files
dev-hops metrics complexity \
  -s "meridian/*" \
  --lang "*.py" \
  --exclude "*/tests/*" \
  --max-files 500
```

**Options:**
| Option | Description |
|--------|-------------|
| `--since` / `--before` / `--backfill N` | Date range (as in `metrics daily`) |
| `--repo-id` | Filter to a specific repo |
| `-s, --search` | Repo name search pattern (glob) |
| `--lang` | Include language globs (e.g. `*.py`) |
| `--exclude` | Exclude language globs (e.g. `*/tests/*`) |
| `--max-files` | Limit files scanned per repo; the resulting rows are that day's complete replacement slice, not an additive preview |
| `--sink` | Analytics backend (`clickhouse` only) |

### `metrics capacity`

Compute capacity / completion-date forecasts using Monte Carlo simulation over historical throughput. Takes its ClickHouse DSN via its own **required** `--db` flag (see the caveat under [Global Arguments](#global-arguments)).

```bash
# Forecast a single team
dev-hops metrics capacity --db "$CLICKHOUSE_URI" --team-id eng-core

# Forecast all discovered team/scope combinations, print without persisting
dev-hops metrics capacity --db "$CLICKHOUSE_URI" --all-teams --dry-run
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | ClickHouse connection string (**required**) |
| `--team-id` | Filter by team ID |
| `--work-scope-id` | Filter by work scope ID (project/board) |
| `--target-items` | Number of items to complete (defaults to current backlog) |
| `--target-date` | Target deadline (YYYY-MM-DD) |
| `--history-days` | Days of history to use (default: 90) |
| `--simulations` | Number of Monte Carlo simulations (default: 10000) |
| `--all-teams` | Compute forecasts for all team/scope combinations |
| `--dry-run` | Print forecasts without persisting |

### `metrics release-impact`

Compute release-impact daily metrics from telemetry signal buckets. Re-computes a trailing window on each run so late-arriving signals are captured. Uses `CLICKHOUSE_URI`.

```bash
dev-hops metrics release-impact --backfill 7

# Widen the recomputation window
dev-hops metrics release-impact --recomputation-window 14
```

**Options:**
| Option | Description |
|--------|-------------|
| `--since` / `--before` / `--backfill N` | Date range (as in `metrics daily`) |
| `--recomputation-window N` | Days to recompute on each run (default: 7) |
| `--sink` | Analytics backend (`clickhouse` only) |

### `metrics validate-flags`

Run feature-flag pipeline validation checks against recent data. Uses `CLICKHOUSE_URI`.

```bash
dev-hops metrics validate-flags --lookback 30
```

**Options:**
| Option | Description |
|--------|-------------|
| `--lookback N` | Number of days to inspect (default: 30) |
| `--sink` | Analytics backend (`clickhouse` only) |

### `metrics compounding-risk`

Compute the Compounding Risk composite from persisted inputs (`repo_metrics_daily` + `repo_complexity_daily`) and write `compounding_risk_daily`. Requires `CLICKHOUSE_URI` **and** an organization id.

> **Note (CHAOS-2888):** this command exits `0` whenever the compounding-risk query and write both complete, even if some rows have `severity="unknown"` due to missing required inputs — it exits non-zero only for configuration, validation, or infrastructure failures. Missing-input reason counts (`missing_rework_churn`, `missing_complexity_delta`, `missing_review_latency`, `missing_ownership_signal`) are logged per run. For API-triggered backfills, the same missing-input counts and per-day table coverage are surfaced on `GET /backfill-jobs/{job_id}` via `metrics_diagnostics`.

```bash
dev-hops metrics compounding-risk --org "$ORG_ID"

# Backfill seven days ending before 2025-02-02, i.e. through 2025-02-01
dev-hops metrics compounding-risk --before 2025-02-02 --backfill 7

# Explicit inclusive start with exclusive end
dev-hops metrics compounding-risk --since 2025-01-01 --before 2025-02-02
```

**Options:**
| Option | Description |
|--------|-------------|
| `--since` | Start date (inclusive). Mutually exclusive with `--backfill` |
| `--before` | End date (exclusive, default: tomorrow) |
| `--backfill N` | Process N days ending before `--before` (default: 1) |
| `--sink` | Analytics backend (`clickhouse` only) |

> CHAOS-2475 follow-up: `--day` is not supported. Use `--before <day-after-target> --backfill 1` for one historical day.

---

## Audit Commands

Diagnostic audits for data completeness, schema integrity, provider coverage, and query performance.

### `audit completeness`

Check data freshness and completeness across providers within a time window.

```bash
# Table output (default)
dev-hops audit completeness --db "clickhouse://localhost:8123/default" --org ORG_ID --days 7

# JSON output
dev-hops audit completeness --db "clickhouse://localhost:8123/default" --org ORG_ID --days 30 --format json
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | Database connection string (required) |
| `--org` | Organization ID used to scope canonical incident and mapping rows (required) |
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
  --team-count 10
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
| `--with-metrics` | off | Also generate derived metrics (daily, DORA, complexity, investment, Cockpit/TestOps risk inputs, etc.) |
| `--with-work-graph` | off | Build work graph edges after generation (ClickHouse only) |
| `--team-count` | `10` | Number of synthetic teams to create |
| `--db-type` | auto-detected | Explicit DB type (`postgres`, `clickhouse`, etc). Overrides the auto-detection described below; only needed when the sink URI's scheme doesn't name it unambiguously. |

Database type is auto-detected from the sink URI unless `--db-type` overrides it.

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

`--with-metrics` also writes the Cockpit/Govern risk inputs used by Compounding
Risk and TestOps Delivery Risk: `repo_complexity_daily`, `repo_metrics_daily`,
`compounding_risk_daily`, `testops_pipeline_metrics_daily`,
`testops_test_metrics_daily`, and `testops_coverage_metrics_daily`. A base
fixture run without this flag is suitable for raw-ingest checks, but those risk
surfaces should be expected to report missing inputs until metrics are computed.

### `fixtures validate`

Validate that fixture data is sufficient for work graph and investment analysis.

```bash
dev-hops fixtures validate --sink "clickhouse://localhost:8123/default"
```

**Options:**
| Option | Description |
|--------|-------------|
| `--sink` | Analytics sink URI (required, ClickHouse only) |

Checks raw data counts, the ClickHouse team catalog, cycle time metrics, work
graph edges, connected components, security alert fixture coverage, AI
fixture/rollup tables, and evidence bundle quality.

### `fixtures product-telemetry`

Seed `product_telemetry_events` across one or more orgs so the platform-admin dashboard and per-org product views have data locally. Uses `CLICKHOUSE_URI`, and reads org IDs from PostgreSQL when `--org` is not supplied.

```bash
# Seed the first 3 orgs from Postgres, 30 days each
dev-hops fixtures product-telemetry --orgs 3 --days 30

# Seed explicit orgs (repeatable --org)
dev-hops fixtures product-telemetry \
  --org 550e8400-e29b-41d4-a716-446655440000 \
  --days 60 --sessions-per-day 50 --seed 42
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--orgs` | — | Number of orgs to seed when `--org` is not provided (first N from Postgres `organizations`; falls back to synthetic UUIDs) |
| `--org` | — | Explicit org id to seed (repeatable; overrides `--orgs`) |
| `--days` | — | Days of data per org |
| `--sessions-per-day` | — | Average synthetic sessions per day per org |
| `--seed` | random | Deterministic seed (mixed with org_id) for repeatable runs |

---

### `fixtures world-snapshot` / `fixtures world-restore`

Move the versioned `ask-dev-world.v1` fixture world from a scratch database into the database a stack actually serves.

`fixtures world` deliberately refuses to run against a non-scratch database (`_require_scratch_database`), which includes ClickHouse `default` and Postgres `postgres` — the two databases the Ask Dev acceptance stack serves. Separately, two independent `fixtures world` runs do **not** produce the same `WORLD_DIGEST` (declared-blocked, see `world.json`'s `cross_generation_digest_status`), so regenerating the world per boot can never match a pinned digest.

Both are solved the same way: **generate once into scratch, snapshot it, restore that snapshot on every boot.**

```bash
# 1. generate once, into a real scratch database
dev-hops fixtures world --manifest tests/acceptance/world/ask-dev-world.v1/world.json \
  --sink clickhouse://ch:ch@clickhouse:8123/ask_dev_world_scratch \
  --postgres-uri postgresql+asyncpg://postgres:postgres@postgres:5432/ask_dev_world_scratch

# 2. snapshot it (which tables were written is derived by diffing against a
#    freshly-migrated baseline, never from a hardcoded list)
dev-hops fixtures world-snapshot --manifest .../world.json \
  --sink clickhouse://.../ask_dev_world_scratch \
  --postgres-uri postgresql+asyncpg://.../ask_dev_world_scratch \
  --baseline-sink clickhouse://.../default \
  --baseline-postgres-uri postgresql+asyncpg://.../postgres \
  --out tests/acceptance/world/ask-dev-world.v1/snapshot

# 3. restore into the serving databases and verify the pin (what every boot runs)
dev-hops fixtures world-restore --manifest .../world.json \
  --sink clickhouse://.../default \
  --postgres-uri postgresql+asyncpg://.../postgres \
  --snapshot tests/acceptance/world/ask-dev-world.v1/snapshot
```

In practice you never run these by hand: `scripts/acceptance/mint_ask_dev_world_snapshot.sh` performs the whole mint (steps 1–3 plus `--mint-digest`), and `scripts/acceptance/run_ask_dev_compose.sh` runs the restore on every acceptance boot.

`world-restore` is INSERT-only and issues no DDL. It refuses, before writing anything, unless `ENVIRONMENT=acceptance` **and** every table its snapshot carries is empty in the target — a real dev or production database always has organizations and commits, so it always fails that check. There is no `--force`. After restoring it verifies two things and exits non-zero on either: the per-table row-count delta it produced must equal the delta the original generation produced (which catches a table the snapshot missed), and the recomputed `WORLD_DIGEST` must equal the pinned one.

It also refuses a snapshot that was not minted for the *current* `world.json`: the artifact records the world's `schema_version`, `master_seed`, and a hash of the identity/credential contract (org and user aliases, `id_seed`s, emails, usernames, roles, superuser flags), and all three must match the manifest being restored. This is not redundant with the digest guard — `WORLD_DIGEST` is computed from the restored *database*, so a `world.json` edit that leaves the derived ids alone (a changed email or `membership_role`, say) leaves every restored row and the digest bit-for-bit identical, while the manifest every consumer reads now disagrees with what the stack serves. A snapshot carrying no contract hash is refused rather than trusted. Re-mint after any such edit.

Every acceptance boot additionally proves the restored world's principals can actually *authenticate* (`scripts/acceptance/assert_world_principals_can_log_in.py`, the same script the mint runs, wrong-password negative control included). The digest cannot cover this: it proves the credential bytes restored identically, not that the API still accepts them, so a login-path or bcrypt-policy regression would otherwise ride behind a green digest with the corpus silently falling back to the superuser.

**`world-snapshot` options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--manifest` | — | Path to `world.json` (required) |
| `--sink` | `CLICKHOUSE_URI` | ClickHouse URI of the **generated scratch** database |
| `--postgres-uri` | — | Postgres URI of the **generated scratch** database (required) |
| `--baseline-sink` | — | ClickHouse URI of a freshly-migrated, unseeded database (required) |
| `--baseline-postgres-uri` | — | Postgres URI of a freshly-migrated, unseeded database (required) |
| `--out` | — | Directory to write the snapshot into (required) |

**`world-restore` options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--manifest` | — | Path to `world.json` (required) |
| `--sink` | `CLICKHOUSE_URI` | Target ClickHouse URI |
| `--postgres-uri` | — | Target Postgres URI (required) |
| `--snapshot` | — | Snapshot directory to restore (required) |
| `--digest-path` | alongside `--manifest` | `WORLD_DIGEST` file to verify against |
| `--mint-digest` | off | Re-pin `WORLD_DIGEST` from the restored state instead of verifying. Mint flow only — on an ordinary boot this would make the digest guard verify the world against itself |

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
| `--tier` | Subscription tier (default: `community`) |
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

### `admin users update`

Update an existing user (identified by `--id`, `--email`, or `--username`) and optionally manage org memberships. Uses `POSTGRES_URI`.

```bash
python -m dev_health_ops.cli admin users update \
  --email user@example.com \
  --full-name "New Name" \
  --no-active

# Add to an org with a role
python -m dev_health_ops.cli admin users update \
  --email user@example.com --org my-org --role admin
```

**Options:**
| Option | Description |
|--------|-------------|
| `--id` / `--email` / `--username` | Identify the user to update |
| `--new-email` / `--new-username` | Change email/username (empty string clears username) |
| `--full-name` | Set the user's full name |
| `--password` | Set a new password (min 8 chars; revokes existing sessions) |
| `--verified` / `--no-verified` | Set verified status |
| `--superuser` / `--no-superuser` | Set superuser status |
| `--active` / `--no-active` | Set active status |
| `--org` | Org slug or ID: add the user or update their role |
| `--role` | Membership role with `--org`: `owner`, `admin`, `member`, `viewer` (default: `member`) |
| `--remove-from-org` | Org slug or ID: remove the user's membership |

### `admin orgs delete`

Delete an organization and all of its scoped data. Uses `POSTGRES_URI`.

```bash
# Preview the deletion plan
python -m dev_health_ops.cli admin orgs delete --org-id <uuid> --dry-run

# Delete
python -m dev_health_ops.cli admin orgs delete --org-id <uuid>
```

**Options:**
| Option | Description |
|--------|-------------|
| `--org-id` | Organization ID (required) |
| `--dry-run` | Return the deletion plan without deleting data |

### `admin licenses`

Offline license key management (Ed25519-signed). `create` uses `POSTGRES_URI`.

```bash
# Generate a signing key pair
python -m dev_health_ops.cli admin licenses keygen

# Create a signed license key
python -m dev_health_ops.cli admin licenses create \
  --org-id <uuid> --tier enterprise --duration-days 365 \
  --org-name "Acme" --contact-email billing@acme.com
```

| Subcommand | Description |
|------------|-------------|
| `keygen` | Generate an Ed25519 key pair for license signing |
| `create` | Create a signed license key (`--org-id`, `--tier {community,team,enterprise}`, `--duration-days` (default 365), `--org-name`, `--contact-email`) |

### `admin features`

Feature flag management. Uses `POSTGRES_URI`.

```bash
python -m dev_health_ops.cli admin features seed
```

| Subcommand | Description |
|------------|-------------|
| `seed` | Seed standard feature flags into the database |

### `admin billing`

Billing plan management and Stripe synchronization. Uses `POSTGRES_URI`.

```bash
python -m dev_health_ops.cli admin billing seed
python -m dev_health_ops.cli admin billing list
python -m dev_health_ops.cli admin billing pull-stripe --dry-run
python -m dev_health_ops.cli admin billing sync-stripe
```

| Subcommand | Description |
|------------|-------------|
| `seed` | Seed standard billing plans (Community, Team, Enterprise) with prices |
| `list` | List all billing plans with prices and Stripe sync status |
| `pull-stripe` | Pull billing plans from Stripe into the database (`--dry-run` to preview) |
| `sync-stripe` | Push unsynced billing plans to Stripe |

### `admin bundles`

Feature bundle management (groups of feature keys mapped to plans/orgs). Uses `POSTGRES_URI`.

```bash
python -m dev_health_ops.cli admin bundles create \
  --key pro --name "Pro" --features "metrics,investment,reports"
python -m dev_health_ops.cli admin bundles list
python -m dev_health_ops.cli admin bundles assign-plan --bundle-key pro --plan-key team
python -m dev_health_ops.cli admin bundles assign-org --org-id <uuid> --feature-key reports
```

| Subcommand | Description |
|------------|-------------|
| `create` | Create a bundle (`--key`, `--name`, `--features` comma-separated, `--description`) |
| `list` | List all bundles with features and plan assignments |
| `assign-plan` | Assign a bundle to a billing plan (`--bundle-key`, `--plan-key`) |
| `assign-org` | Grant an org a feature override (`--org-id`, `--feature-key`, `--reason`, `--expires-days`) |

---

## Backfill Commands

### `backfill run`

> ⚠️ **Warning (CHAOS-2475):** The `backfill run` command runs inline and requires provider credentials that the CLI doesn't enforce at startup. Running it inline can cause silent failures. Additionally, there is a known preflight-token bug (CHAOS-2479) where the CLI fails to validate credentials correctly.
>
> **Interim Workaround:** Trigger the backfill via `POST /api/v1/admin/sync-configs/{config_id}/backfill`. The API plans a backfill-mode `SyncRun` and commits the same durable reference-discovery wakeup used by full and continuation syncs.

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
| `--backfill N` | Backfill N days ending before `--before`. Mutually exclusive with `--since` |
| `--sink` | Analytics backend (`clickhouse` only; default) |

Backfill depth is limited by organization tier:

| Tier | Max Backfill Depth |
|------|-------------------|
| Community | 30 days |
| Team | 90 days |
| Enterprise | Unlimited |

> **Important:** Backfill never updates SyncWatermarks. Incremental sync state is preserved.

## API Server

### `api`

Run the Dev Health Ops API server (FastAPI/uvicorn), which serves REST and GraphQL for `dev-health-web`. Uses both `POSTGRES_URI` and `CLICKHOUSE_URI`.

```bash
dev-hops api --reload
dev-hops api --host 0.0.0.0 --port 8000 --workers 4
```

**Options:**
| Option | Description |
|--------|-------------|
| `--host` | Bind host |
| `--port` | Bind port |
| `--workers` | Number of worker processes |
| `--reload` | Enable auto-reload for local development |

OpenAPI docs are served at `/docs` and GraphQL at the API's GraphQL endpoint when the server is running.

---

## Workers

**Celery is retired (CHAOS-4026, 2026-08-21): zero Python celery services run
in prod since the 2026-08-19 stop.** `workers start-worker`/`workers
start-scheduler` (which booted a real `celery worker`/`celery beat` process)
were deleted along with it — they were the last CLI-level way to falsify
CUT-18's "no Celery process is running" criterion. Go owns every periodic
maintenance cadence; see [Run workers and jobs](../../operate/run/workers-and-jobs.md)
for the Go worker/scheduler/reconciler/stream-runner processes.

### `workers inspect`

Read-only: shows sanitized active/reserved/scheduled task state from
Celery's control-plane RPC. Survives deliberately -- useful against the
non-prod `tests/acceptance/compose.ask-dev.yml` acceptance fleet, which
still boots a real Celery worker/beat directly via the `celery` CLI (not
through `dev-hops`) -- and cannot itself start a process.

```bash
dev-hops workers inspect --state active
```

### Worker operator CLI

`dev-health-workerctl` is the authenticated Go operator binary. Read this before
the verb reference below — three of its requirements are not discoverable from
the verbs themselves.

**Mint a credential first.** The operator token the verbs require is a service
credential, created with the Python CLI against the Postgres backend:

```bash
# --service defaults to `acr`. A workerctl credential MUST name worker-operator,
# and --scope is repeated once per scope rather than given as a list.
dev-hops service-credentials create \
  --service worker-operator \
  --scope workers:read \
  --scope workers:operate

dev-hops service-credentials list --service worker-operator

dev-hops service-credentials rotate <credential-id> \
  --service worker-operator \
  --scope workers:read \
  --scope workers:operate \
  --overlap-seconds 300

dev-hops service-credentials revoke <credential-id>
```

All four subcommands require the Postgres backend. The secret is printed once,
by `create` and `rotate` only; `list` returns metadata without secrets. Supply
the secret to `workerctl` through `WORKER_OPERATOR_TOKEN` or
`WORKER_OPERATOR_TOKEN_FILE`. Status and inspection verbs require the
`workers:read` scope; every mutation requires `workers:operate`.

**Flags must precede the positional argument.** Go's `flag` package stops
parsing at the first positional, so an id-first invocation fails with a generic
`invalid_request` that names neither the cause nor the fix:

```bash
dev-health-workerctl jobs retry 9457 --reason r --correlation-id c   # invalid_request
dev-health-workerctl jobs retry --reason r --correlation-id c 9457   # parses
```

**`COORDINATOR_DATABASE_URI` is required, and not every image carries it.**
`workerctl` authenticates the operator token against `internal_service_credentials`,
which is a coordinator-exclusive read, so without that DSN the CLI is entirely
non-functional and returns `configuration_error`. A `workerctl` invocation needs
all three database URIs — `POSTGRES_URI`, `WORKER_DATABASE_URI`, and
`COORDINATOR_DATABASE_URI` — plus the session-safe mode settings. The
`go-reconciler` container carries the coordinator DSN; `go-worker-heavy` does
not, so reaching for the nearest worker container returns `configuration_error`
with no hint that a different container would work.

!!! warning "`jobs retry` and `jobs cancel` cannot succeed in Phase 1"
    Both verbs are advertised and both are refused unconditionally. The Phase-1
    domain guard returns an unsupported-precondition error from every branch,
    ignores the requested action, and inspects no domain state, so no amount of
    configuration makes either verb work. The refusal is deliberate: the frozen
    contracts name domain links that have no authoritative semantic table yet.
    Treat them as unavailable until CHAOS-4030 lands. There is currently no
    generic supported path to re-drive a stranded job by hand — `metrics
    daily-redrive` below is a narrow, daily-metrics-specific exception, not a
    counterexample to this warning.

### `dev-health-workerctl metrics`

Repair a daily-metrics run stranded by CHAOS-4358: every `daily_partition`
River job for it already failed and was discarded, and nothing else ever
re-enqueues work for that run on its own (a fresh `metrics.daily_dispatch`
run still hits the SAME permanent per-partition outbox dedupe key its
original dispatch used, so a bare re-dispatch alone is not enough — see
[job-recovery-lifecycle.md](../../operate/run/job-recovery-lifecycle.md)).

```bash
WORKER_OPERATIONAL_BRIDGE_URL=http://metrics-api:8000 \
WORKER_METRIC_REPAIR_TOKEN=<repair-token> \
dev-health-workerctl metrics daily-redrive \
  --org 70d529e0-3c06-4597-8480-794fd02328b6 \
  --from 2026-08-08 \
  --to 2026-08-27 \
  --review-evidence "confirmed via ClickHouse readback that testops_test/dora/cicd have zero rows for these repo+day scopes; safe to re-run"
```

`--review-evidence` is **required**, with no default (codex review round 3):
"ambiguous" means a progress-having failure MAY have already written real
output, and claim expiration alone is not evidence retry is safe —
`worker_metrics.py`'s single-execution `/repair` endpoint already requires a
human to pick `retry_safe` vs `confirm_succeeded` per execution based on
actual review, and this bulk path must not quietly bypass that by
auto-authorizing every ambiguous row with a generic hardcoded string. State
what you actually checked — e.g. the redriven families' zero-row counters,
or a fresh ClickHouse readback confirming no output landed yet. This matters
most for families whose readers do not `argMax`/dedup by `computed_at` (e.g.
`file_hotspots`/`file_metrics_daily`, which `SUM`s raw rows) — a needless
retry there silently inflates scores rather than landing a harmless
duplicate.

Scoped to one organization and an inclusive UTC calendar-day range, in two
steps that MUST run in this order (codex review, round 1: publishing a
partition job before the ledger repair only reproduces `ambiguous_refused`
and re-terminalizes the partition `failed_permanent`, undoing the reset):

1. **Ledger repair first.** Calls
   `POST /internal/worker/daily-metrics/v1/redrive` (`WORKER_METRIC_REPAIR_TOKEN`
   bearer auth, base URL from `WORKER_OPERATIONAL_BRIDGE_URL` — both
   required) for every `running` run in scope, applying the SAME
   `retry_safe` CAS the single-execution `/metric-executions/v1/{id}/repair`
   endpoint uses (CHAOS-4304) to every `ambiguous`/stuck-`executing`
   compatibility-bridge ledger row underneath them, carrying your
   `--review-evidence` text. This path only ever authorizes `retry_safe` —
   never `confirm_succeeded`, which needs per-row `output_evidence` a bulk
   call cannot supply; an operator who has confirmed a SPECIFIC execution's
   output already landed correctly should use the single-execution
   `/repair` endpoint with `confirm_succeeded` instead.
2. **Partition redrive second.** Resets any `failed_permanent` partition
   back to `failed` (clearing `failure_reason`), then publishes a fresh
   `metrics.daily_partition` job for every `pending`/`failed` partition in
   scope — plus any `running` partition whose lease has already expired
   (the final River attempt died after claiming it but before releasing
   it; `ClaimPartition` already treats this as reclaimable) — under a
   redrive-scoped dedupe key distinct from the partition's original
   dispatch (CHAOS-4358). A live (unexpired) lease is never touched.

If step 1 reports `skipped_claim_active > 0` (an execution's original claim
still read as active at repair time), step 2 does **not** run at all: the
command returns `{"status":
"ledger_repair_incomplete_retry_after_claims_settle", "partitions": null}`
instead (codex review round 2: publishing a partition job for a run with an
unrepaired ambiguous row is a race — that ledger row can 409
`ambiguous_refused` the instant the job reaches it, re-terminalizing the
partition `failed_permanent`). Re-run the same command once those claims
have settled (their owning job finishes or its lease expires).

Otherwise, returns `{"ledger_repair": {"repaired", "skipped_claim_active"},
"partitions": {"PermanentReset", "RedispatchedRunIDs",
"RedrivenPartitions"}}`. Ledger repairs are chunked to ≤200 run ids per
request (the bridge's own request limit); a window spanning many post_sync
fanouts is handled automatically.

**Observability**: `dev_health_daily_metrics_redrive_partitions_total{reason}`
is wired but not live for THIS caller — `workerctl` is a one-shot CLI with no
Prometheus scrape endpoint, so the counter only becomes real if a future
long-lived caller (e.g. an automatic strand-repair reconciler) invokes the
same Go function. The durable, queryable record of a manual redrive today is
the `worker_job_outbox` rows this command commits, under the
`metrics.daily_partition:redrive:<nonce>` dedupe-key prefix:

```sql
SELECT dedupe_key, status, created_at FROM worker_job_outbox
WHERE dedupe_key LIKE 'metrics.daily_partition:redrive:%'
ORDER BY created_at DESC;
```

#### `metrics remaining start` (CHAOS-4254)

Dispatch a **new** remaining-metrics run for a historical `(organization,
family, day)` that no automatic trigger ever dispatched at all (CHAOS-4254) —
sync never ran that day, or the row aged out of River's retention. This is
narrower than it sounds: `jobs retry` recovers a `remaining_metric_runs` row
that was dispatched and then discarded, and `metrics daily-redrive` above is
DAILY-family-only (`daily_metrics_runs`/`daily_metrics_partitions`) and never
touches the remaining family's native Go executors (dora, capacity, …) at
all. Neither helps when the day was never computed in the first place — this
command is also the prod recovery path for CHAOS-4384's dora-frozen-at-0
incident, since a day the pre-fix same-day coverage bug froze at 0 rows
already has a "succeeded" partition and needs exactly this bypass.

```bash
WORKER_OPERATOR_TOKEN=<operator-token> \
dev-health-workerctl metrics remaining start \
  --family dora \
  --day 2026-08-25 --to 2026-08-27 \
  --org c6a38355-dad6-42e4-8cc9-4c712450827d \
  --review-evidence "CHAOS-4384: dora frozen at 0 rows for 08-25..08-27 by the pre-fix same-day coverage bug (5ddab4c65); deployments/incidents have since landed for these closed days"
```

Supported `--family` values are the day-scoped remaining-metrics families
only: `complexity`, `dora`, `release_impact`. `capacity`, `recommendations`,
and `membership_backfill` are real families (`families.json`) but do not
scope by calendar day — `capacity` needs a `GenerationSeed` the CLI has no
flag for, and the other two scope by window/repo set — so `--family capacity`
etc. is `invalid_request`. `--to` defaults to `--day` (a single day); the
`[--day, --to]` span is capped at 31 days — this is a manual, human-invoked
recovery tool for a handful of days, not a bulk backfill mechanism.

**Coverage rule — deliberately not the same one the automatic dora trigger
applies.** `StartRunTx`'s own `family=="dora"` cross-trigger dedup
(CHAOS-4384) treats ANY succeeded partition for a day that has already
CLOSED as terminal coverage, 0 rows or not, because for the automatic
triggers a genuinely quiet closed day and a day nobody ever computed are
indistinguishable. This command is more precise, because a human is
authorizing each recompute individually, and checks every succeeded
partition whose scope window `[anchor - backfill_days + 1, anchor]`
contains the requested day (`generation` is not part of the check — a
prior run under ANY generation counts):

- An **exact single-day partition** (`backfill_days == 1`, the shape every
  automatic post-sync/fixed-schedule dispatch normally uses) is
  unambiguous: its `rows_written` evidence IS that day's own total. A
  **0-row** exact match is exactly the CHAOS-4384 shape this command
  exists to recompute and is NOT refused; a **non-zero-row** exact match
  is refused (`"already_covered"`).
- A **wide multi-day partition** (`backfill_days > 1`, e.g. a post-sync
  catch-up run) is ambiguous for any day inside its window that is not
  provably isolated: `DORAExecutor.ComputePartition` accumulates ONE
  `rows_written` total across the WHOLE window, so neither a zero nor a
  non-zero aggregate proves what any single interior day got. This command
  refuses (`"already_covered"`) rather than guess in either direction —
  verify via the `readback_hint` query and, if the day is genuinely
  uncovered, request it with a narrower `--day`/`--to` that does not land
  inside the ambiguous run's window.
- A run for the same day still **`pending`/`running`** under a different
  generation (an automatic trigger currently executing) is refused as
  `"in_progress"` — its eventual completion is invisible to the checks
  above, so inserting a manual run alongside it risks a genuine duplicate
  once both finish. Re-run once it settles.

Retrying the **identical** command (same `--family`/`--org`/`--day`/`--to`,
which derives the same `generation` — see below) is always safe: it reuses
the same run rather than inserting a duplicate, even across an in-flight
retry (`"already_ran"`) or after the run itself legitimately completed with
0 rows (a fresh generation is minted automatically so the retry actually
recomputes, rather than reloading the exhausted 0-row run forever).

Returns, per day in the requested span:

```json
{
  "family": "dora", "org": "c6a3...", "generation": "manual-backfill:dora:c6a3...:2026-08-25..2026-08-27",
  "days": [
    {"day": "2026-08-25", "status": "started", "run_id": "...", "partition_id": "..."},
    {"day": "2026-08-26", "status": "already_ran", "run_id": "...", "partition_id": "..."},
    {"day": "2026-08-27", "status": "already_covered", "run_id": "..."}
  ],
  "readback_hint": "ClickHouse: SELECT day, count() FROM dora_metrics_daily WHERE org_id = '...' AND day BETWEEN '2026-08-25' AND '2026-08-27' GROUP BY day ORDER BY day"
}
```

`status` is one of `started` (a new run/partition was inserted), `already_ran`
(idempotent retry, not an error), `already_covered` or `in_progress` (refused,
see above), or `error` (an unexpected store/publisher failure for that one
day — the command's own exit code is nonzero if ANY day is `error`, even
though the full per-day JSON is still printed to stdout for diagnosis).
`generation` is derived deterministically from the request's own flags
(family/org/day-range), never wall-clock time, so a retried invocation is
recognizable as the same logical request. Run the `readback_hint` query (or
the daily-redrive query below, substituted with the printed `run_id`s) to
confirm rows actually landed once the dispatched partition jobs execute — a
`"started"` status is dispatch confirmation, not completion.

**Observability**: `dev_health_remaining_metrics_manual_backfill_total{family,outcome}`
is wired but not live for THIS caller, for the identical reason
`dev_health_daily_metrics_redrive_partitions_total` above is not: `workerctl`
is a one-shot CLI with no Prometheus scrape endpoint. The durable record of a
manual backfill is the `remaining_metric_runs`/`remaining_metric_partitions`
rows themselves, findable by the printed `generation`:

```sql
SELECT run.id, run.status, partition.id, partition.status, partition.output_evidence
FROM remaining_metric_runs run
JOIN remaining_metric_partitions partition ON partition.run_id = run.id
WHERE run.org_id = '<org>' AND run.family = '<family>' AND run.generation = '<printed generation>';
```

### `dev-health-workerctl routes`

Inspect or control one fixed sync-dispatch transport route through the
authenticated, payload-redacted Go operator binary:

```bash
dev-health-workerctl routes status dispatch_sync_run

dev-health-workerctl routes apply \
  --reason deployment \
  --correlation-id change-123 \
  dispatch_sync_run

dev-health-workerctl routes pause \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run

dev-health-workerctl routes drain \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run

dev-health-workerctl routes resume \
  --transport celery \
  --reason maintenance \
  --correlation-id change-123 \
  dispatch_sync_run
```

The fixed kinds are `dispatch_sync_run`, `finalize_sync_run`, `post_sync`, and
`reference_discovery`. Supply the one-time service credential through
`WORKER_OPERATOR_TOKEN` or `WORKER_OPERATOR_TOKEN_FILE`; status requires
`workers:read`, while apply/pause/drain/resume require `workers:operate`. Mutations
are serialized per semantic database, persist audit intent before changing
state, and may return `outcome_unknown`; inspect the route before retrying.

The checked-in transport for all four sync-dispatch kinds is River and the
rollback transport is Celery. `routes apply` converges one unpaused Celery
route to its checked-in River transport after proving the matching capability
exists and no live outbox claim remains. It is idempotent when the route is
already active. `routes resume --transport celery` remains the explicit
rollback path. Before resuming on Celery, drain the external River queue for
the kind as well as the database claims: there must be no queued or running
River job and no pending or claimed outbox row. `routes drain` proves the
database-claim condition only; it does not inspect River job state.

---

## Maintenance

Operational cleanup tasks. Use `POSTGRES_URI`.

### `maintenance cleanup-tokens`

Delete expired refresh tokens.

```bash
dev-hops maintenance cleanup-tokens
```

### `maintenance cleanup-all`

Run all maintenance cleanup tasks (currently refresh-token cleanup).

```bash
dev-hops maintenance cleanup-all
```

---

## Billing

### `billing reconcile`

Run billing reconciliation against Stripe. Uses `POSTGRES_URI`.

```bash
# Reconcile all orgs
dev-hops billing reconcile

# Reconcile a single org since a date
dev-hops billing reconcile --org-id <uuid> --since 2025-01-01
```

**Options:**
| Option | Description |
|--------|-------------|
| `--org-id` | Reconcile a single organization (UUID). Omit to reconcile all orgs |
| `--since` | Only reconcile invoices on or after this date (ISO YYYY-MM-DD) |

---

## AI Governance

### `ai allowlist`

Manage the org-level AI tool allowlist (which AI tools/models are permitted). Requires `CLICKHOUSE_URI` **and** an organization id.

```bash
# Set a policy for a tool (optionally a specific model)
dev-hops ai allowlist set --tool claude-code --status allowed --reason "approved"
dev-hops ai allowlist set --tool claude-code --model opus --status deprecated

# List the latest allowlist entries for the org
dev-hops ai allowlist list
```

| Subcommand | Description |
|------------|-------------|
| `set` | Create/update an entry (`--tool`, optional `--model`, `--status {allowed,disallowed,deprecated}`, `--reason`) |
| `list` | Show the latest allowlist entries for the org |

---

## Work Graph

### `work-graph build`

> ⚠️ **Warning (CHAOS-2475):** The `work-graph build` command runs inline and requires configurations that the CLI doesn't enforce at startup. Running it inline can cause silent failures.
>
> **Interim Workaround:** We recommend triggering the equivalent Celery job on the `metrics` queue. See [Run workers and jobs](../../operate/run/workers-and-jobs.md) for details on Celery worker configuration.

Build work graph edges from raw data (issue → PR → commit linkages). Takes its ClickHouse DSN via its own **required** `--db` flag.

```bash
dev-hops work-graph build --db "$CLICKHOUSE_URI" \
  --from 2025-01-01 --to 2025-02-01
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | ClickHouse connection string (**required**) |
| `--from` | Start date (YYYY-MM-DD, default: 30 days ago) |
| `--to` | End date (YYYY-MM-DD, default: today) |
| `--repo-id` | Filter to a specific repository UUID |
| `--heuristic-window` | Days window for heuristic issue→PR matching (default: 7) |
| `--heuristic-confidence` | Confidence score for heuristic matches (default: 0.3) |
| `--allow-degenerate` | Allow single connected-component graphs (default: fail) |
| `--check-components` | Perform component analysis (enabled by default) |

---

## Investment

### `investment materialize`

Materialize WorkUnit investment categorization (theme/subcategory distributions and edges) into ClickHouse sinks. Takes its ClickHouse DSN via its own `--db` flag (default: `CLICKHOUSE_URI`).

```bash
# Full org materialization (publishes coverage marker)
dev-hops investment materialize --db "$CLICKHOUSE_URI" --org "$ORG_ID"

# Date-windowed refresh (unscoped → still publishes the org-wide coverage
# marker via the full-coverage membership projection; CHAOS-2776)
dev-hops investment materialize --window-days 30 --llm-provider none
```

**Options:**
| Option | Description |
|--------|-------------|
| `--db` | ClickHouse connection string (default: `CLICKHOUSE_URI`) |
| `--from` / `--to` | Date range (`--from` defaults to `--window-days` before `--to`; `--to` defaults to now) |
| `--window-days` | Window size when `--from` is not set (default: 30). Windowing does NOT suppress the org-wide membership marker — the post-run projection is full-coverage by construction (CHAOS-2776) |
| `--repo-id` / `--team-id` | Filter to specific repos/teams. Scoped runs skip the membership projection and publish no org-wide marker |
| `-l, --llm-provider` | LLM provider (`auto`, `openai`, `anthropic`, `local`, `mock`, `none`). Use `none` for distributions without explanations |
| `-m, --model` | LLM model name (overrides provider default) |
| `--persist-evidence-snippets` / `--no-persist-evidence-snippets` | Persist or skip extractive evidence quotes |
| `--force` | Force re-materialization |

> Investment categorization runs at **compute time** and persists distributions through sinks only.

---

## Recommendations

### `recommendations compute`

> ⚠️ **Warning (CHAOS-2475):** The `recommendations compute` command runs inline and requires configurations that the CLI doesn't enforce at startup. Running it inline can cause silent failures.
>
> **Interim Workaround:** We recommend triggering the equivalent Celery job on the `metrics` queue. See [Run workers and jobs](../../operate/run/workers-and-jobs.md) for details on Celery worker configuration.

Evaluate rule-based recommendations for a team and persist results to ClickHouse — both fired recommendations and explicit `fired=False` tombstones, so a recovered signal is cleared rather than left lingering. Uses `CLICKHOUSE_URI`.

```bash
dev-hops recommendations compute --team eng-core --window 7d

# Override the window with an explicit date range and print JSON
dev-hops recommendations compute --team eng-core \
  --since 2025-01-01 --until 2025-01-31 --output-json
```

**Options:**
| Option | Description |
|--------|-------------|
| `--team` | Team ID to evaluate (required) |
| `--window` | Evaluation window, e.g. `7d` or `14d` (default: `7d`) |
| `--since` | Override window start (exclusive end = `--until`) |
| `--until` | Override window end (inclusive). Requires `--since` |
| `--output-json` | Print fired recommendations as JSON to stdout |

---

## Reports

> ℹ️ **Note:** Reports are not managed or triggered via the CLI. They are managed entirely through the GraphQL API or the Report Center UI. See [Reports](../../use/reports/index.md) for details.

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

Reports can be scheduled with a five-field cron expression (via `scheduleCron` in the create/update mutation). Create and update validate the field count and value ranges before persistence. Invalid input returns an error that identifies how to correct it. The periodic scan for due reports was `dispatch_scheduled_reports` (a Celery beat task, run every 5 minutes) until CHAOS-4026 (2026-08-21) deleted it -- Go's `report.execute_scheduled` fixed schedule now owns that scan. `execute_saved_report` (the per-report execution work, dispatched via `execute_saved_report.apply_async(...)`) was not part of that cleanup and still runs on the `reports` Celery queue.

### Worker Configuration

Reports execution still runs on Celery's `reports` queue; see [Run workers and jobs](../../operate/run/workers-and-jobs.md) for how the Go-only runtime is started now that `workers start-worker` is gone.

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
# Apply all ordinary pending migrations
dev-hops migrate postgres
dev-hops migrate postgres upgrade

# Upgrade to a specific revision
dev-hops migrate postgres upgrade abc123

# Revert one migration
dev-hops migrate postgres downgrade -1

# Show current applied revision
dev-hops migrate postgres current

# Read-only application-schema check (exit 1 while required revisions are pending)
dev-hops migrate postgres status --check

# Show migration history
dev-hops migrate postgres history

# Show available heads
dev-hops migrate postgres heads
```

The migration graph has two branches after revision `0065`:

- `application_schema` contains ordinary application migrations and is always
  advanced by `dev-hops migrate postgres`;
- `river_cutover` contains revision `0066`, the Celery-to-River ownership
  activation, and remains pending by default.

Set `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1` only on the explicitly authorized
migration run after the River consumers are ready and Celery is drained. With
that opt-in, the migrator advances both heads. Without it, River support tables
and route records may remain present but unused while application schema such
as Ask Dev persistence continues to migrate normally.

`migrate postgres status --check` is read-only. It requires the current
`application_schema` head and, only when the cutover opt-in is present, the
`river_cutover` head.

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

# Exit non-zero if any migration is pending (read-only wait primitive for deploy tooling)
dev-hops migrate clickhouse status --check

# Remediate stale duplicate repo rows (dry-run unless --apply)
dev-hops migrate clickhouse repair
dev-hops migrate clickhouse repair --apply
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

## push

`dev-hops push` is the client CLI for [Customer Push](../../integrate/customer-push/overview.md)
— submitting your own data to `/api/v1/external-ingest/*` instead of relying on a FullChaos-managed
connector. See [Register a source](../../integrate/customer-push/register-source.md) for a full first-batch
walkthrough and [Record kinds and enums](../../reference/schemas/record-kinds-and-enums.md)
for the record kinds. `push` subcommands are excluded from `dev-hops`'s global `--org` auto-resolution
and ClickHouse/Postgres preflight entirely — `validate`/`sample` are fully offline; `batch`/`status`
talk to the FullChaos API over HTTP and resolve their own credentials (below).

### Credentials (`batch` / `status`)

| Flag | Env var | Notes |
|------|---------|-------|
| `--api-url` | `FULLCHAOS_API_URL` | FullChaos API base URL. |
| `--token` | `FULLCHAOS_INGEST_TOKEN` (deprecated alias: `FULLCHAOS_API_TOKEN`) | An `fcpush_...` ingest token — see [Register a source](../../integrate/customer-push/register-source.md). |
| `--org` | `FULLCHAOS_ORG_ID` | Organization id. |

A flag always wins over its env var. If any of the three can't be resolved, the command prints
`error: missing required: ...` to stderr and exits 2 — not an argparse `required=True` error,
so the env-var fallback still works.

### `dev-hops push validate <payload>`

Validates a batch envelope locally — **no network call**. Reads a JSON file, or `-` for stdin.

| Flag | Notes |
|------|-------|
| `--schema` | Schema version to validate against. Default and only supported value: `external-ingest.v1`. |
| `--json` | Emit machine-readable JSON to stdout instead of a human rejection table. |

```bash
dev-hops push validate batch.json
dev-hops push validate - < batch.json --json
```

### `dev-hops push sample`

Prints a canonical sample batch envelope built from the packaged example payloads (the same
files `GET /schemas/{version}` embeds) — no network call.

| Flag | Notes |
|------|-------|
| `--kind KIND` | One record kind, bare or versioned (e.g. `pull_request` or `pull_request.v1`). Mutually exclusive with `--all`. |
| `--all` | Combined batch envelope with one record of every kind. Mutually exclusive with `--kind`. |

```bash
dev-hops push sample --kind pull_request > sample.json
dev-hops push sample --all | dev-hops push validate -
```

### `dev-hops push batch <payload>`

Submits a batch to `POST /api/v1/external-ingest/batches`. Reads a JSON file, or `-` for stdin.

| Flag | Notes |
|------|-------|
| `--api-url`, `--token`, `--org` | See [Credentials](#credentials-batch-status) above. |
| `--poll` | Poll `GET /batches/{id}` until the batch reaches a terminal status, instead of returning immediately after the `202`/`200`. |
| `--poll-interval` | Seconds between polls. Default 5 (an internal floor of 0.5s is enforced). |
| `--poll-timeout` | Give up polling after this many seconds. |
| `--skip-limits-check` | Skip the `GET /schemas` limits pre-flight; enforce hardcoded client defaults (1000 records / 10MB) instead of the server's live limits. |
| `--json` | Emit machine-readable JSON to stdout. |

```bash
dev-hops push batch sample.json --poll
dev-hops push batch - --json < sample.json
```

### `dev-hops push status <ingestion_id>`

Fetches (and optionally polls) a batch's status via `GET /batches/{id}`.

| Flag | Notes |
|------|-------|
| `--api-url`, `--token`, `--org` | See [Credentials](#credentials-batch-status) above. |
| `--poll`, `--poll-interval`, `--poll-timeout` | Same semantics as `push batch`. |
| `--json` | Emit machine-readable JSON to stdout. |

```bash
dev-hops push status b6c1e6b0-...-uuid --poll
```

### `dev-hops push export <provider>`

Reserved extension point for provider-native export helpers (e.g. `github`, `gitlab`). **Not
implemented in v1** — every provider currently prints an error and exits with a data-failure
status. Use `dev-hops push sample` plus a hand-written export, or the provider's native
FullChaos connector, in the meantime.

### `push` exit codes

`push` uses its own exit-code contract, distinct from the rest of `dev-hops` (see
[Exit Codes](#exit-codes) below):

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Data-level failure — invalid payload, or a batch that completed with rejections / reached a terminal `failed` status |
| 2 | Usage error — bad/missing CLI args, or unresolved `--api-url`/`--token`/`--org` |
| 3 | Transport/API error after retries, or `stream_unavailable` |
| 4 | Poll timeout — the batch was still non-terminal when `--poll-timeout` elapsed |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error — includes argparse usage errors and missing required inputs surfaced by the [preflight](#input-validation-preflight) (e.g. unset `CLICKHOUSE_URI`/`POSTGRES_URI`/`ORG_ID`) |
| 3 | Authentication error |
| 4 | Rate limit exceeded |
