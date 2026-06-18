# CLI

The CLI is the primary way to run sync jobs, compute metrics, and manage dashboards.

## Common commands

### Sync local Git data

```bash
CLICKHOUSE_URI="clickhouse://localhost:8123/default" \
dev-hops sync git --provider local --repo-path /path/to/repo
```

### Sync teams

```bash
POSTGRES_URI="postgresql+asyncpg://localhost:5555/postgres" \
dev-hops sync teams --provider config --path /path/to/teams.yml
```

### Sync work items

```bash
CLICKHOUSE_URI="clickhouse://localhost:8123/default" \
dev-hops sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*"
```

### Metrics

```bash
CLICKHOUSE_URI="clickhouse://localhost:8123/default" dev-hops metrics daily
CLICKHOUSE_URI="clickhouse://localhost:8123/default" dev-hops metrics complexity --repo-path . -s "*"
```

`metrics daily` defaults to `--provider auto`, which loads work items from the database only.

### Fixtures

```bash
CLICKHOUSE_URI="clickhouse://localhost:8123/default" dev-hops fixtures generate --days 30
```

### Migrations

Use `migrate clickhouse repair` when ClickHouse contains duplicate `repos`
records for the same repository `id` across different `org_id`s (typically
from running fixtures or syncs under a changed `--org`). The command previews
by default and only deletes rows when `--apply` is passed.

```bash
# PostgreSQL (Alembic) — users, orgs, settings
dev-hops migrate postgres
dev-hops migrate postgres upgrade          # same as above
dev-hops migrate postgres current          # show current revision
dev-hops migrate postgres history          # show migration history

# One-time data migration: legacy parent/child sync configs -> integration/
# source/dataset model (CHAOS-2516). Idempotent; safe to re-run.
dev-hops migrate configs-to-integrations --dry-run   # preview, no writes
dev-hops migrate configs-to-integrations             # apply + commit

# ClickHouse — analytics tables (commits, PRs, metrics, etc.)
dev-hops migrate clickhouse
dev-hops migrate clickhouse upgrade        # same as above
dev-hops migrate clickhouse status         # show applied/pending migrations

# Repair stale duplicate rows in `repos` (different org_id, same id)
dev-hops migrate clickhouse repair                   # dry-run; no writes
dev-hops migrate clickhouse repair --org <uuid>      # dry-run scoped to one org
dev-hops migrate clickhouse repair --apply           # delete the rows shown by dry-run
dev-hops migrate clickhouse repair --apply --org <uuid>
```

> Run both migration commands after setting up a fresh environment, before any sync or metrics commands.

## Flags and overrides

CLI flags override environment variables. Set `CLICKHOUSE_URI` for analytics and `POSTGRES_URI` for semantic data. Subcommands accept `--sink` for analytics and `--since`/`--before`/`--backfill` for date ranges.

## Input validation

Commands that need a database connection or an organization id are validated **before** they run. If a required input is missing, the command fails fast with a usage error (**exit code 2**) that names exactly what is missing, rather than failing partway through:

```bash
$ dev-hops metrics compounding-risk        # CLICKHOUSE_URI / org not set
dev-health-ops metrics compounding-risk: error: missing required input(s):
  - ClickHouse analytics database — pass --analytics-db or set CLICKHOUSE_URI (...)
  - organization id — pass --org or set ORG_ID (could not auto-resolve ...)
```

Every affected command lists its requirements at the bottom of `--help` (a `Requires:` line). See the [CLI Reference requirement matrix](ops/cli-reference.md#input-validation-preflight) for the full per-command list.
