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
