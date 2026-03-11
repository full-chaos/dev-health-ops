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

## Flags and overrides

CLI flags override environment variables. Set `CLICKHOUSE_URI` for analytics and `POSTGRES_URI` for semantic data. Subcommands accept `--sink` for analytics and `--since`/`--before`/`--backfill` for date ranges.
