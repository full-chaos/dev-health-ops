# CLI

The CLI is the primary way to run sync jobs, compute metrics, and manage dashboards.

## Common commands

### Sync local Git data

```bash
dev-hops sync git --provider local --db "<DB_CONN>" --repo-path /path/to/repo
```

### Sync teams

```bash
dev-hops sync teams --provider config --db "<DB_CONN>" --path /path/to/teams.yml
```

### Sync work items

```bash
dev-hops sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*" --db "<DB_CONN>"
```

### Metrics

```bash
dev-hops metrics daily --db "<DB_CONN>"
dev-hops metrics complexity --repo-path . -s "*" --db "<DB_CONN>"
```

`metrics daily` defaults to `--provider auto`, which loads work items from the database only.

### Fixtures

```bash
dev-hops fixtures generate --db "<DB_CONN>" --days 30
```

## Flags and overrides

CLI flags override environment variables. Use `--db` or `DATABASE_URI` to target a specific database.
