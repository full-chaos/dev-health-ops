# CLI

The CLI is the primary way to run sync jobs, compute metrics, and manage dashboards.

## Common commands

### Sync local Git data

```bash
python cli.py sync git --provider local --db "<DB_CONN>" --repo-path /path/to/repo
```

### Sync teams

```bash
python cli.py sync teams --provider config --db "<DB_CONN>" --path /path/to/teams.yml
```

### Sync work items

```bash
python cli.py sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*" --db "<DB_CONN>"
```

### Metrics

```bash
python cli.py metrics daily --db "<DB_CONN>"
python cli.py metrics complexity --repo-path . -s "*" --db "<DB_CONN>"
```

`metrics daily` defaults to `--provider auto`, which loads work items from the database only.

### Fixtures

```bash
python cli.py fixtures generate --db "<DB_CONN>" --days 30
```

## Flags and overrides

CLI flags override environment variables. Use `--db` or `DATABASE_URI` to target a specific database.
