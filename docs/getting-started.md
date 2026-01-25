# Getting started

## Install

If you are using the project as a package:

```bash
pip install dev-health-ops
```

If you are working from source:

```bash
pip install -r requirements.txt
```

## Docs site

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

## Quick start

### Sync a local repository

```bash
python cli.py sync git --provider local --db "<DB_CONN>" --repo-path /path/to/repo
```

### Sync work items from GitHub

```bash
python cli.py sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*" --db "<DB_CONN>"
```

### Compute daily metrics

```bash
python cli.py metrics daily --db "<DB_CONN>"
```

### Bring up Grafana dashboards

```bash
docker compose -f compose.yml up -d
```

## Environment notes

CLI flags override environment variables. Common env vars:

- `DATABASE_URI`
- `SECONDARY_DATABASE_URI`
- `GITHUB_TOKEN`
- `GITLAB_TOKEN`
- `REPO_PATH`
