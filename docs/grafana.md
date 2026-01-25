# Grafana

This repo ships Grafana provisioning for ClickHouse dashboards under `grafana/`.

## Start Grafana + ClickHouse (dev)

From the repo root, use Docker Compose directly (the CLI grafana command is deprecated):

```bash
docker compose -f compose.yml up -d
```

- Grafana: `http://localhost:3000` (default `admin` / `admin`)
- ClickHouse HTTP: `http://localhost:8123`

## Datasource provisioning

Provisioning file:
- `grafana/datasources/clickhouse.yaml`

It reads connection settings from environment variables inside the Grafana container:
- `CLICKHOUSE_HOST` (defaults to `clickhouse` in the provided docker compose)
- `CLICKHOUSE_PORT` (defaults to `8123`)
- `CLICKHOUSE_DB`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`

## Dashboards

Dashboards are provisioned automatically into the “Developer Health” folder:
- Repo Health: `grafana/dashboards/repo_health.json`
- Code Hotspots: `grafana/dashboards/code_hotspots.json`
- Work Tracking: `grafana/dashboards/work_tracking.json`
- Advanced Work Tracking: `grafana/dashboards/advanced_work_tracking.json`
- Collaboration: `grafana/dashboards/collaboration.json`
- Quality & Risk: `grafana/dashboards/quality_risk.json`
- Well-being (team-level): `grafana/dashboards/wellbeing.json`

The Work Tracking dashboard supports filtering by `provider`, `team_id`, and `work_scope_id` (Jira project key / GitHub repo or project board / GitLab project path).

## Expected ClickHouse tables

Git facts (synced via the CLI, e.g., `python cli.py sync ...`):
- `repos`
- `git_commits`
- `git_commit_stats`
- `git_pull_requests`

Derived metrics (computed by `scripts/compute_metrics_daily.py`):
- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`
- `team_metrics_daily`
- `work_item_metrics_daily`
- `work_item_user_metrics_daily`
- `work_item_cycle_times`

## Populate data

1) Sync git data into ClickHouse:

```bash
python cli.py sync git --provider local --db "clickhouse://localhost:8123/default" --repo-path .
```

2) Compute derived metrics:

```bash
python cli.py sync work-items --provider all --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
python cli.py metrics daily --date 2025-02-01 --backfill 30 --db "clickhouse://localhost:8123/default"
```

Work tracking providers require credentials; see `docs/task_trackers.md`.

## Troubleshooting

- The provisioned dashboards use repo *names* (e.g. `org/repo`) in the Repo picker and support multi-select + “All”. Repo filtering is applied by resolving selected repo names to `repos.id` in ClickHouse.
