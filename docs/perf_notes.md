# ClickHouse performance assessment note

## Target queries

- Grafana work tracking rollups using `argMax` + `GROUP BY` on `stats.work_item_metrics_daily` and `stats.work_item_state_durations_daily` (see `grafana/dashboards/work_tracking.json`).
- Grafana advanced work tracking rollups on `stats.work_item_metrics_daily`, `stats.work_item_state_durations_daily`, and `stats.work_item_cycle_times` (see `grafana/dashboards/advanced_work_tracking.json`).
- Hotspot explorer joins between `stats.file_metrics_daily` and `stats.file_hotspot_daily` (see `grafana/dashboards/code_hotspots.json`).
- Investment flow edges from `stats.v_investment_flow_edges` (see `grafana/dashboards/investment_areas.json`).
- IC landscape scatter queries on `stats.v_ic_landscape_points` (see `grafana/dashboards/developer_landscape.json`).

## Current timings

- No ClickHouse query timings or `system.query_log` samples are checked into the repo.
- No performance benchmarks or issue notes reference slow ClickHouse queries.
- Timing status: unchecked (no evidence to confirm slow queries).
- Use `python cli.py audit perf --db <clickhouse_uri>` to sample slow queries from `system.query_log` (requires `log_queries` enabled).

## Decision

Defer ClickHouse perf migrations until we have concrete timings that show slow queries. This task is non-blocking for Phase 0.
