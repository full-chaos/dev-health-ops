"""CHAOS-4245: assert the prod otel-collector config actually scrapes every
Prometheus target named in .remember/lanes/lane-4245/inventory-2026-08-26.md.

Pure YAML-structure assertions -- no docker, no live collector, no network.
Mirrors the style of ``tests/test_compose_config.py`` (unmarked, always runs
in the full unit suite).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
OTEL_CONFIG_PATH = REPO_ROOT / "deploy" / "otel" / "otel.prod.yml"

GO_WORKER_SERVICES = [
    "go-worker-heavy",
    "go-worker-ops",
    "go-worker-sync",
    "go-worker-sync-provider",
    "go-reconciler",
    "go-scheduler",
    "go-stream-external",
    "go-stream-ingest",
    "go-stream-pagerduty",
]


def _load_config() -> dict:
    with OTEL_CONFIG_PATH.open() as handle:
        return yaml.safe_load(handle)


def _scrape_configs(config: dict) -> list[dict]:
    return config["receivers"]["prometheus"]["config"]["scrape_configs"]


def _job(config: dict, name: str) -> dict:
    jobs = {job["job_name"]: job for job in _scrape_configs(config)}
    assert name in jobs, f"missing prometheus scrape job {name!r}; have {sorted(jobs)}"
    return jobs[name]


def test_metrics_pipeline_wires_every_new_receiver() -> None:
    config = _load_config()
    metrics_receivers = set(config["service"]["pipelines"]["metrics"]["receivers"])
    for receiver in ("prometheus", "postgresql", "redis"):
        assert receiver in metrics_receivers, (
            f"receiver {receiver!r} is declared but not wired into "
            "service.pipelines.metrics.receivers"
        )
    # The pre-existing targets must not have been dropped.
    for receiver in ("otlp", "docker_stats", "hostmetrics"):
        assert receiver in metrics_receivers


def test_clickhouse_prometheus_scrape_target() -> None:
    config = _load_config()
    job = _job(config, "clickhouse")
    targets = job["static_configs"][0]["targets"]
    assert targets == ["clickhouse:9363"]
    assert job["metrics_path"] == "/metrics"
    # Low-cardinality guard: the largest, least-actionable CH metric family
    # must be dropped via relabeling.
    dropped = {
        rule["regex"]
        for rule in job.get("metric_relabel_configs", [])
        if rule.get("action") == "drop"
    }
    assert any("ClickHouseProfileEvents" in pattern for pattern in dropped)


def test_api_prometheus_scrape_target() -> None:
    config = _load_config()
    job = _job(config, "api")
    targets = job["static_configs"][0]["targets"]
    assert targets == ["api:8000"]


def test_go_worker_fleet_scrape_covers_every_service() -> None:
    config = _load_config()
    job = _job(config, "go-workers")
    names = job["dns_sd_configs"][0]["names"]
    assert set(names) == set(GO_WORKER_SERVICES), (
        "go-workers scrape job must cover exactly the 9 services declared in "
        "compose.go-workers.yml"
    )
    assert job["dns_sd_configs"][0]["port"] == 8080
    assert job["dns_sd_configs"][0]["type"] == "A"


def test_otelcol_self_scrape_target() -> None:
    config = _load_config()
    job = _job(config, "otelcol-internal")
    targets = job["static_configs"][0]["targets"]
    assert targets == ["localhost:8888"]
    # The self-scrape target only produces data if telemetry.metrics actually
    # exposes a pull endpoint on the same port.
    readers = config["service"]["telemetry"]["metrics"]["readers"]
    ports = {
        reader["pull"]["exporter"]["prometheus"]["port"]
        for reader in readers
        if "pull" in reader
    }
    assert 8888 in ports


def test_postgresql_receiver_points_directly_at_postgres_not_pgbouncer() -> None:
    config = _load_config()
    receiver = config["receivers"]["postgresql"]
    assert receiver["endpoint"] == "${env:POSTGRES_HOST}:5432"
    assert receiver["username"] == "${env:POSTGRES_USER}"
    assert receiver["password"] == "${env:POSTGRES_PASSWORD}"


def test_redis_receiver_points_at_valkey() -> None:
    config = _load_config()
    receiver = config["receivers"]["redis"]
    assert receiver["endpoint"] == "valkey:6379"


def test_pgbouncer_has_no_receiver_yet() -> None:
    """CHAOS-4245 inventory: no pgbouncer_exporter exists, so there is
    deliberately no pgbouncer scrape target. This test pins that decision so
    a future PR adding one updates this test on purpose, not by accident.
    """
    config = _load_config()
    job_names = {job["job_name"] for job in _scrape_configs(config)}
    assert not any("pgbouncer" in name for name in job_names)
