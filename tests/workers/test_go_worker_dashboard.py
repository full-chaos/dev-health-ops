from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = ROOT / "deploy" / "grafana" / "dashboards" / "go-workers.json"


def test_go_worker_dashboard_covers_required_runtime_signals() -> None:
    dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))
    expressions = "\n".join(
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    )

    required_metrics = {
        "worker_job_oldest_age_seconds",
        "worker_jobs_available",
        "worker_jobs_running",
        "worker_execution_saturation_ratio",
        "worker_job_attempts_total",
        "worker_job_duration_seconds_bucket",
        "worker_domain_state_mismatch_total",
        "worker_stream_lag",
        "worker_stream_pending",
        "worker_database_pool_saturation_ratio",
    }
    for metric in required_metrics:
        assert metric in expressions

    panel_ids = [panel["id"] for panel in dashboard["panels"]]
    assert len(panel_ids) == len(set(panel_ids))
    assert dashboard["refresh"] == "30s"


def test_go_worker_dashboard_queries_are_low_cardinality_and_payload_free() -> None:
    serialized = DASHBOARD_PATH.read_text(encoding="utf-8")
    for prohibited in (
        "organization_id",
        "repository",
        "job_id",
        "correlation_id",
        "encoded_args",
        "payload",
        "error_text",
    ):
        assert prohibited not in serialized
