from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
OTEL_KEYS = {
    "OTEL_ENABLED",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_SERVICE_NAME",
    "OTEL_METRIC_EXPORT_INTERVAL",
}


def _yaml(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text())


def test_compose_worker_fleets_receive_otel_metric_configuration() -> None:
    """Root compose.yml no longer defines a Celery `worker` service (CHAOS-5589
    deleted the celery-legacy fleet outright); only the production Celery
    fleet remains to check here."""
    worker = _yaml("deploy/docker-compose/compose.production.yml")["services"]["worker"]
    assert OTEL_KEYS <= set(worker["environment"])


def test_swarm_worker_fleet_receives_otel_metric_configuration() -> None:
    worker = _yaml("deploy/docker-swarm/stack.yml")["services"]["worker"]
    assert OTEL_KEYS <= set(worker["environment"])


def test_kubernetes_and_helm_workers_receive_otel_metric_configuration() -> None:
    kubernetes = _yaml("deploy/kubernetes/configmap.yaml")["data"]
    helm = _yaml("deploy/helm/dev-health/values.yaml")["config"]
    assert OTEL_KEYS <= set(kubernetes)
    assert OTEL_KEYS <= set(helm)
