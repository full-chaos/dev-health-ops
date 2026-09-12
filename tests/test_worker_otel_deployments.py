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


# CHAOS-5589 deleted the Celery `worker` service outright from compose.yml,
# compose.production.yml, and docker-swarm/stack.yml (R146: not a rollback
# target) -- there is no Celery/Python worker container left anywhere in any
# compose surface to carry these Python OTel env vars. The Go worker fleet's
# own telemetry configuration is covered below, through Kubernetes/Helm.


def test_kubernetes_and_helm_workers_receive_otel_metric_configuration() -> None:
    kubernetes = _yaml("deploy/kubernetes/configmap.yaml")["data"]
    helm = _yaml("deploy/helm/dev-health/values.yaml")["config"]
    assert OTEL_KEYS <= set(kubernetes)
    assert OTEL_KEYS <= set(helm)
