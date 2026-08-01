from pathlib import Path

import yaml


def test_api_liveness_is_process_only_while_readiness_checks_dependencies() -> None:
    values_path = Path(__file__).parents[1] / "deploy/helm/dev-health/values.yaml"
    values = yaml.safe_load(values_path.read_text())

    assert values["api"]["livenessProbe"]["httpGet"]["path"] == "/ready"
    assert values["api"]["readinessProbe"]["httpGet"]["path"] == "/health"
