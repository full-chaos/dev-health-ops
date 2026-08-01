from pathlib import Path
from subprocess import run

import yaml


def test_api_liveness_is_process_only_while_readiness_checks_dependencies() -> None:
    chart_path = Path(__file__).parents[1] / "deploy/helm/dev-health"
    rendered = run(
        [
            "helm",
            "template",
            "probe-contract",
            chart_path,
            "--show-only",
            "templates/api-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    deployment = yaml.safe_load(rendered.stdout)
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert container["livenessProbe"]["httpGet"]["path"] == "/ready"
    assert container["readinessProbe"]["httpGet"]["path"] == "/health"
