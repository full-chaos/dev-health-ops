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


def test_query_api_deployment_exposes_a_named_metrics_port() -> None:
    chart_path = Path(__file__).parents[1] / "deploy/helm/dev-health"
    rendered = run(
        [
            "helm",
            "template",
            "probe-contract",
            chart_path,
            "--set",
            "queryApi.enabled=true",
            "--show-only",
            "templates/query-api-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    documents = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]
    deployment = next(doc for doc in documents if doc["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    ports_by_name = {port["name"]: port for port in container["ports"]}
    assert "metrics" in ports_by_name, (
        "query-api container must carry a port named `metrics` (the same "
        "convention go-workers.yaml uses) so pod-based Prometheus discovery "
        "finds its /metrics route"
    )
    assert (
        ports_by_name["metrics"]["containerPort"]
        == ports_by_name["http"]["containerPort"]
    )
