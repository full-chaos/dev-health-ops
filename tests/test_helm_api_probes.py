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


def test_query_api_deployment_ports_are_unique_per_port_and_protocol() -> None:
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

    ports = container["ports"]
    keys = [(port["containerPort"], port.get("protocol", "TCP")) for port in ports]
    assert len(keys) == len(set(keys)), (
        "query-api container ports must be unique on (containerPort, protocol): "
        "server-side apply keys the ports list on that pair and rejects a "
        "duplicate, so /metrics is scraped through the `http` port"
    )
    assert [port["name"] for port in ports] == ["http"]
