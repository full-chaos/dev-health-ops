"""The query-api internal listener is wired on Kubernetes (queryApi.internal).

query-api serves /metrics, the X-DH-Internal-* identity routes and /query/proof-write on a
second, internal listener (QUERY_API_INTERNAL_ADDR), never on the public query listener. A
deployment that does not set that address and expose the port has no scrape target and no path
for the proof-write route. These tests render the chart and check the env, the named
container port, the internal Service and the NetworkPolicy agree.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "qai"
_FULLNAME = f"{_RELEASE}-dev-health"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _template(*sets: str) -> subprocess.CompletedProcess[str]:
    argv = ["helm", "template", _RELEASE, str(_CHART), "--set", "queryApi.enabled=true"]
    for item in sets:
        argv += ["--set", item]
    return subprocess.run(argv, capture_output=True, text=True)


def _docs(*sets: str) -> list[dict]:
    completed = _template(*sets)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _named(docs: list[dict], kind: str, name: str) -> dict:
    return next(
        doc for doc in docs if doc["kind"] == kind and doc["metadata"]["name"] == name
    )


def _query_api_container(docs: list[dict]) -> dict:
    deployment = _named(docs, "Deployment", f"{_FULLNAME}-query-api")
    return deployment["spec"]["template"]["spec"]["containers"][0]


def test_internal_listener_is_off_by_default() -> None:
    docs = _docs()
    assert not [d for d in docs if "query-api-internal" in d["metadata"]["name"]]
    container = _query_api_container(docs)
    assert "QUERY_API_INTERNAL_ADDR" not in {e["name"] for e in container["env"]}
    assert [p["name"] for p in container["ports"]] == ["http"]


def test_enabled_sets_the_listener_address_and_a_named_port() -> None:
    container = _query_api_container(_docs("queryApi.internal.enabled=true"))
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["QUERY_API_INTERNAL_ADDR"] == ":8091", env
    ports = {p["name"]: p["containerPort"] for p in container["ports"]}
    assert ports == {"http": 8090, "internal": 8091}, ports


def test_enabled_renders_an_internal_service_targeting_the_named_port() -> None:
    docs = _docs("queryApi.internal.enabled=true")
    service = _named(docs, "Service", f"{_FULLNAME}-query-api-internal")
    assert service["spec"]["type"] == "ClusterIP"
    (port,) = service["spec"]["ports"]
    assert (port["port"], port["targetPort"]) == (8091, "internal"), port
    deployment = _named(docs, "Deployment", f"{_FULLNAME}-query-api")
    pod_labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert service["spec"]["selector"].items() <= pod_labels.items(), (
        service["spec"]["selector"],
        pod_labels,
    )
    # The public Service is unchanged and never names the internal port.
    public = _named(docs, "Service", f"{_FULLNAME}-query-api")
    assert [p["port"] for p in public["spec"]["ports"]] == [8090]


def test_extra_allowed_pods_are_added_to_the_internal_port_sources() -> None:
    docs = _docs(
        "queryApi.internal.enabled=true",
        "queryApi.internal.allowedFrom[0].matchLabels.app=tools",
    )
    policy = _named(docs, "NetworkPolicy", f"{_FULLNAME}-query-api-internal")
    internal_rule = policy["spec"]["ingress"][1]
    selectors = [source["podSelector"] for source in internal_rule["from"]]
    assert len(selectors) == 1, selectors
    assert {"matchLabels": {"app": "tools"}} in selectors, selectors
    assert all(s != {} for s in selectors)


def test_internal_port_equal_to_the_public_port_is_refused() -> None:
    completed = _template(
        "queryApi.internal.enabled=true", "queryApi.internal.port=8090"
    )
    assert completed.returncode != 0
    assert "must differ from queryApi.port" in completed.stderr


def test_the_metrics_comment_no_longer_points_the_scrape_at_the_public_port() -> None:
    text = (_CHART / "templates" / "query-api-deployment.yaml").read_text()
    assert "Scrape via `http`" not in text
    assert "listener only (queryApi.internal.enabled)" in text


_TOOLS_POD_SELECTOR = '[{"matchLabels":{"run":"dev-health-go-api-tools-oneoff"}}]'


def test_the_documented_tools_pod_selector_renders_as_an_extra_source() -> None:
    """The bootstrap runbook tells the operator to admit the tools pod (which runs
    `dho goapi prove-write`) with this exact value; the default policy does not admit it."""
    runbook = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "operate"
        / "runbooks"
        / "query-api-bootstrap.md"
    ).read_text()
    assert f"queryApi.internal.allowedFrom={_TOOLS_POD_SELECTOR}" in runbook

    default = _named(
        _docs("queryApi.internal.enabled=true"),
        "NetworkPolicy",
        f"{_FULLNAME}-query-api-internal",
    )
    tools = {"podSelector": {"matchLabels": {"run": "dev-health-go-api-tools-oneoff"}}}
    assert all(tools not in rule.get("from", []) for rule in default["spec"]["ingress"])

    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            "--set",
            "queryApi.enabled=true",
            "--set",
            "queryApi.internal.enabled=true",
            "--set-json",
            f"queryApi.internal.allowedFrom={_TOOLS_POD_SELECTOR}",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    docs = [d for d in yaml.safe_load_all(completed.stdout) if d]
    policy = _named(docs, "NetworkPolicy", f"{_FULLNAME}-query-api-internal")
    assert tools in policy["spec"]["ingress"][1]["from"], policy["spec"]["ingress"]
