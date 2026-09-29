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


def test_internal_port_is_open_to_this_releases_api_pods_only_by_default() -> None:
    docs = _docs("queryApi.internal.enabled=true")
    policy = _named(docs, "NetworkPolicy", f"{_FULLNAME}-query-api-internal")
    deployment = _named(docs, "Deployment", f"{_FULLNAME}-query-api")
    pod_labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert policy["spec"]["podSelector"]["matchLabels"].items() <= pod_labels.items()
    assert policy["spec"]["policyTypes"] == ["Ingress"]
    public_rule, internal_rule = policy["spec"]["ingress"]
    assert public_rule == {"ports": [{"protocol": "TCP", "port": 8090}]}
    assert internal_rule["ports"] == [{"protocol": "TCP", "port": 8091}]
    # The internal listener trusts X-DH-Internal-* headers from any peer that can connect, so the
    # source is this release's api pods: never every pod in the namespace.
    api = _named(docs, "Deployment", f"{_FULLNAME}-api")["spec"]["template"][
        "metadata"
    ]["labels"]
    (source,) = internal_rule["from"]
    assert source["podSelector"] != {}, "the internal port is open to every pod"
    assert source["podSelector"]["matchLabels"].items() <= api.items()
    assert not source["podSelector"]["matchLabels"].items() <= pod_labels.items(), (
        "the query-api pods themselves are not the api pods"
    )


def test_extra_allowed_pods_are_added_to_the_internal_port_sources() -> None:
    docs = _docs(
        "queryApi.internal.enabled=true",
        "queryApi.internal.allowedFrom[0].matchLabels.app=tools",
    )
    policy = _named(docs, "NetworkPolicy", f"{_FULLNAME}-query-api-internal")
    internal_rule = policy["spec"]["ingress"][1]
    selectors = [source["podSelector"] for source in internal_rule["from"]]
    assert len(selectors) == 2, selectors
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
