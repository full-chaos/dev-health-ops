"""The Helm chart must give the Python API and the Go workers ONE Valkey
keyspace (CHAOS-4226).

The Go finalize bumps ``cache_epoch:org:{org_id}`` through ``VALKEY_URI``;
the API's cache reads it through ``REDIS_URL``. Before this test the chart
rendered no ``VALKEY_URI`` at all (Go worker pods could not boot without an
operator-supplied Secret key) and ``REDIS_URL`` on DB 0, while the Go client
refuses any DB but 1 (internal/storage/valkey/factory.go) -- so even a
hand-wired worker would have bumped an epoch the API never read.
"""

from __future__ import annotations

from pathlib import Path
from subprocess import run

import yaml

CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"


def _render(*extra: str) -> list[dict]:
    rendered = run(
        ["helm", "template", "keyspace-contract", str(CHART), *extra],
        check=True,
        capture_output=True,
        text=True,
    )
    return [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]


def _configmap(docs: list[dict]) -> dict:
    for doc in docs:
        if doc.get("kind") == "ConfigMap" and doc["metadata"]["name"].endswith(
            "-config"
        ):
            return doc["data"]
    raise AssertionError("main ConfigMap not rendered")


def test_api_and_go_workers_share_one_valkey_url_on_db_1() -> None:
    data = _configmap(_render())
    assert data["REDIS_URL"] == data["VALKEY_URI"]
    assert data["VALKEY_URI"].endswith(":6379/1"), data["VALKEY_URI"]


def test_go_worker_pods_receive_the_shared_config_map() -> None:
    docs = _render()
    workers = [
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].startswith("keyspace-contract-dev-health-go-")
        and "pgbouncer" not in doc["metadata"]["name"]
    ]
    names = {doc["metadata"]["name"] for doc in workers}
    # The sync coordinator is the family CHAOS-4226 makes Valkey-mandatory.
    assert "keyspace-contract-dev-health-go-sync" in names, sorted(names)
    config_name = next(
        doc["metadata"]["name"]
        for doc in docs
        if doc.get("kind") == "ConfigMap"
        and doc["metadata"]["name"].endswith("-config")
    )
    for deployment in workers:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        sources = [
            ref["configMapRef"]["name"]
            for ref in container.get("envFrom", [])
            if "configMapRef" in ref
        ]
        assert config_name in sources, deployment["metadata"]["name"]
        # No pod pins its own VALKEY_URI, so the ConfigMap value wins.
        assert all(env["name"] != "VALKEY_URI" for env in container.get("env", []))


def test_explicit_external_valkey_uri_suppresses_the_derived_one() -> None:
    data = _configmap(
        _render(
            "--set",
            "secrets.data.VALKEY_URI=redis://external-valkey:6379/1",
            "--set",
            "secrets.data.REDIS_URL=redis://external-valkey:6379/1",
        )
    )
    assert "VALKEY_URI" not in data
    assert "REDIS_URL" not in data


def test_api_pod_resolves_the_same_shared_config_map() -> None:
    """The API (REDIS_URL reader) and the Go sync worker (VALKEY_URI reader)
    must both take their URL from the one ConfigMap -- neither pins a
    conflicting value in its own env."""
    docs = _render()
    api = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"] == "keyspace-contract-dev-health-api"
    )
    config_name = next(
        doc["metadata"]["name"]
        for doc in docs
        if doc.get("kind") == "ConfigMap"
        and doc["metadata"]["name"].endswith("-config")
    )
    container = api["spec"]["template"]["spec"]["containers"][0]
    sources = [
        ref["configMapRef"]["name"]
        for ref in container.get("envFrom", [])
        if "configMapRef" in ref
    ]
    assert config_name in sources
    assert all(
        env["name"] not in {"REDIS_URL", "VALKEY_URI"}
        for env in container.get("env", [])
    )
