"""Rendered Helm contract for the Go PgBouncer topology.

This is intentionally a chart-level oracle: values alone cannot prove which
pool endpoint a long-running binary receives, and a template-only assertion
cannot prove the rendered Secret references stay role-scoped.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "pool-contract"
_DEPLOYMENT = _CHART.parents[1] / "go-workers" / "deployment.json"
_PROFILE_NAMES = {
    "heavy",
    "ops",
    "sync",
    "reconciler",
    "scheduler",
    "stream-external",
    "stream-ingest",
    "stream-pagerduty",
}
_COORDINATOR_GROUPS = {"reconciler", "scheduler"}
_POOLERS = {
    "transaction": (6432, "transaction", 20, 1000, "devhealth_domain"),
    "queue-session": (6433, "session", 22, 128, "devhealth_queue"),
    "coordinator-session": (6434, "session", 10, 32, "devhealth_coordinator"),
}
_ROLE_SECRET_KEYS = {
    "RIVER_DOMAIN_DATABASE_PASSWORD",
    "RIVER_QUEUE_DATABASE_PASSWORD",
    "RIVER_COORDINATOR_DATABASE_PASSWORD",
    "POSTGRES_URI",
    "WORKER_DATABASE_URI",
    "COORDINATOR_DATABASE_URI",
}


def _render(*args: str) -> list[dict]:
    completed = subprocess.run(
        ["helm", "template", _RELEASE, _CHART, *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [document for document in yaml.safe_load_all(completed.stdout) if document]


def _go_values(*extra: str) -> list[str]:
    return [
        "--set",
        "goWorkers.enabled=true",
        "--set",
        "goWorkers.pgbouncer.enabled=true",
        "--set",
        "goWorkers.pgbouncer.postgres.host=postgres.internal",
        "--set",
        "goWorkers.pgbouncer.postgres.database=devhealth",
        "--set-string",
        "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=domain-password",
        "--set-string",
        "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=queue-password",
        "--set-string",
        "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=coordinator-password",
        *extra,
    ]


def _env(container: dict) -> dict[str, dict]:
    return {item["name"]: item for item in container["env"]}


def _river_groups() -> dict[str, dict]:
    manifest = json.loads(_DEPLOYMENT.read_text(encoding="utf-8"))
    return {
        process["name"]: process
        for process in manifest["processes"]
        if process["runtime"] == "river" and process["binary"] == "dev-health-worker"
    }


def _queue_concurrency(process: dict) -> str:
    return ",".join(
        f"{entry['queue']}={entry['max_workers']}" for entry in process["queue_workers"]
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_go_workers_require_the_pgbouncer_topology() -> None:
    rejected = subprocess.run(
        ["helm", "template", _RELEASE, _CHART, "--set", "goWorkers.enabled=true"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "values don't meet the specifications" in rejected.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_go_pgbouncer_render_scopes_role_dsns_and_preserves_direct_migrations() -> None:
    documents = _render(*_go_values())
    secrets = {
        doc["metadata"]["name"]: doc for doc in documents if doc["kind"] == "Secret"
    }
    runtime_secret = secrets[f"{_RELEASE}-dev-health-go-pgbouncer"]
    assert set(runtime_secret["stringData"]) == _ROLE_SECRET_KEYS
    assert "MIGRATION_DATABASE_URI" not in runtime_secret["stringData"]
    assert "MIGRATION_DATABASE_URI_FILE" not in runtime_secret["stringData"]
    assert runtime_secret["stringData"]["POSTGRES_URI"].endswith(
        "-go-pgbouncer-transaction:6432/devhealth"
    )
    assert runtime_secret["stringData"]["WORKER_DATABASE_URI"].endswith(
        "-go-pgbouncer-queue-session:6433/devhealth"
    )
    assert runtime_secret["stringData"]["COORDINATOR_DATABASE_URI"].endswith(
        "-go-pgbouncer-coordinator-session:6434/devhealth"
    )

    pooler_deployments = {
        doc["metadata"]["labels"]["app.kubernetes.io/component"]: doc
        for doc in documents
        if doc["kind"] == "Deployment"
        and doc["metadata"]["labels"]["app.kubernetes.io/component"].startswith(
            "go-pgbouncer-"
        )
    }
    assert set(pooler_deployments) == {f"go-pgbouncer-{name}" for name in _POOLERS}
    for name, (port, mode, pool_size, max_clients, role) in _POOLERS.items():
        container = pooler_deployments[f"go-pgbouncer-{name}"]["spec"]["template"][
            "spec"
        ]["containers"][0]
        environment = _env(container)
        assert "@sha256:" in container["image"]
        assert environment["DB_USER"]["value"] == role
        assert environment["POOL_MODE"]["value"] == mode
        assert environment["DEFAULT_POOL_SIZE"]["value"] == str(pool_size)
        assert environment["MAX_CLIENT_CONN"]["value"] == str(max_clients)
        assert container["readinessProbe"]["tcpSocket"]["port"] == "pgbouncer"
        pod_spec = pooler_deployments[f"go-pgbouncer-{name}"]["spec"]["template"][
            "spec"
        ]
        assert pod_spec["securityContext"] == {
            "runAsNonRoot": True,
            "runAsUser": 70,
            "runAsGroup": 70,
            "fsGroup": 70,
            "seccompProfile": {"type": "RuntimeDefault"},
        }
        assert container["securityContext"] == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        }
        assert container["volumeMounts"] == [
            {"name": "generated-config", "mountPath": "/etc/pgbouncer"}
        ]
        assert pod_spec["volumes"] == [{"name": "generated-config", "emptyDir": {}}]

    services = {
        doc["metadata"]["labels"]["app.kubernetes.io/component"]: doc
        for doc in documents
        if doc["kind"] == "Service"
        and doc["metadata"]["labels"]
        .get("app.kubernetes.io/component", "")
        .startswith("go-pgbouncer-")
    }
    for name, (port, *_rest) in _POOLERS.items():
        service_port = services[f"go-pgbouncer-{name}"]["spec"]["ports"][0]
        assert service_port == {
            "name": "pgbouncer",
            "port": port,
            "targetPort": "pgbouncer",
        }

    workers = {
        doc["metadata"]["labels"]["dev-health.io/worker-group"]: doc
        for doc in documents
        if doc["kind"] == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "go-worker"
    }
    assert set(workers) == _PROFILE_NAMES
    for profile, deployment in workers.items():
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        environment = _env(container)
        assert (
            environment["POSTGRES_URI"]["valueFrom"]["secretKeyRef"]["key"]
            == "POSTGRES_URI"
        )
        assert (
            environment["WORKER_DATABASE_URI"]["valueFrom"]["secretKeyRef"]["key"]
            == "WORKER_DATABASE_URI"
        )
        assert environment["WORKER_DATABASE_MODE"]["value"] == "session"
        assert environment["PGBOUNCER_TRANSACTION_MODE"]["value"] == "true"
        assert "MIGRATION_DATABASE_URI" not in environment
        assert "MIGRATION_DATABASE_URI_FILE" not in environment
        if profile in _COORDINATOR_GROUPS:
            assert (
                environment["COORDINATOR_DATABASE_URI"]["valueFrom"]["secretKeyRef"][
                    "key"
                ]
                == "COORDINATOR_DATABASE_URI"
            )
            assert environment["COORDINATOR_DATABASE_MODE"]["value"] == "session"
        else:
            assert "COORDINATOR_DATABASE_URI" not in environment
            assert "COORDINATOR_DATABASE_MODE" not in environment

    migration = next(doc for doc in documents if doc["kind"] == "Job")
    migration_env_from = migration["spec"]["template"]["spec"]["containers"][0][
        "envFrom"
    ]
    assert all(
        source["secretRef"]["name"] != runtime_secret["metadata"]["name"]
        for source in migration_env_from
        if "secretRef" in source
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_go_pgbouncer_network_policy_pins_bundled_and_external_postgres_paths() -> None:
    bundled_documents = _render(
        *_go_values(
            "--set",
            "networkPolicy.enabled=true",
            "--set",
            "postgresql.enabled=true",
        )
    )
    policies = [doc for doc in bundled_documents if doc["kind"] == "NetworkPolicy"]
    pooler_policies = [
        policy
        for policy in policies
        if policy["metadata"]["name"].startswith(f"{_RELEASE}-dev-health-go-pgbouncer-")
    ]
    assert len(pooler_policies) == 3
    for policy in pooler_policies:
        egress_selector = policy["spec"]["egress"][0]["to"][0]["podSelector"][
            "matchLabels"
        ]
        assert egress_selector["app.kubernetes.io/component"] == "postgresql"

    rejected = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            _CHART,
            *_go_values("--set", "networkPolicy.enabled=true"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "networkPolicyCIDR" in rejected.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_river_workers_select_manifest_queues_and_queue_metrics() -> None:
    documents = _render(*_go_values())
    river = _river_groups()
    workers = {
        doc["metadata"]["labels"]["dev-health.io/worker-group"]: doc
        for doc in documents
        if doc["kind"] == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "go-worker"
        and doc["metadata"]["labels"].get("dev-health.io/worker-group") in river
        and "dev-health.io/worker-group" in doc["metadata"]["labels"]
    }
    assert set(workers) == set(river)
    for group, process in river.items():
        deployment = workers[group]
        labels = deployment["metadata"]["labels"]
        assert "dev-health.io/profile" not in labels
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        environment = _env(container)
        assert "DEV_HEALTH_PROFILE" not in environment
        assert environment["DEV_HEALTH_QUEUES"]["value"] == ",".join(process["queues"])
        assert environment["DEV_HEALTH_QUEUE_CONCURRENCY"][
            "value"
        ] == _queue_concurrency(process)
        assert environment["DEV_HEALTH_WORKER_GROUP"]["value"] == group

        scaler = next(
            doc
            for doc in documents
            if doc["kind"] == "HorizontalPodAutoscaler"
            and doc["spec"]["scaleTargetRef"]["name"] == deployment["metadata"]["name"]
        )
        selectors = [
            metric["external"]["metric"]["selector"]["matchLabels"]
            for metric in scaler["spec"]["metrics"]
        ]
        assert all("profile" not in selector for selector in selectors)
        assert {selector["queue"] for selector in selectors} == set(process["queues"])

    for deployment in workers.values():
        group = deployment["metadata"]["labels"]["dev-health.io/worker-group"]
        if group in river:
            continue
        environment = _env(deployment["spec"]["template"]["spec"]["containers"][0])
        assert "DEV_HEALTH_QUEUES" not in environment
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
