"""Rendered Helm contract for the Go PgBouncer topology.

This is intentionally a chart-level oracle: values alone cannot prove which
pool endpoint a long-running binary receives, and a template-only assertion
cannot prove the rendered Secret references stay role-scoped.
"""

from __future__ import annotations

import json
import re
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
    "sync-provider",
    "reconciler",
    "scheduler",
    "stream-external",
    "stream-ingest",
    "stream-pagerduty",
}
_COORDINATOR_GROUPS = {"reconciler", "scheduler"}
# Pool sizes and client caps come from the deployment manifest's postgres
# budget rather than being restated here: the queue and coordinator session
# defaults had drifted below it (22/10 against 23/11) precisely because this
# oracle carried its own copy of the numbers (CHAOS-3872).
_POSTGRES_BUDGET = json.loads(_DEPLOYMENT.read_text(encoding="utf-8"))[
    "postgres_budget"
]
_POOLERS = {
    "transaction": (
        6432,
        "transaction",
        _POSTGRES_BUDGET["pgbouncer_transaction_pool_size"],
        _POSTGRES_BUDGET["pgbouncer_transaction_max_client_connections"],
        "devhealth_domain",
    ),
    "queue-session": (
        6433,
        "session",
        _POSTGRES_BUDGET["pgbouncer_queue_session_pool_size"],
        _POSTGRES_BUDGET["pgbouncer_queue_session_max_client_connections"],
        "devhealth_queue",
    ),
    "coordinator-session": (
        6434,
        "session",
        _POSTGRES_BUDGET["pgbouncer_coordinator_session_pool_size"],
        _POSTGRES_BUDGET["pgbouncer_coordinator_session_max_client_connections"],
        "devhealth_coordinator",
    ),
}
_ROLE_SECRET_KEYS = {
    "RIVER_DOMAIN_DATABASE_PASSWORD",
    "RIVER_QUEUE_DATABASE_PASSWORD",
    "RIVER_COORDINATOR_DATABASE_PASSWORD",
    "POSTGRES_URI",
    "WORKER_DATABASE_URI",
    "COORDINATOR_DATABASE_URI",
    # CHAOS-5594: the chart's default goWorkers.pgbouncer.transaction.
    # extraUsers now carries one entry (devhealth_keda_readonly, KEDA's
    # postgresql-scaler role) -- CHAOS-5616's extraUsers mechanism emits its
    # password under its own passwordKey plus the shared userlist.txt the
    # transaction pooler's AUTH_FILE mounts, so both are now part of the
    # role-scoped Secret's default shape, not an opt-in extra.
    "RIVER_KEDA_READONLY_PASSWORD",
    "userlist.txt",
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
    """CHAOS-4195: goWorkers.enabled and goWorkers.pgbouncer.enabled both
    default to true now (there is no Celery baseline left to default to), so
    plain `--set goWorkers.enabled=true` alone no longer changes anything --
    prove the schema still rejects the pairing by explicitly disabling
    pgbouncer while goWorkers stays enabled."""
    rejected = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            _CHART,
            "--set",
            "goWorkers.enabled=true",
            "--set",
            "goWorkers.pgbouncer.enabled=false",
        ],
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
        expected_mounts: list[dict[str, object]] = [
            {"name": "generated-config", "mountPath": "/etc/pgbouncer"}
        ]
        expected_volumes: list[dict[str, object]] = [
            {"name": "generated-config", "emptyDir": {}}
        ]
        if name == "transaction":
            # CHAOS-5616 + CHAOS-5594: the chart's default
            # goWorkers.pgbouncer.transaction.extraUsers now carries one
            # entry (KEDA's devhealth_keda_readonly), so the TRANSACTION
            # pooler alone projects the rendered userlist.txt as its
            # AUTH_FILE -- queue-session/coordinator-session stay
            # single-user and untouched, asserted below.
            expected_mounts.append(
                {
                    "name": "userlist",
                    "mountPath": "/etc/pgbouncer-userlist",
                    "readOnly": True,
                }
            )
            expected_volumes.append(
                {
                    "name": "userlist",
                    "secret": {
                        "secretName": runtime_secret["metadata"]["name"],
                        "defaultMode": 0o440,
                        "items": [{"key": "userlist.txt", "path": "userlist.txt"}],
                    },
                }
            )
        assert container["volumeMounts"] == expected_mounts
        assert pod_spec["volumes"] == expected_volumes

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
        # CHAOS-4020: the pool semantics are flags, not environment. Only the
        # DSNs themselves stay in env, where a secretKeyRef can project them
        # without exposing them in the pod's command line.
        arguments = container["args"]
        assert "--queue-database-mode=session" in arguments
        assert "--domain-transaction-pooler=true" in arguments
        assert "WORKER_DATABASE_MODE" not in environment
        assert "PGBOUNCER_TRANSACTION_MODE" not in environment
        assert "MIGRATION_DATABASE_URI" not in environment
        assert "MIGRATION_DATABASE_URI_FILE" not in environment
        if profile in _COORDINATOR_GROUPS:
            assert (
                environment["COORDINATOR_DATABASE_URI"]["valueFrom"]["secretKeyRef"][
                    "key"
                ]
                == "COORDINATOR_DATABASE_URI"
            )
            assert "--coordinator-database-mode=session" in arguments
            assert "COORDINATOR_DATABASE_MODE" not in environment
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
        assert "DEV_HEALTH_QUEUES" not in environment
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
        # The queue topology leads the argument list; CHAOS-4020 appends the
        # rest of the worker's configuration after it, so this pins the prefix
        # and the settings it must carry rather than the exact whole list.
        assert container["args"][:4] == [
            f"--queues={','.join(process['queues'])}",
            f"--queue-concurrency={_queue_concurrency(process)}",
            f"--worker-group={group}",
            f"--shutdown-timeout={process['shutdown_grace_seconds']}s",
        ]
        assert "--http-addr=:8080" in container["args"]
        assert not any(
            argument.startswith("--profile=") for argument in container["args"]
        ), f"{group} is a queue worker and must not run a runtime profile"

        # CHAOS-5594: KEDA's ScaledObject replaced the External-metrics
        # HorizontalPodAutoscaler this used to assert on (that HPA required a
        # Prometheus Adapter that was never built and never actually scaled
        # anything). The intent this block still pins -- the autoscaler's
        # signal is scoped to exactly THIS group's own queues, never a
        # profile or another group's queues -- now lives in the postgresql
        # trigger's SQL query rather than a label selector, so there is no
        # analogous "profile not in selector" shape to assert: KEDA's trigger
        # metadata carries no label selector at all.
        scaler = next(
            doc
            for doc in documents
            if doc["kind"] == "ScaledObject"
            and doc["spec"]["scaleTargetRef"]["name"] == deployment["metadata"]["name"]
        )
        triggers = scaler["spec"]["triggers"]
        assert len(triggers) == 1
        trigger = triggers[0]
        assert trigger["type"] == "postgresql"
        query_match = re.search(r"queue IN \(([^)]*)\)", trigger["metadata"]["query"])
        assert query_match is not None, trigger["metadata"]["query"]
        queried_queues = {
            entry.strip().strip("'") for entry in query_match.group(1).split(",")
        }
        assert queried_queues == set(process["queues"])

    for deployment in workers.values():
        group = deployment["metadata"]["labels"]["dev-health.io/worker-group"]
        if group in river:
            continue
        environment = _env(deployment["spec"]["template"]["spec"]["containers"][0])
        assert "DEV_HEALTH_QUEUES" not in environment
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
