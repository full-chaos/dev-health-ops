from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import yaml

from dev_health_ops.workers.config import task_queues


def _parse_queues(command_str: str) -> set[str]:
    """Extract the -Q/--queues list from a celery worker command string."""
    queues: set[str] = set()
    tokens = command_str.split()
    for i, token in enumerate(tokens):
        if token in ("-Q", "--queues") and i + 1 < len(tokens):
            queues.update(q for q in tokens[i + 1].split(",") if q)
        elif token.startswith("--queues="):
            queues.update(q for q in token.split("=", 1)[1].split(",") if q)
        elif token.startswith("-Q") and len(token) > 2:
            queues.update(q for q in token[2:].split(",") if q)
    return queues


def _stringify_command(value: object) -> str:
    return (
        " ".join(str(part) for part in value) if isinstance(value, list) else str(value)
    )


def _container_command_string(service: dict) -> str:
    parts = []
    entrypoint = service.get("entrypoint")
    command = service.get("command")
    if entrypoint:
        parts.append(_stringify_command(entrypoint))
    if command:
        parts.append(_stringify_command(command))
    return " ".join(parts)


def test_compose_workers_cover_every_celery_queue() -> None:
    """CHAOS-2278: the union of -Q lists across all compose celery worker
    services must cover every queue declared in workers.config.task_queues.

    Guards against adding a queue (or a worker topology change) that leaves
    a queue with no consumer — tasks routed there would silently never run.
    The previous topology shipped exactly that bug: `ingest` and `reports`
    existed in task_queues but no compose worker consumed them.
    """
    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    consumed_queues: set[str] = set()
    worker_services: list[str] = []
    for name, service in compose_data["services"].items():
        command = service.get("command")
        if command is None:
            continue
        command_str = _container_command_string(service)
        tokens = command_str.split()
        if "celery" not in tokens or "worker" not in tokens:
            continue
        worker_services.append(name)
        consumed_queues.update(_parse_queues(command_str))

    assert worker_services, "no celery worker services found in compose.yml"

    missing = set(task_queues) - consumed_queues
    assert not missing, (
        f"queues declared in workers.config.task_queues but consumed by no "
        f"compose worker service: {sorted(missing)} "
        f"(workers: {sorted(worker_services)}, consumed: {sorted(consumed_queues)})"
    )


def test_celery_config_has_backfill_queue() -> None:
    assert "backfill" in task_queues


def test_celery_config_has_per_provider_sync_queues() -> None:
    """CHAOS-2299: each known sync provider has a dedicated queue so queue
    depth answers "is <provider> stuck?", and the shared `sync` queue stays
    declared as the fallback for unknown providers and messages already in
    flight at deploy time."""
    for provider in ("github", "gitlab", "linear", "jira", "launchdarkly"):
        assert f"sync.{provider}" in task_queues
    assert "sync" in task_queues


def test_queue_monitor_beat_entry() -> None:
    """CHAOS-2299: queue depth/age telemetry runs every minute on a dedicated
    `monitoring` queue — not `default`, which can flood (telemetry would die
    exactly when it is needed)."""
    from dev_health_ops.workers.config import beat_schedule

    entry = beat_schedule["monitor-queue-depths"]
    assert entry["task"] == "dev_health_ops.workers.tasks.monitor_queue_depths"
    assert entry["schedule"] == 60.0
    assert entry["options"] == {"queue": "monitoring"}


def test_monitoring_queue_declared_and_consumed_redundantly() -> None:
    """The `monitoring` queue must exist in task_queues and be consumed by at
    least two compose worker services so queue telemetry survives one pool
    being saturated or down."""
    assert "monitoring" in task_queues

    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    consumers: list[str] = []
    for name, service in compose_data["services"].items():
        command = service.get("command")
        if command is None:
            continue
        command_str = _container_command_string(service)
        tokens = command_str.split()
        if "celery" not in tokens or "worker" not in tokens:
            continue
        if "monitoring" in _parse_queues(command_str):
            consumers.append(name)

    assert len(consumers) >= 2, (
        f"`monitoring` must be consumed by >=2 worker services for redundancy, "
        f"found: {sorted(consumers)}"
    )


# ---------------------------------------------------------------------------
# CHAOS-2304: production deploy stacks must run migrations as an explicit
# one-shot step — app services never ambient-migrate (AUTO_RUN_MIGRATIONS=false).
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PROD_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml"
_LEGACY_COMPOSE = _REPO_ROOT / "compose.yml"
_SWARM_STACK = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml"
_K8S_DIR = _REPO_ROOT / "deploy" / "kubernetes"
_HELM_DIR = _REPO_ROOT / "deploy" / "helm" / "dev-health"


def _platform_compose_path() -> Path | None:
    for parent in _REPO_ROOT.parents:
        candidate = parent / "compose.yml"
        if not candidate.exists():
            continue
        services = _load_yaml(candidate).get("services") or {}
        api_volumes = services.get("api", {}).get("volumes") or []
        if "api" in services and "worker" in services and "./ops:/app" in api_volumes:
            return candidate
    return None


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _command_string(service: dict) -> str:
    return _container_command_string(service)


def _is_beat_service(name: str, service: dict) -> bool:
    command = _command_string(service).split()
    return name == "beat" or ("celery" in command and "beat" in command)


def _assert_compose_beat_singleton(path: Path) -> None:
    services = _load_yaml(path).get("services") or {}
    for name, service in services.items():
        if not _is_beat_service(name, service):
            continue
        replicas = (service.get("deploy") or {}).get("replicas")
        assert replicas in (None, 1), f"{path.name}:{name} must not exceed 1 replica"


def test_production_compose_has_one_shot_migrate_service() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "compose.production.yml must define a migrate service"
    assert migrate.get("restart") == "no"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    assert "dev-hops migrate postgres" in entrypoint


def test_production_compose_app_services_gate_on_migrate() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker"):
        deps = services[name].get("depends_on") or {}
        assert (
            deps.get("migrate", {}).get("condition") == "service_completed_successfully"
        ), f"{name} must gate on migrate completing successfully"


def test_production_compose_disables_ambient_migrations() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    for name in ("api", "worker"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false", (
            f"{name} must set AUTO_RUN_MIGRATIONS=false — schema is applied by "
            f"the one-shot migrate service"
        )


def test_production_api_healthcheck_uses_ready_probe() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    command = " ".join(str(part) for part in services["api"]["healthcheck"]["test"])

    assert "/ready" in command
    assert "/health" not in command


def test_legacy_compose_has_one_shot_migrate_service() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "compose.yml must define a migrate service"
    assert migrate.get("restart") == "no"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    assert "dev-hops migrate postgres" in entrypoint


def test_legacy_compose_migrate_waits_for_postgres_health() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services["migrate"]

    depends_on = migrate.get("depends_on") or {}
    assert depends_on.get("postgres", {}).get("condition") == "service_healthy"
    assert depends_on.get("clickhouse", {}).get("condition") == "service_healthy"


def test_legacy_compose_migrate_uses_local_build_matching_api() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services["migrate"]
    api = services["api"]

    assert migrate.get("image") is None
    assert isinstance(migrate.get("build"), dict)
    assert migrate["build"] == api["build"]


def test_legacy_compose_disables_ambient_migrations() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker", "worker-ingest", "worker-heavy"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false", (
            f"{name} must set AUTO_RUN_MIGRATIONS=false — schema is applied by "
            f"the one-shot migrate service"
        )


def test_legacy_compose_app_services_gate_on_migrate() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker", "worker-ingest", "worker-heavy"):
        deps = services[name].get("depends_on") or {}
        assert (
            deps.get("migrate", {}).get("condition") == "service_completed_successfully"
        ), f"{name} must gate on migrate completing successfully"


def test_swarm_stack_has_migrate_service_and_disables_ambient_migrations() -> None:
    services = _load_yaml(_SWARM_STACK)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "stack.yml must define a migrate service"
    restart = migrate["deploy"]["restart_policy"]["condition"]
    assert restart == "none", "swarm migrate must be one-shot (restart: none)"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    for name in ("api", "worker"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false"


def test_kubernetes_manifests_run_migrations_as_job() -> None:
    job_docs = [
        d
        for d in yaml.safe_load_all(
            (_K8S_DIR / "migrate-job.yaml").read_text(encoding="utf-8")
        )
        if d
    ]
    jobs = [d for d in job_docs if d.get("kind") == "Job"]
    assert len(jobs) == 1
    pod_spec = jobs[0]["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    command = " ".join(pod_spec["containers"][0]["command"])
    assert "dev-hops migrate clickhouse" in command

    config = _load_yaml(_K8S_DIR / "configmap.yaml")
    assert config["data"]["AUTO_RUN_MIGRATIONS"] == "false"

    kustomization = _load_yaml(_K8S_DIR / "kustomization.yaml")
    assert "migrate-job.yaml" in kustomization["resources"]


def _k8s_docs(filename: str) -> list[dict]:
    return [
        d
        for d in yaml.safe_load_all((_K8S_DIR / filename).read_text(encoding="utf-8"))
        if d
    ]


def test_kubernetes_secret_exposes_clickhouse_uri_for_migrate(monkeypatch) -> None:
    """`dev-hops migrate clickhouse` (the Job) and `status --check` (the
    wait-for-migrations initContainers) resolve CLICKHOUSE_URI via
    resolve_sink_uri — they do NOT read DATABASE_URI. Without CLICKHOUSE_URI
    in the secret the migrate Job fails on first boot."""
    from dev_health_ops.db import resolve_sink_uri

    secret = next(
        d
        for d in _k8s_docs("secrets.yaml")
        if d.get("kind") == "Secret" and d["metadata"]["name"] == "dev-health-secrets"
    )
    uri = secret["stringData"].get("CLICKHOUSE_URI")
    assert uri, "dev-health-secrets must define CLICKHOUSE_URI"
    assert uri.startswith("clickhouse://")

    # The value must be resolvable exactly the way the migrate CLI resolves it.
    monkeypatch.setenv("CLICKHOUSE_URI", uri)
    assert resolve_sink_uri(argparse.Namespace(analytics_db=None)) == uri

    # ...and the Job must actually see the secret (envFrom).
    job = next(d for d in _k8s_docs("migrate-job.yaml") if d.get("kind") == "Job")
    container = job["spec"]["template"]["spec"]["containers"][0]
    secret_refs = {
        ref["secretRef"]["name"]
        for ref in container.get("envFrom", [])
        if "secretRef" in ref
    }
    assert "dev-health-secrets" in secret_refs


@pytest.mark.parametrize("manifest", ["api.yaml", "worker.yaml"])
def test_kubernetes_app_deployments_wait_for_migrations(manifest: str) -> None:
    """CHAOS-2304 safety net: a naive `kubectl apply -k` rolls Deployments
    without waiting for the migrate Job. api/worker must carry a read-only
    wait-for-migrations initContainer that blocks until the schema is
    current (`dev-hops migrate clickhouse status --check`) and never runs
    DDL itself."""
    deployment = next(d for d in _k8s_docs(manifest) if d.get("kind") == "Deployment")
    pod_spec = deployment["spec"]["template"]["spec"]
    waiter = next(
        (
            c
            for c in pod_spec.get("initContainers") or []
            if c["name"] == "wait-for-migrations"
        ),
        None,
    )
    assert waiter is not None, (
        f"{manifest} must define a wait-for-migrations initContainer"
    )

    command = " ".join(waiter["command"])
    assert "dev-hops migrate clickhouse status --check" in command
    # Read-only contract: every dev-hops invocation in the waiter is the
    # status --check probe — it must never run the upgrade (DDL) path.
    assert command.count("dev-hops") == command.count(
        "dev-hops migrate clickhouse status --check"
    )

    secret_refs = {
        ref["secretRef"]["name"]
        for ref in waiter.get("envFrom", [])
        if "secretRef" in ref
    }
    assert "dev-health-secrets" in secret_refs, (
        "waiter needs the secret env (CLICKHOUSE_URI) to resolve the DSN"
    )


def test_helm_chart_runs_migrations_as_pre_upgrade_hook() -> None:
    # Helm templates are Go-templated, so assert on text rather than YAML.
    template = (_HELM_DIR / "templates" / "migrate-job.yaml").read_text(
        encoding="utf-8"
    )
    assert "helm.sh/hook: pre-install,pre-upgrade" in template
    assert "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded" in template
    assert "dev-hops migrate clickhouse" in template

    helpers = (_HELM_DIR / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    assert "AUTO_RUN_MIGRATIONS" in helpers

    values = _load_yaml(_HELM_DIR / "values.yaml")
    assert values["migrations"]["hook"]["enabled"] is True


def test_deploy_stacks_keep_celery_beat_singleton() -> None:
    for stack in (_REPO_ROOT / "compose.yml", _PROD_COMPOSE, _SWARM_STACK):
        _assert_compose_beat_singleton(stack)

    beat_template = (_HELM_DIR / "templates" / "beat-deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "SINGLETON: exactly 1 beat replica" in beat_template
    assert "replicas: 1" in beat_template


def test_celery_worker_prefetch_multiplier_is_one() -> None:
    """CHAOS-2277: long-running tasks (sync, stream consumers) + default
    prefetch (4) let reserved slow-queue messages fill the QoS window and
    block fetching from other queues entirely — Sync Now appeared stuck
    until a worker restart released the unacked reservations. One-at-a-time
    fetching keeps cross-queue round-robin fair."""
    from dev_health_ops.workers.config import worker_prefetch_multiplier

    assert worker_prefetch_multiplier == 1


def test_celery_worker_prefetch_is_disabled_for_redis() -> None:
    from dev_health_ops.workers.config import worker_disable_prefetch

    assert worker_disable_prefetch is True


def test_stream_consumer_beat_ticks_do_not_outlive_cadence() -> None:
    from dev_health_ops.api.ingest.consumer import BLOCK_MS as INGEST_BLOCK_MS
    from dev_health_ops.api.product_telemetry.consumer import (
        BLOCK_MS as PRODUCT_TELEMETRY_BLOCK_MS,
    )
    from dev_health_ops.workers.config import beat_schedule

    cases = {
        "process-ingest-streams": INGEST_BLOCK_MS,
        "process-product-telemetry-streams": PRODUCT_TELEMETRY_BLOCK_MS,
    }

    for entry_name, block_ms in cases.items():
        entry = beat_schedule[entry_name]
        schedule_seconds = float(entry["schedule"])
        max_iterations = int(entry["kwargs"]["max_iterations"])

        assert (max_iterations * block_ms) / 1000 < schedule_seconds
        assert entry["options"] == {"queue": "ingest", "expires": 30}


def test_worker_commands_disable_prefetch_for_redis() -> None:
    for path in (_REPO_ROOT / "compose.yml", _PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        worker_commands = [
            _command_string(service).split()
            for service in services.values()
            if "worker" in _command_string(service).split()
        ]

        assert worker_commands
        for command in worker_commands:
            assert "--disable-prefetch" in command

    k8s_commands: list[list[str]] = []
    for doc in yaml.safe_load_all((_K8S_DIR / "worker.yaml").read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        for container in doc["spec"]["template"]["spec"].get("containers") or []:
            command = container.get("command") or []
            if "worker" in command:
                k8s_commands.append(command)

    assert k8s_commands
    for command in k8s_commands:
        assert "--disable-prefetch" in command

    helm_templates = [
        _HELM_DIR / "templates" / "worker-deployment.yaml",
        _HELM_DIR / "templates" / "worker-pools.yaml",
    ]
    for template in helm_templates:
        text = template.read_text(encoding="utf-8")
        assert "--disable-prefetch" in text


def test_local_compose_workers_import_mounted_source() -> None:
    services = _load_yaml(_REPO_ROOT / "compose.yml").get("services") or {}

    for service_name in ("worker", "worker-ingest", "worker-heavy"):
        service = services[service_name]
        command = _command_string(service).split()
        assert "worker" in command
        assert service["environment"]["PYTHONPATH"] == "/app/src"
        assert "./:/app" in service["volumes"]


def test_platform_compose_workers_and_beat_import_mounted_source() -> None:
    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}

    service_names = ["worker", "worker-ingest", "worker-heavy", "beat"]
    if "worker-wi" in services:
        service_names.append("worker-wi")

    for service_name in service_names:
        service = services[service_name]
        assert service["environment"]["PYTHONPATH"] == "/app/src"
        assert "./ops:/app" in service["volumes"]


def test_platform_compose_provider_worker_consumes_sync_dispatch_queue() -> None:
    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    provider_worker = services.get("worker-wi")
    if provider_worker is None:
        pytest.skip("platform compose.yml has no split provider worker")

    queues = _parse_queues(_container_command_string(provider_worker))
    assert "sync" in queues


def test_compose_workers_override_runner_entrypoint() -> None:
    for path in (_LEGACY_COMPOSE, _PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        for service_name in ("worker", "worker-ingest", "worker-heavy"):
            service = services[service_name]
            assert service["entrypoint"] == ["celery"]
            command = _stringify_command(service["command"])
            assert command.split()[0] != "celery"
            assert "dev_health_ops.workers.celery_app" in command


def test_production_workers_use_semantic_postgres_uri() -> None:
    for path in (_PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        for service_name in ("api", "worker", "worker-ingest", "worker-heavy"):
            environment = services[service_name]["environment"]
            assert environment["POSTGRES_URI"].startswith("postgresql+asyncpg://")
            assert environment["DATABASE_URI"].startswith("postgresql+asyncpg://")
            assert environment["CLICKHOUSE_URI"].startswith("clickhouse://")


def test_production_stacks_consume_monitoring_queue() -> None:
    """The monitor-queue-depths beat entry enqueues to `monitoring`
    unconditionally — every production stack's worker must consume it or
    telemetry tasks accumulate unconsumed forever (1,440/day)."""
    import re

    stacks = [
        _PROD_COMPOSE,
        _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml",
        _REPO_ROOT / "deploy" / "kubernetes" / "worker.yaml",
        _REPO_ROOT / "deploy" / "helm" / "dev-health" / "values.yaml",
    ]
    for stack in stacks:
        text = stack.read_text(encoding="utf-8")
        queue_lists = re.findall(r"(?:- |queues: \")(default,[a-z.,]+)", text)
        assert any("monitoring" in q for q in queue_lists), (
            f"{stack.name}: no worker queue list includes 'monitoring'"
        )


def _compose_worker_queues(path: Path) -> set[str]:
    """Union of -Q lists across every celery worker service in a compose file."""
    data = _load_yaml(path)
    consumed: set[str] = set()
    for _name, service in (data.get("services") or {}).items():
        cmd = _command_string(service)
        toks = cmd.split()
        if "celery" not in toks or "worker" not in toks:
            continue
        consumed |= _parse_queues(cmd)
    return consumed


def _k8s_worker_queues(path: Path) -> set[str]:
    """Union of -Q lists across every worker Deployment in a k8s manifest."""
    consumed: set[str] = set()
    for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if not doc or doc.get("kind") != "Deployment":
            continue
        pod = doc["spec"]["template"]["spec"]
        for container in pod.get("containers") or []:
            cmd = container.get("command") or []
            for i, tok in enumerate(cmd):
                if tok == "-Q" and i + 1 < len(cmd):
                    consumed |= {q for q in str(cmd[i + 1]).split(",") if q}
    return consumed


def _helm_worker_queues(values_path: Path) -> set[str]:
    """Union of queue lists across every enabled worker pool in helm values."""
    values = _load_yaml(values_path)
    consumed: set[str] = set()
    for pool in ("worker", "workerIngest", "workerHeavy"):
        cfg = values.get(pool) or {}
        if cfg.get("enabled") is False:
            continue
        queues = cfg.get("queues")
        if queues:
            consumed |= {q for q in str(queues).split(",") if q}
    return consumed


def test_production_stacks_cover_every_celery_queue() -> None:
    """CHAOS-2308: every production deploy stack must consume every queue in
    workers.config.task_queues across the union of its worker pools. A queue
    declared in task_queues but consumed by no prod worker silently accumulates
    forever (backfill jobs, webhook events, ingest, reports, cost-class sync).
    Mirrors test_compose_workers_cover_every_celery_queue for the prod stacks."""
    all_queues = set(task_queues)
    coverage = {
        "compose.production.yml": _compose_worker_queues(_PROD_COMPOSE),
        "docker-swarm/stack.yml": _compose_worker_queues(_SWARM_STACK),
        "kubernetes/worker.yaml": _k8s_worker_queues(_K8S_DIR / "worker.yaml"),
        "helm values.yaml": _helm_worker_queues(_HELM_DIR / "values.yaml"),
    }
    for name, consumed in coverage.items():
        missing = all_queues - consumed
        assert not missing, (
            f"{name}: production worker pools miss queues {sorted(missing)} "
            f"declared in workers.config.task_queues (consumed: {sorted(consumed)})"
        )
