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
        command_str = (
            " ".join(str(part) for part in command)
            if isinstance(command, list)
            else str(command)
        )
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
        command_str = (
            " ".join(str(part) for part in command)
            if isinstance(command, list)
            else str(command)
        )
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


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _command_string(service: dict) -> str:
    command = service.get("command") or service.get("entrypoint") or ""
    return (
        " ".join(str(part) for part in command)
        if isinstance(command, list)
        else str(command)
    )


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
