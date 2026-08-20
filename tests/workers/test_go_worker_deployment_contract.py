from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import pytest
import tomllib
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _REPO_ROOT / "pyproject.toml"
_DEPLOYMENT = _REPO_ROOT / "deploy" / "go-workers" / "deployment.json"
_APP_DOCKERFILE = _REPO_ROOT / "docker" / "Dockerfile"
_GO_WORKER_DOCKERFILE = _REPO_ROOT / "docker" / "go-worker.Dockerfile"
_PRODUCTION_COMPOSE = (
    _REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml"
)
_SWARM_STACK = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml"
_KUBERNETES = _REPO_ROOT / "deploy" / "kubernetes"
_HELM_CHART = _REPO_ROOT / "deploy" / "helm" / "dev-health"
_GO_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose" / "compose.go-workers.yml"
_GO_COMPOSE_ONLY = (
    _REPO_ROOT / "deploy" / "docker-compose" / "compose.go-workers-only.yml"
)
_GO_SWARM = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.go-workers.yml"
_GO_SWARM_ONLY = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.go-workers-only.yml"
_GO_KUBERNETES = _KUBERNETES / "go-workers.yaml"
_GO_KUBERNETES_ONLY = _KUBERNETES / "go-workers-only.yaml"

_PACKAGED_WORK_ITEM_CONFIG = {
    "status_mapping.yaml": "/app/config/status_mapping.yaml",
    "investment_areas.yaml": "/app/config/investment_areas.yaml",
}

_MIGRATION_CONFIG_DEFAULTS = {
    "RIVER_DATABASE_SCHEMA": "river",
    "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
    "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
}
_FORBIDDEN_SHARED_MIGRATION_SECRETS = {
    "MIGRATION_DATABASE_URI",
    "MIGRATION_DATABASE_URI_FILE",
}

_KUBERNETES_CONFIGMAP = _KUBERNETES / "configmap.yaml"
_KUBERNETES_SECRETS = _KUBERNETES / "secrets.yaml"
_KUBERNETES_API = _KUBERNETES / "api.yaml"

# CHAOS-3076: the PagerDuty stream runner forwards reconciliation to the Python
# worker bridge, so a renderer that declares the process without this wiring
# produces a service that can never construct its handler (missing endpoint or
# insecure opt-in) or is rejected with 401 (missing token).
_PAGERDUTY_PROCESS = "stream-pagerduty"
_PAGERDUTY_RUNTIME_PROFILE = "pagerduty"
# Non-secret half of the contract. It cannot be driven from deployment.json:
# `processes[]` entries carry only `secret_env`, and internal/deploymentcontract
# decodes the manifest with DisallowUnknownFields, so a `config_env` key there
# would fail the Go contract check until the Go schema grows the field.
_PAGERDUTY_CONFIG_ENV = {
    "PAGERDUTY_WEBHOOK_TRANSPORT",
    "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE",
    "WORKER_OPERATIONAL_BRIDGE_URL",
}
# The API serves the bridge endpoint and owns the Celery dispatch decision, so
# it needs the caller's token and the same transport selector.
_API_BRIDGE_ENV = {
    "PAGERDUTY_WEBHOOK_TRANSPORT",
    "WORKER_OPERATIONAL_BRIDGE_TOKEN",
}
_DEFAULT_WEBHOOK_TRANSPORT = "celery"
# strconv.ParseBool's truthy spellings, lowercased.
_TRUTHY = {"1", "t", "true"}
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}

_RIVER_WORKER_SERVICES = {
    "heavy": "go-worker-heavy",
    "ops": "go-worker-ops",
    "sync": "go-worker-sync",
    # Its own process, like the Celery fleet splits worker-ingest and
    # worker-backfill out by queue. Fused with sync it cannot start while
    # provider routes are default-off (CHAOS-3926).
    "sync-provider": "go-worker-sync-provider",
}

# CHAOS-3942: the /health/workers fleet contract. River/queue-consumer
# groups only -- reconciler/scheduler/stream-* run a separate role with
# their own /healthz and never register worker_instances presence.
_EXPECTED_WORKER_GROUPS_VALUE = "heavy,ops,sync,sync-provider"


def _river_processes() -> dict[str, dict]:
    return {
        process["name"]: process
        for process in _load_json(_DEPLOYMENT)["processes"]
        if process["runtime"] == "river" and process["binary"] == "dev-health-worker"
    }


def _queue_env(value: object) -> list[str]:
    return [queue for queue in str(value).split(",") if queue]


def _queue_concurrency_env(process: dict) -> str:
    return ",".join(
        f"{entry['queue']}={entry['max_workers']}" for entry in process["queue_workers"]
    )


def _process_arguments(container: dict) -> dict[str, str]:
    raw = container.get("command") or container.get("args") or []
    assert isinstance(raw, list), "worker process arguments must use list form"
    arguments: dict[str, str] = {}
    for item in raw:
        name, separator, value = str(item).partition("=")
        assert separator and name.startswith("--"), f"invalid process argument: {item}"
        assert name not in arguments, f"duplicate process argument: {name}"
        arguments[name] = value
    return arguments


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_yaml_documents(path: Path) -> list[dict]:
    return [
        document
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8"))
        if document
    ]


def _command_string(container: dict) -> str:
    parts = [container.get("entrypoint"), container.get("command")]
    return " ".join(
        " ".join(str(part) for part in value) if isinstance(value, list) else str(value)
        for value in parts
        if value
    )


def _compose_default(value: object, variable: str) -> int:
    match = re.fullmatch(rf"\$\{{{re.escape(variable)}:-(\d+)\}}", str(value))
    assert match is not None, f"{variable} must keep an explicit numeric default"
    return int(match.group(1))


def _assert_migration_command(command: str) -> None:
    assert "dev-hops migrate postgres" in command
    assert "dev-hops migrate clickhouse" in command
    assert "MIGRATION_DATABASE_URI+x" in command
    assert "MIGRATION_DATABASE_URI_FILE+x" in command
    assert "POSTGRES_URI" in command


def _bridge_secret_env() -> set[str]:
    """Bridge secrets the checked-in manifest declares for the process."""
    process = next(
        item
        for item in _load_json(_DEPLOYMENT)["processes"]
        if item["name"] == _PAGERDUTY_PROCESS
    )
    declared = {
        name
        for name in process["secret_env"]
        if name.startswith("WORKER_OPERATIONAL_BRIDGE")
    }
    assert declared, (
        f"{_PAGERDUTY_PROCESS} must declare its bridge credential in "
        "deploy/go-workers/deployment.json"
    )
    return declared


def _pagerduty_required_env() -> set[str]:
    return _bridge_secret_env() | _PAGERDUTY_CONFIG_ENV


def _compose_variable_default(value: object, variable: str) -> str:
    match = re.fullmatch(rf"\$\{{{re.escape(variable)}:-(.*)\}}", str(value), re.DOTALL)
    assert match is not None, f"{variable} must stay overridable with a default"
    return match.group(1)


def _compose_pagerduty_services(path: Path) -> dict[str, dict]:
    return {
        name: service
        for name, service in (_load_yaml(path).get("services") or {}).items()
        if (service.get("environment") or {}).get("DEV_HEALTH_PROFILE")
        == _PAGERDUTY_RUNTIME_PROFILE
    }


def _kubernetes_env_sources() -> dict[tuple[str, str], set[str]]:
    sources: dict[tuple[str, str], set[str]] = {}
    for path in (_KUBERNETES_CONFIGMAP, _KUBERNETES_SECRETS):
        for document in _load_yaml_documents(path):
            if document["kind"] == "ConfigMap":
                sources[("configMapRef", document["metadata"]["name"])] = set(
                    document.get("data") or {}
                )
            elif document["kind"] == "Secret":
                sources[("secretRef", document["metadata"]["name"])] = set(
                    document.get("stringData") or {}
                )
    return sources


def _kubernetes_container_env(container: dict) -> set[str]:
    """Every env name the container resolves, inline plus envFrom references."""
    sources = _kubernetes_env_sources()
    names = {item["name"] for item in container.get("env") or []}
    for source in container.get("envFrom") or []:
        for kind in ("configMapRef", "secretRef"):
            reference = source.get(kind)
            if reference is None:
                continue
            assert (kind, reference["name"]) in sources, (
                f"unknown {kind} {reference['name']}"
            )
            names |= sources[(kind, reference["name"])]
    return names


def _kubernetes_container_profile(container: dict) -> str | None:
    """The DEV_HEALTH_PROFILE this container actually runs with.

    Inline env wins; otherwise the value is inherited from a referenced
    ConfigMap. A label is metadata and does not reach the process, so it can
    never stand in for this.
    """
    for item in container.get("env") or []:
        if item["name"] == "DEV_HEALTH_PROFILE":
            return item.get("value")
    config = _load_yaml(_KUBERNETES_CONFIGMAP).get("data") or {}
    for source in container.get("envFrom") or []:
        reference = source.get("configMapRef")
        if reference and "DEV_HEALTH_PROFILE" in config:
            return config["DEV_HEALTH_PROFILE"]
    return None


def _kubernetes_pagerduty_containers(path: Path) -> dict[str, dict]:
    """Deployments that run the PagerDuty profile, by effective env not label.

    Discovering by `dev-health.io/worker-group` alone proved nothing: the label is
    metadata, so a Deployment labelled pagerduty whose container ran a different
    DEV_HEALTH_PROFILE passed every assertion here while, at cutover, running
    the wrong profile and leaving PagerDuty entries unconsumed after Celery
    stood down. Discovery follows the env; label agreement is asserted
    separately so a mismatch in either direction fails.
    """
    containers = {}
    for document in _load_yaml_documents(path):
        if document.get("kind") != "Deployment":
            continue
        pod = ((document.get("spec") or {}).get("template") or {}).get("spec") or {}
        pod_containers = pod.get("containers") or []
        if not pod_containers:
            continue
        container = pod_containers[0]
        labelled = (document["metadata"].get("labels") or {}).get(
            "dev-health.io/worker-group"
        ) == _PAGERDUTY_PROCESS
        runs_profile = (
            _kubernetes_container_profile(container) == _PAGERDUTY_RUNTIME_PROFILE
        )
        name = document["metadata"]["name"]
        assert labelled == runs_profile, (
            f"{name}: label says pagerduty={labelled} but the container runs "
            f"DEV_HEALTH_PROFILE={_kubernetes_container_profile(container)!r}"
        )
        if runs_profile:
            containers[name] = container
    return containers


def test_go_worker_groups_are_disabled_future_topology() -> None:
    manifest = _load_json(_DEPLOYMENT)

    assert manifest["deployment_state"] == "coexistence_disabled"
    assert manifest["runtime_role_env"] == [
        "RIVER_COORDINATOR_DATABASE_ROLE",
        "RIVER_DOMAIN_DATABASE_ROLE",
        "RIVER_QUEUE_DATABASE_ROLE",
    ]
    assert all(
        not process["enabled_by_default"] and process["min_replicas"] == 0
        for process in manifest["processes"]
    )
    for process in manifest["processes"]:
        assert [item["queue"] for item in process["queue_workers"]] == process["queues"]
        assert all(item["max_workers"] > 0 for item in process["queue_workers"])
    assert all(
        "POSTGRES_URI" in process["secret_env"]
        and "MIGRATION_DATABASE_URI" not in process["secret_env"]
        for process in manifest["processes"]
    )
    assert {
        process["name"]: process["max_replicas"]
        for process in manifest["processes"]
        if process["name"] in {"heavy", "ops"}
    } == {"heavy": 2, "ops": 2}
    operator = manifest["operator_cli"]
    assert operator == {
        "name": "worker-operator",
        "binary": "dev-health-workerctl",
        "max_concurrent_invocations": 1,
        "queue_control_max_connections": 2,
        "domain_max_connections": 2,
        # CHAOS-3033: workerctl is a coordinator binary -- it authenticates the
        # operator token against coordinator-exclusive
        # internal_service_credentials before any command dispatches -- so it
        # carries a coordinator budget and requires the coordinator DSN.
        "coordinator_max_connections": 2,
        "config_env": [
            "COORDINATOR_DATABASE_MODE",
            "PGBOUNCER_TRANSACTION_MODE",
            "RIVER_COORDINATOR_DATABASE_ROLE",
            "RIVER_DATABASE_SCHEMA",
            "RIVER_DOMAIN_DATABASE_ROLE",
            "RIVER_QUEUE_DATABASE_ROLE",
            "WORKER_DATABASE_MODE",
        ],
        "secret_env": [
            "COORDINATOR_DATABASE_URI",
            "POSTGRES_URI",
            "WORKER_DATABASE_URI",
            "WORKER_OPERATOR_TOKEN",
        ],
    }


def test_go_worker_image_packages_work_item_semantic_config() -> None:
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    for filename, runtime_path in _PACKAGED_WORK_ITEM_CONFIG.items():
        source = f"src/dev_health_ops/config/{filename}"
        staged = f"/runtime/worker{runtime_path}"
        assert (_REPO_ROOT / source).is_file()
        assert f"COPY {source} ./src/dev_health_ops/config/{filename}" in dockerfile
        assert staged in dockerfile


def test_go_worker_image_packages_lifecycle_route_operator() -> None:
    """The worker image must contain Compose's inherited stop-hook executable."""
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp /out/dev-health-workerctl "
        "/runtime/worker/usr/local/bin/dev-health-workerctl;"
    ) in dockerfile


def test_go_deployment_surfaces_are_additive_default_off_and_group_complete() -> None:
    """CHAOS-3052: every supported deploy surface renders an inert, hardened
    topology. It must never change the default Celery/Beat/Valkey deployment
    merely by being present in the repository.
    """
    expected_profiles = {
        process["name"] for process in _load_json(_DEPLOYMENT)["processes"]
    }
    assert expected_profiles == {
        "heavy",
        "ops",
        "reconciler",
        "scheduler",
        "stream-external",
        "stream-ingest",
        "stream-pagerduty",
        "sync",
        "sync-provider",
    }

    compose = _load_yaml(_GO_COMPOSE)["services"]
    runtime_services = {
        "go-worker-heavy",
        "go-worker-ops",
        "go-worker-sync",
        "go-worker-sync-provider",
        "go-reconciler",
        "go-scheduler",
        "go-stream-external",
        "go-stream-ingest",
        "go-stream-pagerduty",
    }
    assert set(compose) == runtime_services | {
        "go-river-provision",
        "go-river-migrate",
        "go-contractcheck",
        # CHAOS-3942: merges EXPECTED_WORKER_GROUPS into the base `api`
        # service so /health/workers flips authority when this overlay is
        # applied; asserted in detail by
        # test_go_worker_health_check_flips_authority_with_the_overlay.
        "api",
    }
    for name in runtime_services:
        service = compose[name]
        assert service["profiles"] == ["go-workers"]
        assert service["read_only"] is True
        assert service["user"] == "65532:65532"
        assert "no-new-privileges:true" in service["security_opt"]
        assert service["environment"]["AUTO_RUN_MIGRATIONS"] == "false"
        assert (
            _compose_variable_default(
                service["environment"]["PGBOUNCER_TRANSACTION_MODE"],
                "PGBOUNCER_TRANSACTION_MODE",
            )
            == "true"
        )
    assert _process_arguments(compose["go-worker-sync"])["--queues"] == "sync"
    assert _process_arguments(compose["go-worker-sync-provider"])["--queues"] == (
        "sync_provider"
    )

    swarm = _load_yaml(_GO_SWARM)["services"]
    # CHAOS-3942: same `api` merge as Compose, see the comment above.
    assert set(swarm) == runtime_services | {"api"}
    for name in runtime_services:
        service = swarm[name]
        assert service["read_only"] is True
        assert service["user"] == "65532:65532"
        assert service["environment"]["AUTO_RUN_MIGRATIONS"] == "false"
        assert (
            _compose_variable_default(
                service["environment"]["PGBOUNCER_TRANSACTION_MODE"],
                "PGBOUNCER_TRANSACTION_MODE",
            )
            == "true"
        )
        assert service["deploy"]["replicas"] == 0
        assert service["deploy"]["update_config"]["order"] == "start-first"

    deployments = {
        document["metadata"]["name"]: document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document["kind"] == "Deployment"
    }
    assert len(deployments) == len(expected_profiles)
    for deployment in deployments.values():
        assert deployment["spec"]["replicas"] == 0
        pod_security = deployment["spec"]["template"]["spec"]["securityContext"]
        assert pod_security["runAsNonRoot"] is True
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["resources"]["requests"]["cpu"]
        assert container["resources"]["limits"]["memory"]
    sync_labels = deployments["dev-health-go-worker-sync"]["metadata"]["labels"]
    assert sync_labels["dev-health.io/worker-group"] == "sync"
    assert "dev-health.io/profile" not in sync_labels
    provider_labels = deployments["dev-health-go-worker-sync-provider"]["metadata"][
        "labels"
    ]
    assert provider_labels["dev-health.io/worker-group"] == "sync-provider"
    assert "dev-health.io/profile" not in provider_labels

    values = _load_yaml(_HELM_CHART / "values.yaml")
    assert values["goWorkers"]["enabled"] is False
    assert "profiles" not in values["goWorkers"]
    assert "groups" in values["goWorkers"]
    assert {
        group["name"] for group in values["goWorkers"]["groups"]
    } == expected_profiles
    sync_profile = next(
        group for group in values["goWorkers"]["groups"] if group["name"] == "sync"
    )
    assert sync_profile["queues"] == ["sync"]
    helm_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert ".Values.goWorkers.profiles" not in helm_template
    assert "$profile" not in helm_template
    assert "worker_jobs_available" in helm_template
    assert "worker_job_oldest_age_seconds" in helm_template
    assert "worker_execution_saturation_ratio" in helm_template


def test_river_worker_renderers_select_manifest_queues_without_profiles() -> None:
    """CHAOS-3851: River workers are queue-selected, not profile-selected.

    The queue sets are read from the deployment manifest instead of being
    duplicated in the oracle. This keeps each renderer tied to the same
    executable process contract while allowing overlapping worker groups.
    """
    river = _river_processes()
    assert set(river) == set(_RIVER_WORKER_SERVICES)

    compose = _load_yaml(_GO_COMPOSE)["services"]
    swarm = _load_yaml(_GO_SWARM)["services"]
    for group, service_name in _RIVER_WORKER_SERVICES.items():
        expected = river[group]["queues"]
        expected_concurrency = _queue_concurrency_env(river[group])
        for renderer_name, services in (("Compose", compose), ("Swarm", swarm)):
            assert (
                services[service_name]["labels"]["dev-health.io/worker-group"] == group
            )
            environment = services[service_name]["environment"]
            assert "DEV_HEALTH_PROFILE" not in environment, (
                f"{renderer_name} {service_name} must select queues explicitly"
            )
            assert "DEV_HEALTH_QUEUES" not in environment
            assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
            assert "DEV_HEALTH_WORKER_GROUP" not in environment
            arguments = _process_arguments(services[service_name])
            assert _queue_env(arguments["--queues"]) == expected
            assert arguments["--queue-concurrency"] == expected_concurrency
            assert arguments["--worker-group"] == group

    deployments = {
        document["metadata"]["name"]: document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "Deployment"
    }
    for group, service_name in _RIVER_WORKER_SERVICES.items():
        deployment = deployments[f"dev-health-{service_name}"]
        labels = deployment["metadata"]["labels"]
        assert labels["dev-health.io/worker-group"] == group
        assert "dev-health.io/profile" not in labels
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        environment = {
            item["name"]: item.get("value") for item in container.get("env", [])
        }
        assert "DEV_HEALTH_PROFILE" not in environment
        assert "DEV_HEALTH_QUEUES" not in environment
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
        arguments = _process_arguments(container)
        assert _queue_env(arguments["--queues"]) == river[group]["queues"]
        assert arguments["--queue-concurrency"] == _queue_concurrency_env(river[group])
        assert arguments["--worker-group"] == group

    horizontal_scalers = [
        document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "HorizontalPodAutoscaler"
    ]
    for scaler in horizontal_scalers:
        target = scaler["spec"]["scaleTargetRef"]["name"]
        group = target.removeprefix("dev-health-go-worker-")
        # No aliasing: sync and sync-provider are distinct manifest processes,
        # each autoscaling on its own queue's backlog (CHAOS-3926).
        if group not in river:
            continue
        selectors = [
            metric["external"]["metric"]["selector"]["matchLabels"]
            for metric in scaler["spec"]["metrics"]
        ]
        assert all("profile" not in selector for selector in selectors)
        assert {selector["queue"] for selector in selectors} == set(
            river[group]["queues"]
        )

    stream_runtime_profiles = {
        "stream-external": ("go-stream-external", "external"),
        "stream-ingest": ("go-stream-ingest", "ingest"),
        "stream-pagerduty": ("go-stream-pagerduty", "pagerduty"),
    }
    for group, (service_name, runtime_profile) in stream_runtime_profiles.items():
        for services in (compose, swarm):
            environment = services[service_name]["environment"]
            assert environment["DEV_HEALTH_PROFILE"] == runtime_profile
            assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
            assert "DEV_HEALTH_WORKER_GROUP" not in environment
        deployment = deployments[f"dev-health-{service_name}"]
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert _kubernetes_container_profile(container) == runtime_profile
        values = _load_yaml(_HELM_CHART / "values.yaml")
        helm_group = next(
            group_values
            for group_values in values["goWorkers"]["groups"]
            if group_values["name"] == group
        )
        assert helm_group["runtimeProfile"] == runtime_profile

    for service_name in ("go-reconciler", "go-scheduler"):
        environment = compose[service_name]["environment"]
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
    for service_name in (
        "go-stream-external",
        "go-stream-ingest",
        "go-stream-pagerduty",
        "go-reconciler",
        "go-scheduler",
    ):
        environment = swarm[service_name]["environment"]
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment
        deployment = deployments[f"dev-health-{service_name}"]
        environment = {
            item["name"]: item.get("value")
            for item in deployment["spec"]["template"]["spec"]["containers"][0].get(
                "env", []
            )
        }
        assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
        assert "DEV_HEALTH_WORKER_GROUP" not in environment


def test_group_replica_and_drain_contract_matches_every_renderer() -> None:
    manifest = {
        process["name"]: process for process in _load_json(_DEPLOYMENT)["processes"]
    }
    services = {
        "heavy": "go-worker-heavy",
        "ops": "go-worker-ops",
        "reconciler": "go-reconciler",
        "scheduler": "go-scheduler",
        "stream-external": "go-stream-external",
        "stream-ingest": "go-stream-ingest",
        "stream-pagerduty": "go-stream-pagerduty",
        "sync": "go-worker-sync",
        "sync-provider": "go-worker-sync-provider",
    }
    compose = _load_yaml(_GO_COMPOSE)["services"]
    swarm = _load_yaml(_GO_SWARM)["services"]
    kubernetes = {
        document["metadata"]["labels"].get("dev-health.io/worker-group"): document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "Deployment"
    }
    horizontal_scalers = {
        document["spec"]["scaleTargetRef"]["name"]: document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "HorizontalPodAutoscaler"
    }
    helm = {
        group["name"]: group
        for group in _load_yaml(_HELM_CHART / "values.yaml")["goWorkers"]["groups"]
    }
    for profile, service_name in services.items():
        contract = manifest[profile]
        desired = contract["desired_replicas"]
        grace = contract["shutdown_grace_seconds"]
        assert compose[service_name]["deploy"]["replicas"] == desired
        assert compose[service_name]["stop_grace_period"] == f"{grace}s"
        assert swarm[service_name]["deploy"]["replicas"] == desired
        assert swarm[service_name]["stop_grace_period"] == f"{grace}s"
        assert kubernetes[profile]["spec"]["replicas"] == desired
        pod_spec = kubernetes[profile]["spec"]["template"]["spec"]
        assert pod_spec["terminationGracePeriodSeconds"] == grace
        if contract["runtime"] == "river":
            assert (
                _process_arguments(compose[service_name])["--shutdown-timeout"]
                == f"{grace}s"
            )
            assert (
                _process_arguments(swarm[service_name])["--shutdown-timeout"]
                == f"{grace}s"
            )
            assert (
                _process_arguments(pod_spec["containers"][0])["--shutdown-timeout"]
                == f"{grace}s"
            )
        else:
            assert (
                compose[service_name]["environment"]["DEV_HEALTH_SHUTDOWN_TIMEOUT"]
                == f"{grace}s"
            )
            assert (
                swarm[service_name]["environment"]["DEV_HEALTH_SHUTDOWN_TIMEOUT"]
                == f"{grace}s"
            )
            shutdown = next(
                item
                for item in pod_spec["containers"][0]["env"]
                if item["name"] == "DEV_HEALTH_SHUTDOWN_TIMEOUT"
            )
            assert shutdown["value"] == f"{grace}s"
        assert helm[profile]["replicas"] == desired
        assert helm[profile]["terminationGracePeriodSeconds"] == grace
        if contract["runtime"] == "river":
            target = kubernetes[profile]["metadata"]["name"]
            assert (
                horizontal_scalers[target]["spec"]["minReplicas"]
                == contract["min_replicas"]
            )
            assert (
                horizontal_scalers[target]["spec"]["maxReplicas"]
                == contract["max_replicas"]
            )
            assert (
                helm[profile]["autoscaling"]["maxReplicas"] == contract["max_replicas"]
            )
    helm_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert "--shutdown-timeout=" in helm_template


def test_go_compose_bootstrap_is_post_alembic_fail_closed_and_route_inert() -> None:
    services = _load_yaml(_GO_COMPOSE)["services"]

    provision = services["go-river-provision"]
    assert provision["profiles"] == ["go-workers"]
    assert provision["restart"] == "no"
    assert (
        provision["depends_on"]["migrate"]["condition"]
        == "service_completed_successfully"
    )
    provision_command = _command_string(provision)
    assert "psql" in provision_command
    assert "provision_river_roles.sql" in provision_command
    assert "--set=coordinator_role" in provision_command
    assert "--set=coordinator_password" in provision_command
    assert "RIVER_COORDINATOR_DATABASE_PASSWORD" in provision["environment"]
    # The SQL travels in the image, not from the host. A relative bind-mount
    # source is unproduceable on a pull-only host, and Docker answers a missing
    # one by creating an empty DIRECTORY rather than failing -- which is how
    # this reached production as `psql: ... Is a directory` (CHAOS-3925).
    assert not [
        volume
        for volume in (provision.get("volumes") or [])
        if "provision_river_roles.sql" in str(volume)
    ]
    assert "/usr/local/share/dev-health/provision_river_roles.sql" in provision_command

    river_migrate = services["go-river-migrate"]
    assert river_migrate["profiles"] == ["go-workers"]
    assert river_migrate["restart"] == "no"
    assert (
        river_migrate["depends_on"]["go-river-provision"]["condition"]
        == "service_completed_successfully"
    )
    migration_command = _command_string(river_migrate)
    assert migration_command.count("dev-health-worker-migrate") == 2
    assert "dev-health-worker-migrate --check" in migration_command
    assert "MIGRATION_DATABASE_URI" in river_migrate["environment"]

    contractcheck = services["go-contractcheck"]
    assert contractcheck["profiles"] == ["go-workers"]
    assert contractcheck["restart"] == "no"
    assert contractcheck["network_mode"] == "none"
    assert contractcheck["build"]["target"] == "contractcheck"
    assert (
        contractcheck["depends_on"]["go-river-migrate"]["condition"]
        == "service_completed_successfully"
    )
    assert "validate" in _command_string(contractcheck)

    for name, service in services.items():
        if name in {"go-river-provision", "go-river-migrate", "go-contractcheck"}:
            continue
        assert "MIGRATION_DATABASE_URI" not in service["environment"]
        if name == "api":
            # CHAOS-3942: the pre-existing base `api` service, not part of the
            # Go bootstrap chain -- it must keep starting independently of it.
            continue
        assert (
            service["depends_on"]["go-contractcheck"]["condition"]
            == "service_completed_successfully"
        ), f"{name} must wait for the complete local Go bootstrap chain"

    rendered = _GO_COMPOSE.read_text(encoding="utf-8")
    assert "workerctl route" not in rendered
    assert _load_json(_DEPLOYMENT)["deployment_state"] == "coexistence_disabled"


@pytest.mark.parametrize(
    "path", [_GO_COMPOSE_ONLY, _GO_SWARM_ONLY, _GO_KUBERNETES_ONLY]
)
def test_go_only_overlays_scale_but_do_not_remove_celery_baseline(path: Path) -> None:
    documents = _load_yaml_documents(path)
    if len(documents) == 1:
        services = documents[0]["services"]
        assert set(services) == {
            "worker",
            "worker-ingest",
            "worker-external-ingest",
            "worker-heavy",
            "beat",
        }
        assert all(service["deploy"]["replicas"] == 0 for service in services.values())
        return

    deployments = {document["metadata"]["name"]: document for document in documents}
    assert set(deployments) == {
        "dev-health-worker",
        "dev-health-worker-ingest",
        "dev-health-worker-external-ingest",
        "dev-health-worker-heavy",
        "dev-health-beat",
    }
    assert all(
        deployment["spec"]["replicas"] == 0 for deployment in deployments.values()
    )


def test_reconciler_image_packages_both_runtime_contract_roots() -> None:
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp -R /src/contracts/jobs/v1 " + "/runtime/reconciler/app/contracts/jobs/v1;"
        in dockerfile
    )
    assert (
        "cp -R /src/contracts/sync-dispatch/v1 "
        + "/runtime/reconciler/app/contracts/sync-dispatch/v1;"
        in dockerfile
    )


def test_operator_image_packages_every_runtime_contract_it_loads() -> None:
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp -R /src/contracts/jobs/v1 " + "/runtime/operator/app/contracts/jobs/v1;"
        in dockerfile
    )
    assert (
        "cp -R /src/contracts/sync-dispatch/v1 "
        + "/runtime/operator/app/contracts/sync-dispatch/v1;"
        in dockerfile
    )
    assert (
        "cp /src/deploy/go-workers/deployment.json "
        + "/runtime/operator/app/deploy/go-workers/deployment.json;"
        in dockerfile
    )


def test_python_image_packages_and_validates_job_contracts() -> None:
    project = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    data_files = project["tool"]["setuptools"]["data-files"]
    patterns = {
        pattern
        for destination, destination_patterns in data_files.items()
        if destination == "contracts/jobs/v1"
        or destination.startswith("contracts/jobs/v1/")
        for pattern in destination_patterns
    }
    packaged = {
        path.relative_to(_REPO_ROOT)
        for pattern in patterns
        for path in _REPO_ROOT.glob(pattern)
    }
    expected = {
        path.relative_to(_REPO_ROOT)
        for path in (_REPO_ROOT / "contracts/jobs/v1").rglob("*")
        if path.is_file() and path.suffix in {".json", ".md"}
    }
    assert packaged == expected

    dockerfile = _APP_DOCKERFILE.read_text(encoding="utf-8")
    runtime = dockerfile.split("FROM python:3.14-slim AS runtime", maxsplit=1)[1]
    runtime = runtime.split("FROM runtime AS api", maxsplit=1)[0]
    assert "load_registry(); load_migration_jobs()" in runtime


def test_scheduler_image_packages_runtime_policy_inputs() -> None:
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp -R /src/contracts/jobs/v1 " + "/runtime/scheduler/app/contracts/jobs/v1;"
        in dockerfile
    )
    assert (
        "cp /src/deploy/go-workers/deployment.json "
        + "/runtime/scheduler/app/deploy/go-workers/deployment.json;"
        in dockerfile
    )
    scheduler_target = dockerfile.split("FROM runtime AS scheduler", maxsplit=1)[1]
    scheduler_target = scheduler_target.split("FROM runtime AS", maxsplit=1)[0]
    assert "WORKDIR /app" in scheduler_target


def test_deployment_pgbouncer_budget_matches_production_compose_defaults() -> None:
    manifest = _load_json(_DEPLOYMENT)
    pgbouncer = _load_yaml(_PRODUCTION_COMPOSE)["services"]["pgbouncer"]

    assert pgbouncer["profiles"] == ["pooler"]
    environment = pgbouncer["environment"]
    assert manifest["postgres_budget"][
        "pgbouncer_transaction_max_client_connections"
    ] == (_compose_default(environment["MAX_CLIENT_CONN"], "PGBOUNCER_MAX_CLIENT_CONN"))
    assert manifest["postgres_budget"]["pgbouncer_transaction_pool_size"] == (
        _compose_default(
            environment["DEFAULT_POOL_SIZE"], "PGBOUNCER_DEFAULT_POOL_SIZE"
        )
    )
    # Existing Celery/application traffic and the new Go domain role create
    # distinct (database,user) server pools in PgBouncer.
    assert manifest["postgres_budget"]["pgbouncer_transaction_server_pool_count"] == 2

    # The SESSION pools are bound to the manifest too. Only the transaction
    # pool used to be, so the queue and coordinator defaults had drifted below
    # the budget (22/10 against the manifest's 23/11) with nothing to catch it
    # (CHAOS-3872).
    services = _load_yaml(_PRODUCTION_COMPOSE)["services"]
    session_pools = {
        "pgbouncer-river-queue": (
            "pgbouncer_queue_session_pool_size",
            "PGBOUNCER_RIVER_QUEUE_POOL_SIZE",
            "pgbouncer_queue_session_max_client_connections",
            "PGBOUNCER_RIVER_QUEUE_MAX_CLIENT_CONN",
        ),
        "pgbouncer-river-coordinator": (
            "pgbouncer_coordinator_session_pool_size",
            "PGBOUNCER_RIVER_COORDINATOR_POOL_SIZE",
            "pgbouncer_coordinator_session_max_client_connections",
            "PGBOUNCER_RIVER_COORDINATOR_MAX_CLIENT_CONN",
        ),
    }
    for service, (pool_key, pool_var, client_key, client_var) in session_pools.items():
        environment = services[service]["environment"]
        assert manifest["postgres_budget"][pool_key] == _compose_default(
            environment["DEFAULT_POOL_SIZE"], pool_var
        ), f"{service} pool size drifted from the manifest budget"
        assert manifest["postgres_budget"][client_key] == _compose_default(
            environment["MAX_CLIENT_CONN"], client_var
        ), f"{service} client cap drifted from the manifest budget"

    # Helm renders the same three pools from values, so bind those too.
    helm_pools = _load_yaml(_HELM_CHART / "values.yaml")["goWorkers"]["pgbouncer"]
    assert (
        manifest["postgres_budget"]["pgbouncer_transaction_pool_size"]
        == helm_pools["transaction"]["poolSize"]
    )
    assert (
        manifest["postgres_budget"]["pgbouncer_queue_session_pool_size"]
        == helm_pools["queueSession"]["poolSize"]
    )
    assert (
        manifest["postgres_budget"]["pgbouncer_coordinator_session_pool_size"]
        == helm_pools["coordinatorSession"]["poolSize"]
    )


@pytest.mark.parametrize("path", [_PRODUCTION_COMPOSE, _SWARM_STACK])
def test_compose_and_swarm_migration_wiring_matches_contract(path: Path) -> None:
    manifest = _load_json(_DEPLOYMENT)
    services = _load_yaml(path)["services"]
    migrate = services["migrate"]
    environment = migrate["environment"]

    assert manifest["migration_job"]["binary"] == "dev-hops"
    assert set(manifest["migration_job"]["config_env"]) == set(
        _MIGRATION_CONFIG_DEFAULTS
    )
    for name, default in _MIGRATION_CONFIG_DEFAULTS.items():
        assert environment[name] == f"${{{name}:-{default}}}"
    assert set(manifest["migration_job"]["secret_env"]).issubset(environment)
    assert "POSTGRES_URI" in environment  # compatibility Alembic-only path
    _assert_migration_command(_command_string(migrate))

    for name, service in services.items():
        if name != "migrate":
            assert "MIGRATION_DATABASE_URI" not in (service.get("environment") or {})


def test_kubernetes_migration_wiring_matches_contract() -> None:
    manifest = _load_json(_DEPLOYMENT)
    config = _load_yaml(_KUBERNETES / "configmap.yaml")["data"]
    for name, default in _MIGRATION_CONFIG_DEFAULTS.items():
        assert config[name] == default

    job = next(
        document
        for document in _load_yaml_documents(_KUBERNETES / "migrate-job.yaml")
        if document["kind"] == "Job"
    )
    container = job["spec"]["template"]["spec"]["containers"][0]
    _assert_migration_command(_command_string(container))
    config_refs = {
        source["configMapRef"]["name"]
        for source in container["envFrom"]
        if "configMapRef" in source
    }
    secret_refs = {
        source["secretRef"]["name"]
        for source in container["envFrom"]
        if "secretRef" in source
    }
    assert config_refs == {"dev-health-config"}
    assert secret_refs == {
        "dev-health-migration-secrets",
    }

    secrets = {
        document["metadata"]["name"]: document
        for document in _load_yaml_documents(_KUBERNETES / "secrets.yaml")
        if document["kind"] == "Secret"
    }
    migration_secret_data = secrets["dev-health-migration-secrets"]["stringData"]
    assert set(migration_secret_data) == {"CLICKHOUSE_URI", "POSTGRES_URI"}
    assert all(migration_secret_data.values())
    assert not (
        set(secrets["dev-health-secrets"]["stringData"])
        & {"MIGRATION_DATABASE_URI", "MIGRATION_DATABASE_URI_FILE"}
    )
    assert set(manifest["migration_job"]["secret_env"]) == {
        "CLICKHOUSE_URI",
        "MIGRATION_DATABASE_URI",
    }


def test_helm_migration_wiring_matches_contract_and_isolates_elevated_dsn() -> None:
    manifest = _load_json(_DEPLOYMENT)
    values = _load_yaml(_HELM_CHART / "values.yaml")

    for name, default in _MIGRATION_CONFIG_DEFAULTS.items():
        assert values["config"][name] == default
    migration_secrets = values["migrations"]["hook"]["secretData"]
    assert set(manifest["migration_job"]["secret_env"]).issubset(migration_secrets)
    assert "POSTGRES_URI" in migration_secrets  # compatibility Alembic-only path
    assert not (_FORBIDDEN_SHARED_MIGRATION_SECRETS & values["secrets"]["data"].keys())

    template = (_HELM_CHART / "templates" / "migrate-job.yaml").read_text(
        encoding="utf-8"
    )
    helpers = (_HELM_CHART / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    _assert_migration_command(template)
    assert 'define "dev-health.migrationSecretData"' in helpers
    assert ".Values.migrations.hook.secretData" in helpers
    assert ".Values.migrations.hook.externalSecretName" in template

    schema = _load_json(_HELM_CHART / "values.schema.json")
    forbidden_clauses = schema["properties"]["secrets"]["properties"]["data"]["not"][
        "anyOf"
    ]
    assert {clause["required"][0] for clause in forbidden_clauses} == (
        _FORBIDDEN_SHARED_MIGRATION_SECRETS
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
@pytest.mark.parametrize("secret_name", sorted(_FORBIDDEN_SHARED_MIGRATION_SECRETS))
def test_helm_rejects_migration_dsn_in_shared_application_secret(
    secret_name: str,
) -> None:
    baseline = subprocess.run(
        ["helm", "template", "phase1", str(_HELM_CHART)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert baseline.returncode == 0, baseline.stderr

    rejected = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set-string",
            f"secrets.data.{secret_name}=postgresql://migration@direct/app",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "values don't meet the specifications of the schema" in rejected.stderr


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_accepts_dedicated_migration_dsn_without_sharing_it() -> None:
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set-string",
            "migrations.hook.secretData.MIGRATION_DATABASE_URI="
            + "postgresql://migration@direct/app",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rendered.returncode == 0, rendered.stderr

    secrets = [
        document
        for document in yaml.safe_load_all(rendered.stdout)
        if document and document.get("kind") == "Secret"
    ]
    holders = [
        secret
        for secret in secrets
        if "MIGRATION_DATABASE_URI" in (secret.get("stringData") or {})
    ]
    assert len(holders) == 1
    assert holders[0]["metadata"]["name"].endswith("-migrate-secrets")
    assert (
        "pre-install,pre-upgrade"
        in holders[0]["metadata"]["annotations"]["helm.sh/hook"]
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_migration_job_uses_its_dedicated_external_secret() -> None:
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set",
            "secrets.create=false",
            "--set-string",
            "secrets.externalSecretName=shared-app-secrets",
            "--set-string",
            "migrations.hook.externalSecretName=elevated-migration-secrets",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rendered.returncode == 0, rendered.stderr

    job = next(
        document
        for document in yaml.safe_load_all(rendered.stdout)
        if document and document.get("kind") == "Job"
    )
    container = job["spec"]["template"]["spec"]["containers"][0]
    secret_refs = {
        source["secretRef"]["name"]
        for source in container["envFrom"]
        if "secretRef" in source
    }
    assert secret_refs == {"elevated-migration-secrets"}


def _assert_insecure_optin_covers_endpoint(
    url: str, allow_insecure: str, source: str
) -> None:
    """Mirror internal/jobs/pagerduty `validBridgeEndpoint`: a plaintext bridge
    endpoint is only usable for loopback unless the insecure opt-in is set, so a
    renderer that defaults to a plaintext service name and leaves the opt-in
    unset produces a runner that can never build its handler.
    """
    parsed = urlparse(url)
    if parsed.scheme == "https" or parsed.hostname in _LOOPBACK_HOSTS:
        return
    assert allow_insecure.strip().lower() in _TRUTHY, (
        f"{source} defaults the bridge endpoint to plaintext {url!r}; the Go "
        "runtime rejects it unless WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE "
        "opts in"
    )


@pytest.mark.parametrize(
    ("path", "required_declaration"),
    [
        (_GO_COMPOSE, True),
        (_GO_COMPOSE_ONLY, False),
        (_GO_SWARM, True),
        (_GO_SWARM_ONLY, False),
    ],
)
def test_compose_surfaces_render_complete_pagerduty_bridge_env(
    path: Path, required_declaration: bool
) -> None:
    """CHAOS-3076: wherever a Compose/Swarm surface declares the PagerDuty
    stream runner, it must render every key the process needs to reach the
    Python worker bridge — including an insecure opt-in that matches the
    checked-in default endpoint.
    """
    services = _compose_pagerduty_services(path)
    assert bool(services) == required_declaration

    required = _pagerduty_required_env()
    for name, service in services.items():
        environment = service["environment"]
        missing = required - set(environment)
        assert not missing, f"{path.name}:{name} drops {sorted(missing)}"
        _assert_insecure_optin_covers_endpoint(
            _compose_variable_default(
                environment["WORKER_OPERATIONAL_BRIDGE_URL"],
                "WORKER_OPERATIONAL_BRIDGE_URL",
            ),
            _compose_variable_default(
                environment["WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE"],
                "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE",
            ),
            f"{path.name}:{name}",
        )
        assert (
            _compose_variable_default(
                environment["PAGERDUTY_WEBHOOK_TRANSPORT"],
                "PAGERDUTY_WEBHOOK_TRANSPORT",
            )
            == _DEFAULT_WEBHOOK_TRANSPORT
        )


@pytest.mark.parametrize(
    ("path", "required_declaration"),
    [(_GO_KUBERNETES, True), (_GO_KUBERNETES_ONLY, False)],
)
def test_kubernetes_pagerduty_deployment_resolves_complete_bridge_env(
    path: Path, required_declaration: bool
) -> None:
    containers = _kubernetes_pagerduty_containers(path)
    assert bool(containers) == required_declaration

    required = _pagerduty_required_env()
    for name, container in containers.items():
        missing = required - _kubernetes_container_env(container)
        assert not missing, f"{path.name}:{name} drops {sorted(missing)}"

    if not containers:
        return
    config = _load_yaml(_KUBERNETES_CONFIGMAP)["data"]
    assert config["PAGERDUTY_WEBHOOK_TRANSPORT"] == _DEFAULT_WEBHOOK_TRANSPORT
    _assert_insecure_optin_covers_endpoint(
        config["WORKER_OPERATIONAL_BRIDGE_URL"],
        config["WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE"],
        _KUBERNETES_CONFIGMAP.name,
    )
    # The bearer token stays a Secret key with an empty placeholder; a rendered
    # literal would commit the credential.
    secret = next(
        document
        for document in _load_yaml_documents(_KUBERNETES_SECRETS)
        if document["metadata"]["name"] == "dev-health-secrets"
    )
    assert secret["stringData"]["WORKER_OPERATIONAL_BRIDGE_TOKEN"] == ""


def test_helm_pagerduty_profile_resolves_complete_bridge_env() -> None:
    values = _load_yaml(_HELM_CHART / "values.yaml")
    groups = [
        group
        for group in values["goWorkers"]["groups"]
        if group.get("runtimeProfile") == _PAGERDUTY_RUNTIME_PROFILE
    ]
    assert len(groups) == 1

    # values.runtimeProfile only controls the rendered process if the template
    # actually binds DEV_HEALTH_PROFILE to it. Asserting the values alone let a
    # template edit change the profile every pod runs while this test stayed
    # green, so the binding is pinned too.
    workers_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert (
        "{name: DEV_HEALTH_PROFILE, value: {{ $group.runtimeProfile | quote }}}"
        in workers_template
    ), "the chart must render DEV_HEALTH_PROFILE from $group.runtimeProfile"

    # Every Go worker group inherits the shared ConfigMap and Secret, so the chart's
    # value surface is what decides whether the runner is wired.
    template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert (
        'configMapRef: {name: {{ include "dev-health.configMapName" $ }}}' in template
    )
    assert 'secretRef: {name: {{ include "dev-health.secretName" $ }}}' in template

    resolved = set(values["config"]) | set(values["secrets"]["data"])
    missing = _pagerduty_required_env() - resolved
    assert not missing, f"helm values drop {sorted(missing)}"
    assert values["config"]["PAGERDUTY_WEBHOOK_TRANSPORT"] == (
        _DEFAULT_WEBHOOK_TRANSPORT
    )
    # An empty URL is auto-computed into the plaintext in-cluster API Service,
    # so the opt-in must hold for the derived endpoint as well.
    _assert_insecure_optin_covers_endpoint(
        values["config"]["WORKER_OPERATIONAL_BRIDGE_URL"],
        values["config"]["WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE"],
        "helm values.yaml",
    )
    assert 'define "dev-health.operationalBridgeURL"' in (
        _HELM_CHART / "templates" / "_helpers.tpl"
    ).read_text(encoding="utf-8")
    assert values["secrets"]["data"]["WORKER_OPERATIONAL_BRIDGE_TOKEN"] == ""


@pytest.mark.parametrize("path", [_PRODUCTION_COMPOSE, _SWARM_STACK])
def test_api_service_carries_bridge_token_and_webhook_transport(path: Path) -> None:
    """CHAOS-3076: the API authenticates bridge callers against the shared token
    and decides the PagerDuty dispatch from the transport selector. Omitting
    either leaves the bridge answering 401 or two runtimes consuming the same
    webhook stream.
    """
    environment = _load_yaml(path)["services"]["api"]["environment"]

    missing = _API_BRIDGE_ENV - set(environment)
    assert not missing, f"{path.name}:api drops {sorted(missing)}"
    assert (
        _compose_variable_default(
            environment["PAGERDUTY_WEBHOOK_TRANSPORT"], "PAGERDUTY_WEBHOOK_TRANSPORT"
        )
        == _DEFAULT_WEBHOOK_TRANSPORT
    )
    # The token must arrive from the environment, never as a committed literal.
    assert (
        _compose_variable_default(
            environment["WORKER_OPERATIONAL_BRIDGE_TOKEN"],
            "WORKER_OPERATIONAL_BRIDGE_TOKEN",
        )
        == ""
    )


def test_kubernetes_and_helm_api_carry_bridge_token_and_webhook_transport() -> None:
    api = next(
        document
        for document in _load_yaml_documents(_KUBERNETES_API)
        if document["kind"] == "Deployment"
    )
    container = api["spec"]["template"]["spec"]["containers"][0]
    missing = _API_BRIDGE_ENV - _kubernetes_container_env(container)
    assert not missing, f"api.yaml drops {sorted(missing)}"

    template = (_HELM_CHART / "templates" / "api-deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert (
        'configMapRef:\n                name: {{ include "dev-health.configMapName" .'
        in template
    )
    assert (
        'secretRef:\n                name: {{ include "dev-health.secretName" .'
        in template
    )
    values = _load_yaml(_HELM_CHART / "values.yaml")
    resolved = set(values["config"]) | set(values["secrets"]["data"])
    assert not _API_BRIDGE_ENV - resolved


# Every manifest process mapped to the service/deployment name each renderer
# uses for it. The river workers already have _RIVER_WORKER_SERVICES; this is
# the complete set, including the coordinator and stream binaries.
_MANIFEST_RENDERED_SERVICES = {
    "heavy": "go-worker-heavy",
    "ops": "go-worker-ops",
    "sync": "go-worker-sync",
    "sync-provider": "go-worker-sync-provider",
    "reconciler": "go-reconciler",
    "scheduler": "go-scheduler",
    "stream-external": "go-stream-external",
    "stream-ingest": "go-stream-ingest",
    "stream-pagerduty": "go-stream-pagerduty",
}

# The Go client speaks ClickHouse's native wire protocol and eagerly Ping()s at
# construction. Python's clickhouse-connect speaks HTTP on 8123, so the same
# variable name must resolve to a different port per runtime.
_CLICKHOUSE_NATIVE_PORT = ":9000/"
_CLICKHOUSE_HTTP_PORT = ":8123/"


def _compose_environment(service: dict) -> dict[str, str]:
    environment = service["environment"]
    assert isinstance(environment, dict), (
        "worker environments must use mapping form so the manifest binding can read them"
    )
    return {str(name): str(value) for name, value in environment.items()}


def _manifest_required_env(process: dict) -> set[str]:
    return set(process.get("secret_env") or []) | set(process.get("env") or [])


def test_every_manifest_process_env_requirement_is_rendered_by_every_renderer() -> None:
    """CHAOS-3872: bind deployment.json's env contract to what actually ships.

    The queue/replica/drain contract was already test-locked across renderers,
    but the credential and DSN layer was not -- so it drifted per renderer, and
    it is the layer an operator hits FIRST on scale-up. Compose and Swarm never
    passed the sync group SETTINGS_ENCRYPTION_KEY (handler construction fails
    closed without it), and raw Kubernetes declared none of the four DSN/secret
    keys its Go pods need.
    """
    processes = {
        process["name"]: process for process in _load_json(_DEPLOYMENT)["processes"]
    }
    assert set(processes) == set(_MANIFEST_RENDERED_SERVICES), (
        "a manifest process has no renderer mapping; this test would silently skip it"
    )

    compose = _load_yaml(_GO_COMPOSE)["services"]
    swarm = _load_yaml(_GO_SWARM)["services"]
    kubernetes = {
        document["metadata"]["name"]: document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "Deployment"
    }

    for name, service_name in _MANIFEST_RENDERED_SERVICES.items():
        required = _manifest_required_env(processes[name])
        assert required, f"{name} declares no env requirements to bind"

        for renderer, services in (("Compose", compose), ("Swarm", swarm)):
            rendered = set(_compose_environment(services[service_name]))
            missing = required - rendered
            assert not missing, (
                f"{renderer} {service_name} is missing manifest-required env {sorted(missing)}"
            )

        deployment = kubernetes[f"dev-health-{service_name}"]
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        rendered = _kubernetes_container_env(container)
        missing = required - rendered
        assert not missing, (
            f"Kubernetes dev-health-{service_name} is missing manifest-required env "
            f"{sorted(missing)}"
        )


def test_every_renderer_gives_go_workers_a_native_protocol_clickhouse_uri() -> None:
    """CHAOS-3872: CLICKHOUSE_URI must be the native port for Go, HTTP for Python."""
    processes = {
        process["name"]: process for process in _load_json(_DEPLOYMENT)["processes"]
    }
    clickhouse_groups = {
        name
        for name, process in processes.items()
        if "CLICKHOUSE_URI" in _manifest_required_env(process)
    }
    assert clickhouse_groups, "no manifest process requires ClickHouse"

    compose = _load_yaml(_GO_COMPOSE)["services"]
    swarm = _load_yaml(_GO_SWARM)["services"]
    for name in clickhouse_groups:
        service_name = _MANIFEST_RENDERED_SERVICES[name]
        for renderer, services in (("Compose", compose), ("Swarm", swarm)):
            uri = _compose_environment(services[service_name])["CLICKHOUSE_URI"]
            assert _CLICKHOUSE_NATIVE_PORT in uri, (
                f"{renderer} {service_name} CLICKHOUSE_URI is not the native port: {uri}"
            )
            assert _CLICKHOUSE_HTTP_PORT not in uri

    # Raw Kubernetes shares one Secret between Python and Go, so the Go pods
    # take a dedicated Secret listed AFTER it in envFrom; Kubernetes resolves
    # duplicate keys in favour of the later source.
    secrets = {
        document["metadata"]["name"]: document.get("stringData") or {}
        for document in _load_yaml_documents(_KUBERNETES_SECRETS)
        if document["kind"] == "Secret"
    }
    go_secret = secrets["dev-health-go-worker-secrets"]
    assert _CLICKHOUSE_NATIVE_PORT in go_secret["CLICKHOUSE_URI"]
    assert _CLICKHOUSE_HTTP_PORT in secrets["dev-health-secrets"]["CLICKHOUSE_URI"], (
        "the shared Secret still serves Python, which needs the HTTP interface"
    )

    kubernetes = {
        document["metadata"]["name"]: document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document.get("kind") == "Deployment"
    }
    for name in clickhouse_groups:
        container = kubernetes[f"dev-health-{_MANIFEST_RENDERED_SERVICES[name]}"][
            "spec"
        ]["template"]["spec"]["containers"][0]
        references = [
            source["secretRef"]["name"]
            for source in container.get("envFrom") or []
            if "secretRef" in source
        ]
        assert "dev-health-go-worker-secrets" in references, (
            f"dev-health-{_MANIFEST_RENDERED_SERVICES[name]} does not mount the Go secret"
        )
        assert references.index("dev-health-go-worker-secrets") > references.index(
            "dev-health-secrets"
        ), "the Go Secret must come last or the shared HTTP URI wins"

    # Helm renders through templates, which are not parseable as YAML without
    # the helm binary. Bind the wiring itself: the Go worker template must set
    # CLICKHOUSE_URI as an explicit env entry (which beats envFrom) from the
    # native-protocol helper, and that helper must use the native port.
    go_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert 'include "dev-health.goWorkerClickhouseURI"' in go_template
    assert "name: CLICKHOUSE_URI" in go_template
    helpers = (_HELM_CHART / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    native_helper = helpers.split('define "dev-health.goWorkerClickhouseURI"')[1]
    assert "9000" in native_helper.split("{{- end }}")[0] or "9000" in native_helper


def test_go_worker_health_check_flips_authority_with_the_overlay() -> None:
    """CHAOS-3942: /health/workers is Celery-authoritative until told
    otherwise. EXPECTED_WORKER_GROUPS must reach the API service through
    every go-workers opt-in overlay -- and stay ABSENT from the Celery-owned
    base, so a base-only deployment (no go-workers overlay applied) never
    silently loses its Celery signal, and an operator applying the overlay
    gets the authority flip in the same change that stages the fleet.
    """
    base_compose_api = _load_yaml(_PRODUCTION_COMPOSE)["services"]["api"]
    assert "EXPECTED_WORKER_GROUPS" not in (base_compose_api.get("environment") or {})
    overlay_compose_api = _load_yaml(_GO_COMPOSE)["services"]["api"]
    assert (
        _compose_variable_default(
            overlay_compose_api["environment"]["EXPECTED_WORKER_GROUPS"],
            "EXPECTED_WORKER_GROUPS",
        )
        == _EXPECTED_WORKER_GROUPS_VALUE
    )

    base_swarm_api = _load_yaml(_SWARM_STACK)["services"]["api"]
    assert "EXPECTED_WORKER_GROUPS" not in (base_swarm_api.get("environment") or {})
    overlay_swarm_api = _load_yaml(_GO_SWARM)["services"]["api"]
    assert (
        _compose_variable_default(
            overlay_swarm_api["environment"]["EXPECTED_WORKER_GROUPS"],
            "EXPECTED_WORKER_GROUPS",
        )
        == _EXPECTED_WORKER_GROUPS_VALUE
    )

    # Kubernetes: the base api.yaml container only ever resolves the var
    # through an OPTIONAL configMapKeyRef -- so a base-only cluster (no
    # go-workers.yaml applied) never even has the referenced ConfigMap, and
    # the var is absent, not empty.
    api = next(
        document
        for document in _load_yaml_documents(_KUBERNETES_API)
        if document["kind"] == "Deployment"
    )
    container = api["spec"]["template"]["spec"]["containers"][0]
    env_entry = next(
        item for item in container["env"] if item["name"] == "EXPECTED_WORKER_GROUPS"
    )
    ref = env_entry["valueFrom"]["configMapKeyRef"]
    assert ref["optional"] is True
    assert ref["key"] == "EXPECTED_WORKER_GROUPS"

    go_worker_config = next(
        document
        for document in _load_yaml_documents(_GO_KUBERNETES)
        if document["kind"] == "ConfigMap"
        and document["metadata"]["name"] == ref["name"]
    )
    assert go_worker_config["data"]["EXPECTED_WORKER_GROUPS"] == (
        _EXPECTED_WORKER_GROUPS_VALUE
    )
    # A ConfigMap can only be resolved from the SAME namespace as the pod
    # that references it -- a namespace typo here would pass a name-only
    # check yet never resolve at runtime.
    assert go_worker_config["metadata"]["namespace"] == api["metadata"]["namespace"]
    # The base ConfigMap must never declare the key itself -- only the
    # go-workers-only fragment does, which is what makes it absent by
    # default.
    assert "EXPECTED_WORKER_GROUPS" not in _load_yaml(_KUBERNETES_CONFIGMAP)["data"]

    # Helm: no separate overlay file, so the trigger is the goWorkers.enabled
    # value flag itself, rendered directly into the api Deployment's env.
    values = _load_yaml(_HELM_CHART / "values.yaml")
    assert values["goWorkers"]["expectedWorkerGroups"] == [
        "heavy",
        "ops",
        "sync",
        "sync-provider",
    ]
    template = (_HELM_CHART / "templates" / "api-deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "EXPECTED_WORKER_GROUPS" in template
    assert ".Values.goWorkers.enabled" in template


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_api_deployment_carries_expected_worker_groups_only_when_go_workers_enabled() -> (
    None
):
    """CHAOS-3942: render both states through the real templating engine --
    a string match on the template source cannot prove the conditional
    actually gates the rendered manifest."""
    disabled = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--show-only",
            "templates/api-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "EXPECTED_WORKER_GROUPS" not in disabled.stdout

    enabled = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set",
            "goWorkers.enabled=true",
            "--set",
            "goWorkers.pgbouncer.enabled=true",
            "--set-string",
            "goWorkers.pgbouncer.postgres.host=pg",
            "--set-string",
            "goWorkers.pgbouncer.postgres.database=db",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=x",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=x",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=x",
            "--show-only",
            "templates/api-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rendered = next(
        document
        for document in yaml.safe_load_all(enabled.stdout)
        if document and document.get("kind") == "Deployment"
    )
    container = rendered["spec"]["template"]["spec"]["containers"][0]
    entry = next(
        item for item in container["env"] if item["name"] == "EXPECTED_WORKER_GROUPS"
    )
    assert entry["value"] == _EXPECTED_WORKER_GROUPS_VALUE


_COMPOSE_MERGE_REQUIRED_ENV = {
    "POSTGRES_HOST": "pg",
    "POSTGRES_USER": "u",
    "POSTGRES_PASSWORD": "p",
    "POSTGRES_DB": "db",
    "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
    "RIVER_QUEUE_DATABASE_PASSWORD": "x",
    "RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
    "SETTINGS_ENCRYPTION_KEY": "x",
    "WORKER_DATABASE_URI": "postgresql://u:p@pg:5432/db",
    "POSTGRES_URI": "postgresql://u:p@pg:5432/db",
    "COORDINATOR_DATABASE_URI": "postgresql://u:p@pg:5432/db",
}


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not installed")
@pytest.mark.parametrize(
    ("base", "overlay"),
    [(_PRODUCTION_COMPOSE, _GO_COMPOSE), (_SWARM_STACK, _GO_SWARM)],
)
def test_compose_merge_flips_api_authority_without_losing_base_fields(
    base: Path, overlay: Path
) -> None:
    """CHAOS-3942: the string-level `environment:` merge assumption behind
    compose.go-workers.yml/stack.go-workers.yml is exactly what Compose's
    multi-file merge does -- proved here through the real `docker compose
    config` engine, not just by parsing base and overlay independently
    (codex review round 2: independent parsing can't prove the merge itself
    keeps the base service's image/command/ports/other environment intact).
    """
    result = subprocess.run(
        ["docker", "compose", "-f", str(base), "-f", str(overlay), "config"],
        check=True,
        capture_output=True,
        text=True,
        env={**_COMPOSE_MERGE_REQUIRED_ENV, "PATH": os.environ["PATH"]},
    )
    merged = yaml.safe_load(result.stdout)
    api = merged["services"]["api"]
    assert api["image"]
    assert api["environment"]["EXPECTED_WORKER_GROUPS"] == _EXPECTED_WORKER_GROUPS_VALUE
    # The merge must ADD the key, not replace the whole service definition.
    assert api["environment"]["CELERY_BROKER_URL"]
    assert "POSTGRES_URI" in api["environment"]
