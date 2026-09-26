from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import tomllib
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _REPO_ROOT / "pyproject.toml"
_DEPLOYMENT = _REPO_ROOT / "deploy" / "go-workers" / "deployment.json"
_APP_DOCKERFILE = _REPO_ROOT / "docker" / "Dockerfile"
_GO_WORKER_DOCKERFILE = _REPO_ROOT / "docker" / "go-worker.Dockerfile"
_ROOT_COMPOSE = _REPO_ROOT / "compose.yml"
_HELM_CHART = _REPO_ROOT / "deploy" / "helm" / "dev-health"

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

_PAGERDUTY_RUNTIME_PROFILE = "pagerduty"

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
        if process["runtime"] == "river" and process.get("subcommand") == "worker"
    }


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_go_worker_groups_are_enabled_by_default_under_go_default_state() -> None:
    # CHAOS-5541: the manifest moved from coexistence_disabled (every process
    # off by default) to go_default (Go processes on by default, Celery
    # legacy/opt-in). Every one of the nine processes is enabled at
    # desired_replicas 1, matching the intended production topology (#2429's
    # compose.yml runs the nine-process fleet, including sync-provider, at
    # deploy.replicas: 1 each -- CHAOS-3087's deployment.json leaving every
    # process disabled was tracked there as this ticket's own gap).
    manifest = _load_json(_DEPLOYMENT)

    assert manifest["deployment_state"] == "go_default"
    assert manifest["runtime_role_env"] == [
        "RIVER_COORDINATOR_DATABASE_ROLE",
        "RIVER_DOMAIN_DATABASE_ROLE",
        "RIVER_QUEUE_DATABASE_ROLE",
    ]
    for process in manifest["processes"]:
        # sync-provider keeps a floor of one replica: its queue carries
        # system.dimension_fold, a scheduled job whose occurrences are dropped
        # as stale once no consumer claims them, so a zero-replica window
        # leaves the dimension tables unfolded with nothing to wake the pool.
        # Every other process may idle at zero.
        want_floor = 1 if process["name"] == "sync-provider" else 0
        assert process["min_replicas"] == want_floor
        assert process["enabled_by_default"]
        assert process["desired_replicas"] == 1
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
        "binary": "dho",
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
            # CHAOS-4530: dho workers providersync retire-linear-
            # pseudo-projects reads ClickHouse directly (the operator CLI's
            # first ClickHouse-touching verb), so it now requires the DSN too.
            "CLICKHOUSE_URI",
            "COORDINATOR_DATABASE_URI",
            "POSTGRES_URI",
            "WORKER_DATABASE_URI",
        ],
    }


def test_go_worker_image_packages_work_item_semantic_config() -> None:
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    for filename, runtime_path in _PACKAGED_WORK_ITEM_CONFIG.items():
        source = f"src/dev_health_ops/config/{filename}"
        assert (_REPO_ROOT / source).is_file()
        assert f"COPY {source} ./src/dev_health_ops/config/{filename}" in dockerfile
        # The worker is `dho worker`: the dho image and the operator image
        # (prod's pin) both stage the config it reads.
        for image in ("dho", "operator"):
            assert f"/runtime/{image}{runtime_path}" in dockerfile


def test_go_worker_image_packages_lifecycle_route_operator() -> None:
    """A worker pod runs the dho image, so `dho workers ...` runs in it."""
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert "cp /out/dho /runtime/dho/usr/local/bin/dho;" in dockerfile


def test_go_deployment_surfaces_are_additive_and_group_complete() -> None:
    """CHAOS-3052: the supported deploy surface (the Helm chart) renders a
    complete topology for the nine processes. CHAOS-5541: the chart defaults
    to replicas: 1, matching deployment.json's go_default posture. CHAOS-6950
    deleted the unsupported Swarm/raw-Kubernetes/production-Compose renderers
    this test used to also check; the chart is the only one left.
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

    values = _load_yaml(_HELM_CHART / "values.yaml")
    # CHAOS-4195: the Celery Helm templates/values keys and Kubernetes
    # manifests were deleted, so goWorkers is the only topology left and
    # defaults to enabled=true. CHAOS-5541: each group now defaults to
    # replicas: 1, matching deployment.json's go_default posture.
    assert values["goWorkers"]["enabled"] is True
    assert "profiles" not in values["goWorkers"]
    assert "groups" in values["goWorkers"]
    assert {
        group["name"] for group in values["goWorkers"]["groups"]
    } == expected_profiles
    assert all(group["replicas"] == 1 for group in values["goWorkers"]["groups"])
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
    duplicated in the oracle. This keeps the chart tied to the same
    executable process contract while allowing overlapping worker groups.
    CHAOS-6950: the chart is the only renderer checked here; the Compose
    overlay, Swarm and raw Kubernetes renderers this used to also bind are
    deleted.
    """
    river = _river_processes()
    assert set(river) == set(_RIVER_WORKER_SERVICES)

    values = _load_yaml(_HELM_CHART / "values.yaml")
    helm_groups = {group["name"]: group for group in values["goWorkers"]["groups"]}
    for group in _RIVER_WORKER_SERVICES:
        assert helm_groups[group]["subcommand"] == "worker"
        assert helm_groups[group]["queues"] == river[group]["queues"]
        assert "profile" not in helm_groups[group]

    stream_runtime_profiles = {
        "stream-external": "external",
        "stream-ingest": "ingest",
        "stream-pagerduty": "pagerduty",
    }
    for group, runtime_profile in stream_runtime_profiles.items():
        helm_group = helm_groups[group]
        assert helm_group["runtimeProfile"] == runtime_profile
        assert helm_group["subcommand"] == "stream-runner"
        # The group takes the one goWorkers.image, which is the dho image.
        image = helm_group.get("image", values["goWorkers"]["image"])
        assert "/dev-health-go-dho:" in image, image

    for verb in ("reconciler", "scheduler"):
        # `dho reconciler` / `dho scheduler`: the dho image with the verb first.
        assert helm_groups[verb]["subcommand"] == verb
        image = helm_groups[verb].get("image", values["goWorkers"]["image"])
        assert "/dev-health-go-dho:" in image, image


def test_group_replica_and_drain_contract_matches_every_renderer() -> None:
    """The chart's group replicas, drain budgets and autoscaling bounds match
    deployment.json (CHAOS-6950: the chart is the only renderer left)."""
    manifest = {
        process["name"]: process for process in _load_json(_DEPLOYMENT)["processes"]
    }
    helm = {
        group["name"]: group
        for group in _load_yaml(_HELM_CHART / "values.yaml")["goWorkers"]["groups"]
    }
    assert set(helm) == set(manifest)
    for profile, contract in manifest.items():
        desired = contract["desired_replicas"]
        grace = contract["shutdown_grace_seconds"]
        assert helm[profile]["replicas"] == desired
        assert helm[profile]["terminationGracePeriodSeconds"] == grace
        if contract["runtime"] == "river":
            assert (
                helm[profile]["autoscaling"]["maxReplicas"] == contract["max_replicas"]
            )
    helm_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert "--shutdown-timeout=" in helm_template


def test_reconciler_image_packages_both_runtime_contract_roots() -> None:
    # The reconciler is `dho reconciler`, run from the dho image.
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp -R /src/contracts/jobs/v1 " + "/runtime/dho/app/contracts/jobs/v1;"
        in dockerfile
    )
    assert (
        "cp -R /src/contracts/sync-dispatch/v1 "
        + "/runtime/dho/app/contracts/sync-dispatch/v1;"
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
    # The stage header is matched by its STAGE NAME, not by the base image.
    # CHAOS-4922 moved the base to an ARG (`FROM ${PYTHON_BASE_IMAGE} AS
    # runtime`) and this split silently produced an IndexError -- the test was
    # coupled to a detail it does not assert anything about. Anchoring on
    # `AS runtime` keeps it working across base-image changes, which is the only
    # thing that ever moves here.
    # Case-insensitive with optional leading whitespace: Dockerfile
    # instructions are case-insensitive and may be indented, so `from ... as
    # runtime` is a valid stage header this would otherwise miss (codex round 1).
    runtime = re.split(
        r"^\s*FROM\s+.*\s+AS\s+runtime\s*$", dockerfile, maxsplit=1, flags=re.M | re.I
    )[1]
    runtime = runtime.split("FROM runtime AS api", maxsplit=1)[0]
    assert "load_registry(); load_migration_jobs()" in runtime


def test_scheduler_image_packages_runtime_policy_inputs() -> None:
    # The scheduler is `dho scheduler`, run from the dho image; it loads
    # contracts/jobs/v1 relative to /app.
    dockerfile = _GO_WORKER_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "cp -R /src/contracts/jobs/v1 " + "/runtime/dho/app/contracts/jobs/v1;"
        in dockerfile
    )
    dho_target = dockerfile.split("FROM runtime AS dho", maxsplit=1)[1]
    dho_target = dho_target.split("FROM runtime AS", maxsplit=1)[0]
    assert "WORKDIR /app" in dho_target


def test_deployment_pgbouncer_budget_matches_helm_defaults() -> None:
    manifest = _load_json(_DEPLOYMENT)

    # Helm renders the three pools from values, so bind those to the budget
    # (CHAOS-6950: the production Compose file this also bound is deleted).
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


_BRIDGE_NAMES = frozenset(
    {
        "WORKER_OPERATIONAL_BRIDGE_URL",
        "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE",
        "WORKER_OPERATIONAL_BRIDGE_TOKEN",
    }
)
_BRIDGE_FLAG_NAMES = ("--operational-bridge-url", "--operational-bridge-allow-insecure")
_BRIDGE_FLAGS = tuple(f"{name}=" for name in _BRIDGE_FLAG_NAMES)


def _has_bridge_flag(args: list[str]) -> bool:
    """True if `args` renders either deleted flag, in either spelling Go's
    flag package accepts: `--flag=value` (every renderer's own convention)
    or `--flag value` (a separate arg) -- a guard that only matched the
    `=` form would miss a space-separated reintroduction (codex round 2)."""
    return any(arg.startswith(flag) for arg in args for flag in _BRIDGE_FLAGS) or any(
        arg in _BRIDGE_FLAG_NAMES for arg in args
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_no_renderer_still_emits_the_deleted_operational_bridge(
    tmp_path: Path,
) -> None:
    """CHAOS-6279 negative pin: the worker-operational-bridge mechanism
    (WORKER_OPERATIONAL_BRIDGE_URL/TOKEN/ALLOW_INSECURE, and the
    --operational-bridge-url/--operational-bridge-allow-insecure flags that
    carried two of them) is deleted from every in-repo renderer -- the local
    Compose stack and Helm (CHAOS-6950 deleted the production Compose, Swarm
    and raw Kubernetes renderers this also covered). internal/platform/config
    no longer registers any of the three, so a renderer that still emitted one
    of the flags would crash-loop the binary with "unknown flag" outright;
    this test proves none of them does, on the rendered output, not just by
    the absence of a deleted test file.
    """
    # Compose: no service's command/environment may carry any of the four
    # surfaces (flags or env) on the local dev stack.
    services = _load_yaml(_ROOT_COMPOSE).get("services") or {}
    for name, service in services.items():
        command = [str(a) for a in (service.get("command") or [])]
        assert not _has_bridge_flag(command), (
            f"{_ROOT_COMPOSE.name}:{name} still renders an operational-bridge flag"
        )
        environment = service.get("environment") or {}
        leaked = _BRIDGE_NAMES & set(environment)
        assert not leaked, f"{_ROOT_COMPOSE.name}:{name} still carries {sorted(leaked)}"
        # The metrics-api health dependency this mechanism justified must be
        # gone too -- a service still gated on it would fail to start whenever
        # metrics-api is unhealthy, for a call it no longer makes.
        assert "metrics-api" not in (service.get("depends_on") or {}), (
            f"{_ROOT_COMPOSE.name}:{name} still depends on metrics-api"
        )

    # Helm: render the real templating engine (default values and with
    # metricsApi enabled, the state most likely to have re-introduced a
    # bridge reference) and check every container's rendered args, plus the
    # ConfigMap/Secret values.yaml declares by default.
    values = _load_yaml(_HELM_CHART / "values.yaml")
    resolved = set(values.get("config") or {}) | set(
        (values.get("secrets") or {}).get("data") or {}
    )
    assert not (_BRIDGE_NAMES & resolved), (
        f"helm values.yaml still declares {sorted(_BRIDGE_NAMES & resolved)}"
    )
    helpers = (_HELM_CHART / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    assert "operationalBridgeURL" not in helpers, (
        "_helpers.tpl still defines the deleted operationalBridgeURL helper"
    )
    for extra_set in ([], ["metricsApi.enabled=true"]):
        argv = ["helm", "template", "phase1", str(_HELM_CHART)]
        for value in extra_set:
            argv += ["--set", value]
        rendered = subprocess.run(argv, check=True, capture_output=True, text=True)
        for document in yaml.safe_load_all(rendered.stdout):
            if not document or document.get("kind") != "Deployment":
                continue
            containers = document["spec"]["template"]["spec"].get("containers") or []
            for container in containers:
                args = [str(a) for a in (container.get("args") or [])]
                assert not _has_bridge_flag(args), (
                    f"{document['metadata']['name']} (extra_set={extra_set}) still "
                    "renders an operational-bridge flag"
                )


def test_helm_pagerduty_profile_binding_is_pinned() -> None:
    """CHAOS-6279: the bridge-env completeness check this test used to run
    (values.config/secrets.data carrying WORKER_OPERATIONAL_BRIDGE_*) is
    gone along with that mechanism -- see this file's CHAOS-6279 header
    note. What remains real: exactly one goWorkers group runs the pagerduty
    profile, and the chart actually binds it to DEV_HEALTH_PROFILE, not
    just declares it in values.
    """
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
        '- {{ printf "--profile=%s" . | quote }}' in workers_template
        and "{{- with $group.runtimeProfile }}" in workers_template
    ), "the chart must render --profile from $group.runtimeProfile"

    # Every Go worker group inherits the shared ConfigMap and Secret, so the chart's
    # value surface is what decides whether the runner is wired.
    template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    # CHAOS-4984: envFrom is built into a list variable (extraEnvFrom first,
    # then these two) and toYaml'd, rather than written as literal envFrom
    # entries, so the pinned strings are the dict-builder calls instead.
    assert (
        'dict "configMapRef" (dict "name" (include "dev-health.configMapName" $))'
        in template
    )
    assert (
        'dict "secretRef" (dict "name" (include "dev-health.secretName" $))' in template
    )


def test_helm_api_envfrom_wiring() -> None:
    """CHAOS-6279: the bridge-token completeness check this test used to run
    is gone along with that mechanism -- see this file's CHAOS-6279 header
    note. What remains real: the api Deployment template actually wires its
    envFrom to the shared ConfigMap/Secret by name (CHAOS-6950: the raw
    Kubernetes manifest this also checked is deleted).
    """
    template = (_HELM_CHART / "templates" / "api-deployment.yaml").read_text(
        encoding="utf-8"
    )
    # CHAOS-4984: same dict-builder pattern as go-workers.yaml above.
    assert (
        'dict "configMapRef" (dict "name" (include "dev-health.configMapName" .))'
        in template
    )
    assert (
        'dict "secretRef" (dict "name" (include "dev-health.secretName" .))' in template
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_api_deployment_carries_expected_worker_groups_only_when_go_workers_enabled() -> (
    None
):
    """CHAOS-3942: render both states through the real templating engine --
    a string match on the template source cannot prove the conditional
    actually gates the rendered manifest. CHAOS-4195 flipped goWorkers.enabled
    to default true (there is no Celery baseline left to default to), so the
    "disabled" state is no longer the bare-defaults render -- prove the gate
    still holds by asserting it explicitly with --set goWorkers.enabled=false.
    """
    disabled = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set",
            "goWorkers.enabled=false",
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


def test_compose_go_worker_heavy_alone_owns_the_metrics_queue() -> None:
    """CHAOS-4351: `go-worker-heavy` is the only worker group whose queue
    set includes `metrics` (verified below, not just asserted -- a future
    queue reshuffle that quietly added `metrics` to another group would
    silently change which group owns it with nothing failing).

    CHAOS-6279: no worker group's command line references metrics-api and
    no group depends on it being healthy. CHAOS-6950: read from the local
    Compose stack (compose.yml); the production Compose file this also read
    is deleted.
    """
    services = _load_yaml(_ROOT_COMPOSE)["services"]
    go_worker_services = {
        name: svc
        for name, svc in services.items()
        if name.startswith("go-worker")
        or name.startswith("go-reconciler")
        or name.startswith("go-scheduler")
        or name.startswith("go-stream")
    }
    assert len(go_worker_services) >= 7, sorted(go_worker_services)

    metrics_owners = [
        name
        for name, svc in go_worker_services.items()
        for arg in svc.get("command") or []
        if isinstance(arg, str)
        and arg.startswith("--queues=")
        and "metrics" in arg.split("=", 1)[1].split(",")
    ]
    assert metrics_owners == ["go-worker-heavy"], (
        f"expected only go-worker-heavy to own the metrics queue, found: {metrics_owners}"
    )

    for name, svc in go_worker_services.items():
        command = [str(a) for a in (svc.get("command") or [])]
        assert not _has_bridge_flag(command), (
            f"{name}: an operational-bridge flag is deleted (CHAOS-6279), found in command"
        )
        assert "metrics-api" not in (svc.get("depends_on") or {}), (
            f"{name}: the metrics-api depends_on edge is deleted (CHAOS-6279)"
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_metrics_api_deployment_only_renders_when_enabled() -> None:
    """CHAOS-4351: `metricsApi.enabled` (default false) must actually gate
    the Deployment/Service, and the enabled render must carry the same
    resources as `api`'s own -- same workload, same bound, per team-lead's
    ruling that a bridge OOM here must not be able to take `api` down.
    """
    disabled = subprocess.run(
        ["helm", "template", "phase1", str(_HELM_CHART)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "name: phase1-dev-health-metrics-api" not in disabled.stdout

    enabled = subprocess.run(
        [
            "helm",
            "template",
            "phase1",
            str(_HELM_CHART),
            "--set",
            "metricsApi.enabled=true",
            "--show-only",
            "templates/metrics-api-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    docs = list(yaml.safe_load_all(enabled.stdout))
    deployment = next(d for d in docs if d and d.get("kind") == "Deployment")
    service = next(d for d in docs if d and d.get("kind") == "Service")
    assert service["spec"]["type"] == "ClusterIP"
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["image"]
    assert container["resources"]["limits"]["memory"] == "1Gi"


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_helm_go_worker_groups_roll_on_shared_config_or_secret_change() -> None:
    """All nine go-worker groups envFrom the shared ConfigMap and
    Secret (go-workers.yaml), same as api-deployment.yaml -- but envFrom never
    triggers a rollout on its own. Prod rev 28 changed one ConfigMap key and
    rolled api and the since-removed billing-edge (they carried checksum/config
    + checksum/secret pod-template annotations) but none of the nine go-worker groups, which had no such
    annotation; the operator had to `kubectl rollout restart` them by hand.
    Render the chart with two different config values and two different
    secret values and assert every group's pod-template annotations change
    accordingly, while its image and probes do not.
    """
    expected_groups = {
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

    def render(*extra_args: str) -> dict[str, dict]:
        result = subprocess.run(
            [
                "helm",
                "template",
                "phase1",
                str(_HELM_CHART),
                *extra_args,
                "--show-only",
                "templates/go-workers.yaml",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        docs = list(yaml.safe_load_all(result.stdout))
        by_group = {
            doc["metadata"]["labels"]["dev-health.io/worker-group"]: doc
            for doc in docs
            if doc and doc.get("kind") == "Deployment"
        }
        assert set(by_group) == expected_groups
        return by_group

    def fingerprint(deployment: dict) -> tuple[str, object, object]:
        pod = deployment["spec"]["template"]
        container = pod["spec"]["containers"][0]
        return (
            container["image"],
            container.get("livenessProbe"),
            container.get("readinessProbe"),
        )

    base = render()
    config_changed = render("--set-string", "config.LOG_LEVEL=DEBUG")
    secret_changed = render(
        "--set-string", "secrets.data.DATABASE_URI=postgresql://rotated"
    )

    for group in expected_groups:
        base_annotations = base[group]["spec"]["template"]["metadata"]["annotations"]
        assert "checksum/config" in base_annotations, group
        assert "checksum/secret" in base_annotations, group

        config_annotations = config_changed[group]["spec"]["template"]["metadata"][
            "annotations"
        ]
        assert (
            config_annotations["checksum/config"]
            != (base_annotations["checksum/config"])
        ), f"{group}: a ConfigMap-only change must roll this group"
        assert (
            config_annotations["checksum/secret"] == base_annotations["checksum/secret"]
        ), f"{group}: a ConfigMap-only change must not touch the secret checksum"
        assert fingerprint(config_changed[group]) == fingerprint(base[group]), (
            f"{group}: a config value change must not touch image or probes"
        )

        secret_annotations = secret_changed[group]["spec"]["template"]["metadata"][
            "annotations"
        ]
        assert (
            secret_annotations["checksum/secret"]
            != (base_annotations["checksum/secret"])
        ), f"{group}: a Secret-only change must roll this group"
        assert (
            secret_annotations["checksum/config"] == base_annotations["checksum/config"]
        ), f"{group}: a Secret-only change must not touch the config checksum"
        assert fingerprint(secret_changed[group]) == fingerprint(base[group]), (
            f"{group}: a secret value change must not touch image or probes"
        )


def test_helm_go_worker_health_check_authority_is_the_fleet() -> None:
    """CHAOS-3942: /health/workers is Go-fleet-authoritative by default.

    The chart renders EXPECTED_WORKER_GROUPS into the api Deployment straight
    from the goWorkers.enabled value flag (CHAOS-6950 deleted the Compose,
    Swarm and raw Kubernetes renderers this also pinned).
    """
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


def test_helm_gives_go_workers_a_native_protocol_clickhouse_uri() -> None:
    """CHAOS-3872: CLICKHOUSE_URI must be the native port for Go, HTTP for Python.

    Helm renders through templates, which are not parseable as YAML without
    the helm binary. Bind the wiring itself: the Go worker template must set
    CLICKHOUSE_URI as an explicit env entry (which beats envFrom) from the
    native-protocol helper, and that helper must use the native port.
    """
    go_template = (_HELM_CHART / "templates" / "go-workers.yaml").read_text(
        encoding="utf-8"
    )
    assert 'include "dev-health.goWorkerClickhouseURI"' in go_template
    assert "name: CLICKHOUSE_URI" in go_template
    helpers = (_HELM_CHART / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    native_helper = helpers.split('define "dev-health.goWorkerClickhouseURI"')[1]
    assert "9000" in native_helper.split("{{- end }}")[0] or "9000" in native_helper
