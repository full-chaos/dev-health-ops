from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROFILES = _REPO_ROOT / "deploy" / "go-workers" / "profiles.json"
_PRODUCTION_COMPOSE = (
    _REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml"
)
_SWARM_STACK = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml"
_KUBERNETES = _REPO_ROOT / "deploy" / "kubernetes"
_HELM_CHART = _REPO_ROOT / "deploy" / "helm" / "dev-health"

_MIGRATION_CONFIG_DEFAULTS = {
    "RIVER_DATABASE_SCHEMA": "river",
    "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
    "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
}
_FORBIDDEN_SHARED_MIGRATION_SECRETS = {
    "MIGRATION_DATABASE_URI",
    "MIGRATION_DATABASE_URI_FILE",
}


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


def test_go_profiles_are_disabled_future_topology() -> None:
    manifest = _load_json(_PROFILES)

    assert manifest["deployment_state"] == "coexistence_disabled"
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
    operator = manifest["operator_cli"]
    assert operator == {
        "name": "worker-operator",
        "binary": "dev-health-workerctl",
        "max_concurrent_invocations": 1,
        "queue_control_max_connections": 2,
        "domain_max_connections": 2,
        "config_env": [
            "PGBOUNCER_TRANSACTION_MODE",
            "RIVER_DATABASE_SCHEMA",
            "WORKER_DATABASE_MODE",
        ],
        "secret_env": [
            "POSTGRES_URI",
            "WORKER_DATABASE_URI",
            "WORKER_OPERATOR_TOKEN",
        ],
    }


def test_profile_pgbouncer_budget_matches_production_compose_defaults() -> None:
    manifest = _load_json(_PROFILES)
    pgbouncer = _load_yaml(_PRODUCTION_COMPOSE)["services"]["pgbouncer"]

    assert pgbouncer["profiles"] == ["pooler"]
    environment = pgbouncer["environment"]
    assert manifest["postgres_budget"]["pgbouncer_max_client_connections"] == (
        _compose_default(environment["MAX_CLIENT_CONN"], "PGBOUNCER_MAX_CLIENT_CONN")
    )
    assert manifest["postgres_budget"]["pgbouncer_default_pool_size"] == (
        _compose_default(
            environment["DEFAULT_POOL_SIZE"], "PGBOUNCER_DEFAULT_POOL_SIZE"
        )
    )
    # Existing Celery/application traffic and the new Go domain role create
    # distinct (database,user) server pools in PgBouncer.
    assert manifest["postgres_budget"]["pgbouncer_server_pool_count"] == 2


@pytest.mark.parametrize("path", [_PRODUCTION_COMPOSE, _SWARM_STACK])
def test_compose_and_swarm_migration_wiring_matches_contract(path: Path) -> None:
    manifest = _load_json(_PROFILES)
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
    manifest = _load_json(_PROFILES)
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
    manifest = _load_json(_PROFILES)
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
            "postgresql://migration@direct/app",
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
