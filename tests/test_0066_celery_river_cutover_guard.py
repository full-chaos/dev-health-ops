from __future__ import annotations

import importlib
from pathlib import Path

import pytest
import yaml

_CUTOVER_ENV = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"
_REPO_ROOT = Path(__file__).parents[1]
_POSTGRES_TEST_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


def test_migration_deployments_pass_the_cutover_opt_in_to_alembic() -> None:
    compose_paths = (
        _REPO_ROOT / "compose.yml",
        _REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml",
        _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml",
    )
    for compose_path in compose_paths:
        services = yaml.safe_load(compose_path.read_text(encoding="utf-8"))["services"]
        assert services["migrate"]["environment"][_CUTOVER_ENV] == (
            "${DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER:-}"
        )

    manifests = list(
        yaml.safe_load_all(
            (_REPO_ROOT / "deploy" / "kubernetes" / "migrate-job.yaml").read_text(
                encoding="utf-8"
            )
        )
    )
    job = next(manifest for manifest in manifests if manifest["kind"] == "Job")
    env = job["spec"]["template"]["spec"]["containers"][0]["env"]
    assert {
        "name": _CUTOVER_ENV,
        "valueFrom": {
            "secretKeyRef": {
                "name": "dev-health-migration-secrets",
                "key": _CUTOVER_ENV,
                "optional": True,
            }
        },
    } in env


def test_gitlab_unit_job_enables_real_postgres_migration_tests() -> None:
    pipeline = yaml.safe_load(
        (_REPO_ROOT / ".gitlab-ci.yml").read_text(encoding="utf-8")
    )
    assert pipeline["test"]["variables"]["DEV_HEALTH_POSTGRES_TEST_URI"] == (
        "postgresql+asyncpg://postgres:postgres@postgres:5432/test_db"
    )


def test_github_unit_job_enables_real_postgres_migration_tests() -> None:
    workflow = yaml.safe_load(
        (_REPO_ROOT / ".github" / "workflows" / "test.yml").read_text(encoding="utf-8")
    )
    postgres_step = next(
        step
        for step in workflow["jobs"]["test-matrix"]["steps"]
        if step.get("name") == "Run PostgreSQL migration tests"
    )
    assert postgres_step["env"][_POSTGRES_TEST_URI_ENV] == (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db"
    )
    assert "tests/test_0066_celery_river_cutover_postgres.py" in postgres_step["run"]

    unit_step = next(
        step
        for step in workflow["jobs"]["test-matrix"]["steps"]
        if step.get("name") == "Run parallel unit test contract"
    )
    assert (
        "--ignore=tests/test_0066_celery_river_cutover_postgres.py"
        in (unit_step["env"]["PYTEST_ADDOPTS"])
    )

    coverage_step = next(
        step
        for step in workflow["jobs"]["coverage"]["steps"]
        if step.get("name") == "Run coverage-gated test contract"
    )
    assert coverage_step["env"][_POSTGRES_TEST_URI_ENV] == (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db"
    )


def test_live_e2e_allows_the_disposable_cutover_migration() -> None:
    workflow = yaml.safe_load(
        (_REPO_ROOT / ".github" / "workflows" / "live-e2e.yml").read_text(
            encoding="utf-8"
        )
    )
    live_e2e_step = next(
        step
        for step in workflow["jobs"]["live-e2e"]["steps"]
        if step.get("name") == "Run live-e2e tier"
    )
    assert live_e2e_step["env"][_CUTOVER_ENV] == "1"


def test_missing_postgres_test_uri_fails_in_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    postgres_tests = importlib.import_module(
        "tests.test_0066_celery_river_cutover_postgres"
    )
    monkeypatch.delenv(_POSTGRES_TEST_URI_ENV, raising=False)
    monkeypatch.setenv("CI", "true")

    with pytest.raises(pytest.fail.Exception, match=_POSTGRES_TEST_URI_ENV):
        postgres_tests._require_postgres_test_uri()


@pytest.mark.parametrize(
    "module_name",
    (
        "tests.test_dispatch_outbox",
        "tests.test_sync_reconciler",
        "tests.test_sync_planner",
        "tests.test_service_credentials_cli",
    ),
)
def test_known_postgres_tests_fail_in_ci_without_uri(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
) -> None:
    postgres_tests = importlib.import_module(module_name)
    monkeypatch.delenv(_POSTGRES_TEST_URI_ENV, raising=False)
    monkeypatch.setenv("CI", "true")

    with pytest.raises(pytest.fail.Exception, match=_POSTGRES_TEST_URI_ENV):
        postgres_tests._require_postgres_test_uri()
