from __future__ import annotations

from pathlib import Path

import yaml

_CUTOVER_ENV = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"
_REPO_ROOT = Path(__file__).parents[1]


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
