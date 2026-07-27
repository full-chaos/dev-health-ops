from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType

import pytest
import sqlalchemy as sa
import yaml
from alembic.migration import MigrationContext
from alembic.operations import Operations

_CUTOVER_ENV = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"
_CANARY_KIND = "sync.provider_unit"
_REPO_ROOT = Path(__file__).parents[1]


def _migration() -> ModuleType:
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0066_activate_river_worker_job_routes"
    )


def _create_pre_0066_state(connection: sa.Connection, migration: ModuleType) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)
            """
        )
    )
    connection.execute(sa.text("INSERT INTO alembic_version VALUES ('0065')"))
    connection.execute(
        sa.text(
            """
            CREATE TABLE worker_job_routes (
                job_kind TEXT PRIMARY KEY,
                transport TEXT NOT NULL,
                paused BOOLEAN NOT NULL,
                generation BIGINT NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
    )
    route_rows = [
        {
            "job_kind": kind,
            "transport": "celery",
            "paused": False,
            "generation": 1,
        }
        for kind in migration._KINDS
    ]
    route_rows.append(
        {
            "job_kind": _CANARY_KIND,
            "transport": "river_canary",
            "paused": False,
            "generation": 1,
        }
    )
    connection.execute(
        sa.text(
            """
            INSERT INTO worker_job_routes
                (job_kind, transport, paused, generation, updated_at)
            VALUES
                (:job_kind, :transport, :paused, :generation, CURRENT_TIMESTAMP)
            """
        ),
        route_rows,
    )


def _apply_and_record_revision(
    connection: sa.Connection, migration: ModuleType
) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        migration.upgrade()
    connection.execute(sa.text("UPDATE alembic_version SET version_num = '0066'"))


def _route_rows(connection: sa.Connection) -> list[tuple[str, str, int, int]]:
    rows = connection.execute(
        sa.text(
            """
            SELECT job_kind, transport, paused, generation
            FROM worker_job_routes
            ORDER BY job_kind
            """
        )
    )
    return [(str(row[0]), str(row[1]), int(row[2]), int(row[3])) for row in rows]


def test_0066_bring_up_stops_at_0065_without_explicit_cutover_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0066_state(connection, migration)
            before = _route_rows(connection)
            monkeypatch.delenv(_CUTOVER_ENV, raising=False)

            with pytest.raises(RuntimeError, match=f"{_CUTOVER_ENV}=1"):
                _apply_and_record_revision(connection, migration)

            assert (
                connection.execute(
                    sa.text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0065"
            )
            assert _route_rows(connection) == before
    finally:
        engine.dispose()


def test_0066_retargets_routes_only_with_explicit_cutover_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0066_state(connection, migration)
            monkeypatch.setenv(_CUTOVER_ENV, "1")

            _apply_and_record_revision(connection, migration)

            assert (
                connection.execute(
                    sa.text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0066"
            )
            rows = _route_rows(connection)
            retargeted = [row for row in rows if row[0] in migration._KINDS]
            assert retargeted == [
                (kind, "river", 0, 2) for kind in sorted(migration._KINDS)
            ]
            assert (_CANARY_KIND, "river_canary", 0, 1) in rows
    finally:
        engine.dispose()


def test_0066_rejects_non_explicit_opt_in_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0066_state(connection, migration)
            monkeypatch.setenv(_CUTOVER_ENV, "true")

            with pytest.raises(RuntimeError, match=f"{_CUTOVER_ENV}=1"):
                _apply_and_record_revision(connection, migration)

            assert (
                connection.execute(
                    sa.text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0065"
            )
            assert all(row[1] != "river" for row in _route_rows(connection))
    finally:
        engine.dispose()


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
