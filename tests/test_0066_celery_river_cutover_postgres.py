from __future__ import annotations

import importlib
import os
import uuid
from argparse import Namespace
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

_CUTOVER_ENV = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"
_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_MODULE = "dev_health_ops.alembic.versions.0066_activate_river_worker_job_routes"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def _require_postgres_test_uri() -> None:
    if os.getenv(_POSTGRES_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(
            f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
        )
    pytest.skip(f"requires {_POSTGRES_URI_ENV}")


@pytest.fixture(autouse=True, scope="module")
def require_postgres_test_uri() -> None:
    _require_postgres_test_uri()


@pytest.fixture
def migrated_to_0065(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ[_POSTGRES_URI_ENV]
    configured_url = make_url(configured_uri)
    assert configured_url.get_backend_name() == "postgresql"
    database_name = f"test_0066_{uuid.uuid4().hex}"
    admin_url = configured_url.set(
        drivername="postgresql+psycopg2", database="postgres"
    )
    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    database_created = False
    engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True
        async_url = configured_url.set(
            drivername="postgresql+asyncpg", database=database_name
        )
        monkeypatch.setenv(
            "POSTGRES_URI", async_url.render_as_string(hide_password=False)
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "0065")
        engine = sa.create_engine(
            configured_url.set(drivername="postgresql+psycopg2", database=database_name)
        )
        yield PostgresMigrationHarness(engine=engine)
    finally:
        if engine is not None:
            engine.dispose()
        if database_created:
            with admin_engine.connect() as connection:
                connection.execute(
                    sa.text(
                        """
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = :database_name
                          AND pid <> pg_backend_pid()
                        """
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def _revision(engine: Engine) -> str:
    with engine.connect() as connection:
        return str(
            connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        )


def _routes(engine: Engine, migration: ModuleType) -> list[tuple[str, str, bool, int]]:
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                """
                SELECT job_kind, transport, paused, generation
                FROM worker_job_routes
                WHERE job_kind = ANY(:kinds)
                ORDER BY job_kind
                """
            ),
            {"kinds": list(migration._KINDS)},
        )
        return [(str(row[0]), str(row[1]), bool(row[2]), int(row[3])) for row in rows]


def test_0066_real_postgres_refuses_without_opt_in_without_advancing_revision(
    migrated_to_0065: PostgresMigrationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    before = _routes(migrated_to_0065.engine, migration)
    monkeypatch.delenv(_CUTOVER_ENV, raising=False)

    with pytest.raises(RuntimeError, match=f"{_CUTOVER_ENV}=1"):
        command.upgrade(_migration_config(), "0066")

    assert _revision(migrated_to_0065.engine) == "0065"
    assert _routes(migrated_to_0065.engine, migration) == before


def test_application_migrator_defers_0066_without_opt_in(
    migrated_to_0065: PostgresMigrationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    before = _routes(migrated_to_0065.engine, migration)
    monkeypatch.delenv(_CUTOVER_ENV, raising=False)

    from dev_health_ops.migrate import _run_upgrade

    assert _run_upgrade(Namespace(db=None, revision="head")) == 0

    assert _revision(migrated_to_0065.engine) == "0065"
    assert _routes(migrated_to_0065.engine, migration) == before


def test_0066_real_postgres_applies_only_with_opt_in_and_downgrades(
    migrated_to_0065: PostgresMigrationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    monkeypatch.setenv(_CUTOVER_ENV, "1")

    command.upgrade(_migration_config(), "0066")

    assert _revision(migrated_to_0065.engine) == "0066"
    assert _routes(migrated_to_0065.engine, migration) == [
        (kind, "river", False, 2) for kind in sorted(migration._KINDS)
    ]

    monkeypatch.delenv(_CUTOVER_ENV, raising=False)
    from dev_health_ops.migrate import _run_upgrade

    assert _run_upgrade(Namespace(db=None, revision="head")) == 0
    assert _revision(migrated_to_0065.engine) == "0071"
    assert _routes(migrated_to_0065.engine, migration) == [
        (kind, "river", False, 2) for kind in sorted(migration._KINDS)
    ]

    command.downgrade(_migration_config(), "0065")

    assert _revision(migrated_to_0065.engine) == "0065"
    assert _routes(migrated_to_0065.engine, migration) == [
        (kind, "celery", False, 3) for kind in sorted(migration._KINDS)
    ]


def test_0066_real_postgres_locks_routes_before_retargeting(
    migrated_to_0065: PostgresMigrationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _migration()
    monkeypatch.setenv(_CUTOVER_ENV, "1")
    original_existing_rows = migration._existing_rows
    routes_locked = Event()
    writer_attempted = Event()
    writer_committed = Event()

    def hold_locks(routes):
        rows = original_existing_rows(routes)
        routes_locked.set()
        assert writer_attempted.wait(timeout=10)
        assert not writer_committed.wait(timeout=0.5)
        return rows

    monkeypatch.setattr(migration, "_existing_rows", hold_locks)

    def apply_migration() -> None:
        with Session(migrated_to_0065.engine) as session:
            context = MigrationContext.configure(session.connection())
            with Operations.context(context):
                migration.upgrade()
            session.commit()

    def update_locked_route() -> None:
        assert routes_locked.wait(timeout=10)
        with Session(migrated_to_0065.engine) as session:
            writer_attempted.set()
            session.execute(
                sa.text(
                    """
                    UPDATE worker_job_routes
                    SET transport = 'shadow'
                    WHERE job_kind = :kind
                    """
                ),
                {"kind": "workgraph.build"},
            )
            session.commit()
        writer_committed.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        migration_future = executor.submit(apply_migration)
        writer_future = executor.submit(update_locked_route)
        migration_future.result(timeout=30)
        writer_future.result(timeout=30)

    assert writer_committed.is_set()
