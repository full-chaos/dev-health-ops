"""0125 promotes lingering celery-transport worker_job_routes rows to their
checked-in policy route, for the 18 kinds CHAOS-5320 moved off the celery
rollback route.

Real Postgres, not sqlite -- unlike the in-memory-schema migration tests
elsewhere in this suite, this proves the migration against the actual
alembic chain up through its own down_revision (0124), matching
test_0066_celery_river_cutover_postgres.py's pattern for the sibling
wholesale cutover this migration completes the retirement of.
"""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_MODULE = (
    "dev_health_ops.alembic.versions.0125_promote_worker_job_routes_off_celery_rollback"
)


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


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


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


@pytest.fixture
def migrated_to_0124(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ[_POSTGRES_URI_ENV]
    configured_url = make_url(configured_uri)
    assert configured_url.get_backend_name() == "postgresql"
    database_name = f"test_0125_{uuid.uuid4().hex}"
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
        monkeypatch.setenv("DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "1")
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "0124")
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


def _set_route(
    engine: Engine, kind: str, transport: str, paused: bool, generation: int
) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE worker_job_routes SET transport = :t, paused = :p, generation = :g "
                "WHERE job_kind = :k"
            ),
            {"t": transport, "p": paused, "g": generation, "k": kind},
        )


def _route(engine: Engine, kind: str) -> tuple[str, bool, int]:
    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT transport, paused, generation FROM worker_job_routes WHERE job_kind = :k"
            ),
            {"k": kind},
        ).one()
        return (str(row[0]), bool(row[1]), int(row[2]))


def test_0125_promotes_celery_rows_for_listed_kinds_and_leaves_the_rest(
    migrated_to_0124: PostgresMigrationHarness,
) -> None:
    migration = _migration()
    engine = migrated_to_0124.engine

    # Every row's starting state is pinned explicitly below rather than
    # trusted from wherever the real chain happens to leave it (0066 sits on
    # a SEPARATE alembic branch from 0125's own down_revision chain, so
    # upgrading straight to 0124 does not imply 0066 ran -- confirmed
    # empirically). This simulates the actual production risk 0125 exists
    # for: a row an operator rolled back to celery at some point, sitting
    # there indefinitely with no executor left to process it.
    listed_kind = "operational.webhook_delivery"
    assert listed_kind in migration._KIND_TARGET_ROUTE
    _set_route(engine, listed_kind, "celery", False, 7)

    paused_listed_kind = "system.heartbeat"
    assert paused_listed_kind in migration._KIND_TARGET_ROUTE
    _set_route(engine, paused_listed_kind, "celery", True, 3)

    already_promoted_kind = "operational.billing_notification"
    assert already_promoted_kind in migration._KIND_TARGET_ROUTE
    _set_route(engine, already_promoted_kind, "river", False, 6)
    before_already_promoted = _route(engine, already_promoted_kind)

    unlisted_kind = "sync.provider_unit"
    assert unlisted_kind not in migration._KIND_TARGET_ROUTE
    _set_route(engine, unlisted_kind, "celery", False, 1)

    command.upgrade(_migration_config(), "0125")

    # Direction 1: a seeded celery row for a listed kind is promoted, with
    # generation bumped and paused state preserved.
    assert _route(engine, listed_kind) == ("river", False, 8)
    assert _route(engine, paused_listed_kind) == ("river", True, 4)

    # Direction 2: a row that was never on celery is left byte-for-byte
    # untouched (no spurious generation bump on a no-op match).
    assert _route(engine, already_promoted_kind) == before_already_promoted

    # Direction 2 continued: a kind NOT in this migration's map (0107 already
    # owns sync.provider_unit's promotion) is left untouched even though its
    # transport happens to be celery.
    assert _route(engine, unlisted_kind) == ("celery", False, 1)


def test_0125_downgrade_is_a_documented_no_op(
    migrated_to_0124: PostgresMigrationHarness,
) -> None:
    migration = _migration()
    engine = migrated_to_0124.engine

    kind = "workgraph.build"
    assert kind in migration._KIND_TARGET_ROUTE
    _set_route(engine, kind, "celery", False, 5)

    command.upgrade(_migration_config(), "0125")
    after_upgrade = _route(engine, kind)
    assert after_upgrade == ("river", False, 6)

    command.downgrade(_migration_config(), "0124")

    assert _route(engine, kind) == after_upgrade
