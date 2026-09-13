"""0132 retires the Celery rollback route for the four sync-dispatch kinds.

Real Postgres, not sqlite -- ``op.drop_constraint``/``op.create_check_-
constraint`` on ``sync_dispatch_transport_routes`` are DDL alterations
sqlite cannot apply outside alembic's batch mode, and this migration does
not use batch mode (there is exactly one production dialect for this
table). Matches ``test_0125_promote_worker_job_routes_off_celery_postgres.py``'s
isolated-database harness pattern rather than
``test_provider_unit_route_promotion_off_canary_migration.py``'s in-memory
sqlite one, which only works because that migration's own body is portable
SQLAlchemy Core with no DDL.
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

from tests._alembic_heads import application_schema_head

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_MODULE = (
    "dev_health_ops.alembic.versions."
    "0132_retire_sync_dispatch_transport_route_celery_rollback"
)
_KINDS = (
    "dispatch_sync_run",
    "finalize_sync_run",
    "post_sync",
    "reference_discovery",
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
def migrated_to_0131(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_url = make_url(os.environ[_POSTGRES_URI_ENV])
    assert configured_url.get_backend_name() in ("postgresql", "postgresql+asyncpg")
    database_name = f"test_0132_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0131")
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


def _routes(engine: Engine) -> dict[str, tuple[str, int, str]]:
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT kind, transport, generation, rollback_transport "
                "FROM sync_dispatch_transport_routes ORDER BY kind"
            )
        ).all()
        return {row[0]: (str(row[1]), int(row[2]), str(row[3])) for row in rows}


def test_0132_is_the_application_schema_head_and_chains_after_0131() -> None:
    """Derived, not typed. See ``tests/_alembic_heads.py``'s own docstring:
    the next migration author renumbers THIS check (or supersedes it) rather
    than leaving a stale pin.
    """
    migration = _migration()
    assert migration.revision == application_schema_head(), (
        "0132 must be the application_schema head; if another migration "
        "landed first, renumber this one and re-run"
    )
    assert migration.down_revision == "0131"


def test_0132_retires_rollback_transport_and_bumps_generation(
    migrated_to_0131: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0131.engine

    # 0049's untouched seed: every kind on celery/celery, generation 1,
    # exactly the production risk this migration closes -- a row nothing
    # ever cut over that still declares a rollback target no Celery
    # producer serves.
    before = _routes(engine)
    for kind in _KINDS:
        assert before[kind] == ("celery", 1, "celery")

    command.upgrade(_migration_config(), "0132")

    after = _routes(engine)
    for kind in _KINDS:
        transport, generation, rollback = after[kind]
        assert rollback == "none"
        assert generation == 2
        # transport itself is untouched by this migration -- only the
        # rollback declaration and the generation fence around it move.
        assert transport == "celery"


def test_0132_leaves_a_row_already_on_none_untouched(
    migrated_to_0131: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0131.engine

    # Run the migration once so the widened constraint is in place, then
    # hand-set a row to an already-"none" state at an arbitrary generation --
    # proving the upgrade's WHERE clause keys off rollback_transport, not a
    # generation threshold, before re-running it.
    command.upgrade(_migration_config(), "0132")
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE sync_dispatch_transport_routes "
                "SET transport = 'river', rollback_transport = 'none', generation = 9 "
                "WHERE kind = 'post_sync'"
            )
        )
    before = _routes(engine)["post_sync"]

    command.upgrade(_migration_config(), "0132")

    assert _routes(engine)["post_sync"] == before


def test_0132_is_idempotent(migrated_to_0131: PostgresMigrationHarness) -> None:
    engine = migrated_to_0131.engine

    command.upgrade(_migration_config(), "0132")
    settled = _routes(engine)

    command.upgrade(_migration_config(), "0132")
    assert _routes(engine) == settled


def test_0132_widens_the_check_constraint_rather_than_narrowing_it(
    migrated_to_0131: PostgresMigrationHarness,
) -> None:
    """A hard ``= 'none'`` constraint would break the Postgres-backed Python
    tests that hand-construct ``transport='celery', rollback_transport=
    'celery'`` rows via ``Base.metadata.create_all`` to exercise claim/lock/
    pause mechanics generically. The widened constraint must still accept
    that legacy pair after this migration ships.
    """
    engine = migrated_to_0131.engine
    command.upgrade(_migration_config(), "0132")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE sync_dispatch_transport_routes "
                "SET rollback_transport = 'celery', generation = generation + 1 "
                "WHERE kind = 'post_sync'"
            )
        )
    assert _routes(engine)["post_sync"][2] == "celery"

    with pytest.raises(sa.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "UPDATE sync_dispatch_transport_routes "
                    "SET rollback_transport = 'river_canary', generation = generation + 1 "
                    "WHERE kind = 'post_sync'"
                )
            )


def test_0132_downgrade_is_a_documented_no_op(
    migrated_to_0131: PostgresMigrationHarness,
) -> None:
    """A deliberate exception to the reversible-downgrade rule, pinned here.

    ``internal/syncroute`` resolves every row's expected rollback transport
    from the checked-in contract, which no longer declares ``celery`` for
    any of these four kinds -- reintroducing it here would not restore a
    usable rollback path. This test exists so "does nothing" stays a
    decision rather than decaying into a forgotten stub someone later
    "fixes".
    """
    engine = migrated_to_0131.engine
    command.upgrade(_migration_config(), "0132")
    after_upgrade = _routes(engine)

    command.downgrade(_migration_config(), "0131")

    assert _routes(engine) == after_upgrade
