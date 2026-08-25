"""PostgreSQL proof for alembic 0110's extra_metrics/team_metrics retirement.

CHAOS-4243 retired metrics.remaining.extra_metrics/team_metrics on the premise
that no producer ever enqueued either kind. That premise is asserted here,
not assumed: 0110 refuses to delete the two worker_job_routes rows or narrow
remaining_metric_runs' family CHECK constraint if it finds ANY river.river_job
row (any state) for either kind -- such a row surviving would mean something
DID enqueue it, and 0110 would otherwise silently orphan that evidence.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = "src/dev_health_ops/alembic"
_RETIRED_KINDS = (
    "metrics.remaining.extra_metrics",
    "metrics.remaining.team_metrics",
)


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", _ALEMBIC_DIR)
    return config


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _routes(engine: Engine) -> dict[str, tuple[str, bool, int]]:
    with engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT job_kind, transport, paused, generation FROM worker_job_routes "
                "WHERE job_kind = ANY(:kinds)"
            ),
            {"kinds": list(_RETIRED_KINDS)},
        ).all()
    return {row.job_kind: (row.transport, row.paused, row.generation) for row in rows}


def _family_check(engine: Engine) -> str:
    with engine.connect() as connection:
        return str(
            connection.execute(
                sa.text(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conname = 'ck_remaining_metric_run_family'"
                )
            ).scalar_one()
        )


@pytest.fixture
def migrated_to_0109(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
            )
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4243_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    database_created = False
    engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True

        monkeypatch.setenv(
            "POSTGRES_URI",
            configured_url.set(
                drivername="postgresql+asyncpg",
                database=database_name,
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "0109")

        engine = sa.create_engine(
            configured_url.set(
                drivername="postgresql+psycopg2",
                database=database_name,
            )
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


def test_0110_deletes_routes_and_narrows_family_check_when_no_river_job_table_exists(
    migrated_to_0109: PostgresMigrationHarness,
) -> None:
    # A fresh database (Alembic-migrated only, never touched by River's own
    # Go migration tool) has no river.river_job table at all -- the guard
    # must treat that as trivially zero rows, not an error.
    assert not sa.inspect(migrated_to_0109.engine).has_table(
        "river_job", schema="river"
    )
    before = _routes(migrated_to_0109.engine)
    assert set(before) == set(_RETIRED_KINDS)

    command.upgrade(_migration_config(), "0110")

    assert _routes(migrated_to_0109.engine) == {}
    assert "extra_metrics" not in _family_check(migrated_to_0109.engine)
    assert "team_metrics" not in _family_check(migrated_to_0109.engine)
    assert _revisions(migrated_to_0109.engine) == {"0110"}


def test_0110_refuses_when_a_river_job_row_exists_for_a_retired_kind(
    migrated_to_0109: PostgresMigrationHarness,
) -> None:
    with migrated_to_0109.engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE SCHEMA river; CREATE TABLE river.river_job "
                "(id bigint PRIMARY KEY, kind text NOT NULL, state text NOT NULL)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO river.river_job (id, kind, state) "
                "VALUES (1, :kind, 'completed')"
            ),
            {"kind": _RETIRED_KINDS[0]},
        )

    with pytest.raises(RuntimeError, match=_RETIRED_KINDS[0]):
        command.upgrade(_migration_config(), "0110")

    # A refused migration must not have partially applied.
    assert _revisions(migrated_to_0109.engine) == {"0109"}
    assert set(_routes(migrated_to_0109.engine)) == set(_RETIRED_KINDS)

    with migrated_to_0109.engine.begin() as connection:
        connection.execute(
            sa.text("DELETE FROM river.river_job WHERE kind = :kind"),
            {"kind": _RETIRED_KINDS[0]},
        )

    command.upgrade(_migration_config(), "0110")
    assert _revisions(migrated_to_0109.engine) == {"0110"}
    assert _routes(migrated_to_0109.engine) == {}


def test_0110_downgrade_restores_routes_and_the_wide_family_check(
    migrated_to_0109: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0110")
    assert _routes(migrated_to_0109.engine) == {}

    command.downgrade(_migration_config(), "0109")

    restored = _routes(migrated_to_0109.engine)
    assert restored == {
        "metrics.remaining.extra_metrics": ("celery", False, 1),
        "metrics.remaining.team_metrics": ("celery", False, 1),
    }
    assert "extra_metrics" in _family_check(migrated_to_0109.engine)
    assert "team_metrics" in _family_check(migrated_to_0109.engine)
    assert _revisions(migrated_to_0109.engine) == {"0109"}
