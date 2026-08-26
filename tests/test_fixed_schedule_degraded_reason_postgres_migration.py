"""PostgreSQL proof for durable fixed-schedule degraded verdicts."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.engine.interfaces import ReflectedColumn

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "fixed_schedule_occurrences"
_COLUMN = "degraded_reason"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0099(
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

    database_name = f"test_chaos_3161_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0099")

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


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _columns(engine: Engine) -> dict[str, ReflectedColumn]:
    return {
        str(column["name"]): column for column in sa.inspect(engine).get_columns(_TABLE)
    }


def _seed_materialized_occurrence(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_TABLE} (
                    occurrence_key, identity_version, schedule_id, target_kind,
                    scheduled_for, observed_at, status, handoff_count,
                    skip_reason, completed_at, created_at, updated_at
                ) VALUES (
                    'legacy-evaluation', 'fixed_schedule_occurrence_v1',
                    'scheduled_reports_dispatch', 'report.execute_scheduled',
                    now(), now(), 'materialized', 1, NULL, now(), now(), now()
                )
                """
            )
        )


def test_0100_adds_nullable_bounded_degraded_reason_without_rewriting_history(
    migrated_to_0099: PostgresMigrationHarness,
) -> None:
    _seed_materialized_occurrence(migrated_to_0099.engine)
    command.upgrade(_migration_config(), "0100")

    column = _columns(migrated_to_0099.engine)[_COLUMN]
    assert isinstance(column["type"], postgresql.VARCHAR)
    assert column["type"].length == 64
    assert column["nullable"] is True

    with migrated_to_0099.engine.connect() as connection:
        legacy_reason = connection.execute(
            sa.text(
                f"SELECT {_COLUMN} FROM {_TABLE} WHERE occurrence_key = 'legacy-evaluation'"
            )
        ).scalar_one()
    assert legacy_reason is None
    assert _revisions(migrated_to_0099.engine) == {"0100"}


def test_0100_persists_a_degraded_verdict_alongside_materialized_work(
    migrated_to_0099: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0100")
    _seed_materialized_occurrence(migrated_to_0099.engine)

    with migrated_to_0099.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"UPDATE {_TABLE} SET {_COLUMN} = :reason WHERE occurrence_key = 'legacy-evaluation'"
            ),
            {"reason": "scheduled_reports_undeliverable"},
        )
    with migrated_to_0099.engine.connect() as connection:
        reason = connection.execute(
            sa.text(
                f"SELECT {_COLUMN} FROM {_TABLE} WHERE occurrence_key = 'legacy-evaluation'"
            )
        ).scalar_one()
    assert reason == "scheduled_reports_undeliverable"


def test_0100_downgrade_and_reupgrade_converge(
    migrated_to_0099: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0100")
    command.downgrade(_migration_config(), "0099")

    assert _COLUMN not in _columns(migrated_to_0099.engine)
    assert _revisions(migrated_to_0099.engine) == {"0099"}

    command.upgrade(_migration_config(), "application_schema@head")
    assert _COLUMN in _columns(migrated_to_0099.engine)
    assert _revisions(migrated_to_0099.engine) == {"0112"}
