"""PostgreSQL proof for durable worker-job delivery abandonment evidence."""

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

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "worker_job_delivery_abandonments"
_INDEX = "ix_worker_job_delivery_abandonments_kind_time"
_CHECK = "ck_worker_job_delivery_abandonments_attempt_count"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0098(
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

    database_name = f"test_chaos_3160_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0098")

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


def test_0099_adds_only_bounded_delivery_abandonment_evidence(
    migrated_to_0098: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0099")
    inspector = sa.inspect(migrated_to_0098.engine)

    columns = {str(column["name"]): column for column in inspector.get_columns(_TABLE)}
    assert set(columns) == {
        "dedupe_key",
        "job_kind",
        "abandoned_at",
        "attempt_count",
        "last_error_code",
    }
    assert isinstance(columns["dedupe_key"]["type"], postgresql.VARCHAR)
    assert columns["dedupe_key"]["type"].length == 256
    assert columns["dedupe_key"]["nullable"] is False
    assert isinstance(columns["job_kind"]["type"], postgresql.VARCHAR)
    assert columns["job_kind"]["type"].length == 96
    assert columns["job_kind"]["nullable"] is False
    assert isinstance(columns["abandoned_at"]["type"], postgresql.TIMESTAMP)
    assert columns["abandoned_at"]["type"].timezone is True
    assert columns["abandoned_at"]["nullable"] is False
    assert isinstance(columns["attempt_count"]["type"], sa.Integer)
    assert columns["attempt_count"]["nullable"] is False
    assert isinstance(columns["last_error_code"]["type"], postgresql.VARCHAR)
    assert columns["last_error_code"]["type"].length == 64
    assert columns["last_error_code"]["nullable"] is True

    assert inspector.get_pk_constraint(_TABLE)["constrained_columns"] == ["dedupe_key"]
    indexes = {
        str(index["name"]): tuple(str(column) for column in index["column_names"])
        for index in inspector.get_indexes(_TABLE)
    }
    assert indexes[_INDEX] == ("job_kind", "abandoned_at")
    checks = {
        str(check["name"]): str(check["sqltext"])
        for check in inspector.get_check_constraints(_TABLE)
    }
    assert set(checks) == {_CHECK}
    assert "attempt_count >= 0" in checks[_CHECK]
    assert _revisions(migrated_to_0098.engine) == {"0099"}


def test_0099_accepts_zero_attempt_history_and_rejects_negative_counts(
    migrated_to_0098: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0099")

    with migrated_to_0098.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_TABLE} (
                    dedupe_key, job_kind, abandoned_at,
                    attempt_count, last_error_code
                ) VALUES (
                    'report.run:zero', 'report.run', now(), 0, NULL
                )
                """
            )
        )

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0098.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_TABLE} (
                        dedupe_key, job_kind, abandoned_at,
                        attempt_count, last_error_code
                    ) VALUES (
                        'report.run:negative', 'report.run', now(), -1, 'invalid'
                    )
                    """
                )
            )


def test_0099_downgrade_and_reupgrade_converge(
    migrated_to_0098: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0099")
    command.downgrade(_migration_config(), "0098")

    assert not sa.inspect(migrated_to_0098.engine).has_table(_TABLE)
    assert _revisions(migrated_to_0098.engine) == {"0098"}

    command.upgrade(_migration_config(), "application_schema@head")
    assert sa.inspect(migrated_to_0098.engine).has_table(_TABLE)
    assert _revisions(migrated_to_0098.engine) == {"0107"}
