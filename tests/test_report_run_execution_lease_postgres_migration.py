"""PostgreSQL proof for durable report execution leases."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

from dev_health_ops.models.reports import ReportRun
from dev_health_ops.reports.export import start_report_run

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_INDEX = "ix_report_runs_execution_reclaim"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0097(
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

    database_name = f"test_chaos_3158_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(
            drivername="postgresql+psycopg2",
            database="postgres",
        ),
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
        command.upgrade(_migration_config(), "0097")

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


def _column_map(engine: Engine) -> dict[str, Any]:
    return {
        str(column["name"]): column
        for column in sa.inspect(engine).get_columns("report_runs")
    }


def _index_map(engine: Engine) -> dict[str, tuple[str, ...]]:
    return {
        str(index["name"]): tuple(str(column) for column in index["column_names"])
        for index in sa.inspect(engine).get_indexes("report_runs")
    }


def _seed_legacy_running_run(engine: Engine) -> tuple[uuid.UUID, uuid.UUID]:
    report_id = uuid.uuid4()
    run_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO saved_reports (
                    id, org_id, name, report_plan, is_template, is_active,
                    created_at, updated_at
                ) VALUES (
                    :id, 'chaos-3158', :name, '{}', FALSE, TRUE, now(), now()
                )
                """
            ),
            {"id": report_id, "name": f"report-{report_id}"},
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO report_runs (
                    id, report_id, status, started_at, attempt_count,
                    notification_status, triggered_by, created_at
                ) VALUES (
                    :id, :report_id, 'running', now(), 1,
                    'pending', 'scheduler', now()
                )
                """
            ),
            {"id": run_id, "report_id": report_id},
        )
    return report_id, run_id


def test_0098_adds_exact_execution_lease_schema(
    migrated_to_0097: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0098")

    columns = _column_map(migrated_to_0097.engine)
    token = columns["execution_claim_token"]
    lease = columns["execution_lease_expires_at"]
    count = columns["execution_reclaim_count"]

    assert isinstance(token["type"], postgresql.UUID)
    assert token["nullable"] is True
    assert isinstance(lease["type"], postgresql.TIMESTAMP)
    assert lease["type"].timezone is True
    assert lease["nullable"] is True
    assert isinstance(count["type"], sa.Integer)
    assert count["nullable"] is False
    assert str(count["default"]) == "0"
    assert _index_map(migrated_to_0097.engine)[_INDEX] == (
        "status",
        "execution_lease_expires_at",
    )
    assert _revisions(migrated_to_0097.engine) == {"0098"}


def test_0098_makes_a_legacy_running_run_immediately_reclaimable(
    migrated_to_0097: PostgresMigrationHarness,
) -> None:
    _, run_id = _seed_legacy_running_run(migrated_to_0097.engine)
    command.upgrade(_migration_config(), "0098")

    with migrated_to_0097.engine.connect() as connection:
        row = connection.execute(
            sa.text(
                """
                SELECT execution_claim_token, execution_lease_expires_at,
                       execution_reclaim_count
                FROM report_runs
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id},
        ).one()
    assert row == (None, None, 0)

    with Session(migrated_to_0097.engine) as session:
        claim = start_report_run(session, str(run_id))
        session.commit()
    assert claim is not None
    assert claim.reclaimed is True

    with Session(migrated_to_0097.engine) as session:
        run = session.get(ReportRun, run_id)
        assert run is not None
        assert run.id == run_id
        assert run.execution_claim_token == claim.token
        assert run.execution_reclaim_count == 1
        assert run.execution_lease_expires_at is not None
        assert run.execution_lease_expires_at > datetime.now(UTC)


def test_0098_downgrade_and_reupgrade_converge(
    migrated_to_0097: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0098")
    command.downgrade(_migration_config(), "0097")

    columns = _column_map(migrated_to_0097.engine)
    assert "execution_claim_token" not in columns
    assert "execution_lease_expires_at" not in columns
    assert "execution_reclaim_count" not in columns
    assert _INDEX not in _index_map(migrated_to_0097.engine)
    assert _revisions(migrated_to_0097.engine) == {"0097"}

    command.upgrade(_migration_config(), "application_schema@head")
    assert _INDEX in _index_map(migrated_to_0097.engine)
    assert _revisions(migrated_to_0097.engine) == {"0102"}
