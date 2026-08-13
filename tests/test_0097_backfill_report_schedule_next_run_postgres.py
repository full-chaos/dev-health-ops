"""PostgreSQL proof for the recurring-report paging-marker backfill."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration() -> ModuleType:
    return import_module(
        "dev_health_ops.alembic.versions.0097_backfill_report_schedule_next_run"
    )


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0096(
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

    database_name = f"test_chaos_3159_{uuid.uuid4().hex}"
    admin_url = configured_url.set(
        drivername="postgresql+psycopg2",
        database="postgres",
    )
    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
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
        command.upgrade(_migration_config(), "0096")

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


def _seed_schedule(
    engine: Engine,
    *,
    cron: str,
    timezone_name: str,
    created_at: datetime,
    last_run_at: datetime | None = None,
    next_run_at: datetime | None = None,
    job_type: str = "report",
) -> uuid.UUID:
    schedule_id = uuid.uuid4()
    report_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO scheduled_jobs (
                    id, org_id, name, job_type, provider, schedule_cron,
                    timezone, job_config, status, next_run_at, created_at, updated_at
                ) VALUES (
                    :id, :org_id, :name, :job_type, '', :cron,
                    :timezone_name, '{}', 0, :next_run_at, :created_at, :created_at
                )
                """
            ),
            {
                "id": schedule_id,
                "org_id": f"chaos-3159-{schedule_id}",
                "name": f"schedule-{schedule_id}",
                "job_type": job_type,
                "cron": cron,
                "timezone_name": timezone_name,
                "next_run_at": next_run_at,
                "created_at": created_at,
            },
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO saved_reports (
                    id, org_id, name, report_plan, is_template, schedule_id,
                    is_active, last_run_at, created_at, updated_at
                ) VALUES (
                    :id, :org_id, :name, '{}', FALSE, :schedule_id,
                    TRUE, :last_run_at, :created_at, :created_at
                )
                """
            ),
            {
                "id": report_id,
                "org_id": f"chaos-3159-{schedule_id}",
                "name": f"report-{report_id}",
                "schedule_id": schedule_id,
                "last_run_at": last_run_at,
                "created_at": created_at,
            },
        )
    return schedule_id


def _next_run_at(engine: Engine, schedule_id: uuid.UUID) -> datetime | None:
    with engine.connect() as connection:
        return connection.execute(
            sa.text("SELECT next_run_at FROM scheduled_jobs WHERE id = :id"),
            {"id": schedule_id},
        ).scalar_one()


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def test_0097_backfills_exact_report_markers_and_leaves_other_jobs_unchanged(
    migrated_to_0096: PostgresMigrationHarness,
) -> None:
    stale = datetime(2099, 1, 1, tzinfo=UTC)
    never_run = _seed_schedule(
        migrated_to_0096.engine,
        cron="0 6 * * *",
        timezone_name="UTC",
        created_at=datetime(2026, 7, 25, 5, 30, tzinfo=UTC),
        next_run_at=stale,
    )
    previously_run = _seed_schedule(
        migrated_to_0096.engine,
        cron="30 1 * * *",
        timezone_name="America/Los_Angeles",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_run_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
    )
    other_job = _seed_schedule(
        migrated_to_0096.engine,
        cron="0 6 * * *",
        timezone_name="UTC",
        created_at=datetime(2026, 7, 25, 5, 30, tzinfo=UTC),
        next_run_at=stale,
        job_type="metrics",
    )

    command.upgrade(_migration_config(), "0097")

    assert _next_run_at(migrated_to_0096.engine, never_run) == datetime(
        2026, 7, 25, 6, tzinfo=UTC
    )
    assert _next_run_at(migrated_to_0096.engine, previously_run) == datetime(
        2026, 7, 26, 8, 30, tzinfo=UTC
    )
    assert _next_run_at(migrated_to_0096.engine, other_job) == stale
    assert _revisions(migrated_to_0096.engine) == {"0097"}

    command.downgrade(_migration_config(), "0096")
    assert _next_run_at(migrated_to_0096.engine, never_run) == datetime(
        2026, 7, 25, 6, tzinfo=UTC
    )
    assert _revisions(migrated_to_0096.engine) == {"0096"}


def test_0097_processes_more_than_one_bounded_marker_batch(
    migrated_to_0096: PostgresMigrationHarness,
) -> None:
    schedule_ids = [
        _seed_schedule(
            migrated_to_0096.engine,
            cron="0 6 * * *",
            timezone_name="UTC",
            created_at=datetime(2026, 7, 25, 5, 30, tzinfo=UTC),
        )
        for _ in range(5)
    ]

    migration = _migration()
    with migrated_to_0096.engine.begin() as connection:
        processed = migration._backfill_next_run_markers(connection, batch_size=2)

    assert processed == 5
    assert all(
        _next_run_at(migrated_to_0096.engine, schedule_id)
        == datetime(2026, 7, 25, 6, tzinfo=UTC)
        for schedule_id in schedule_ids
    )


def test_0097_refuses_an_unevaluable_legacy_cron_without_partial_backfill(
    migrated_to_0096: PostgresMigrationHarness,
) -> None:
    original = datetime(2099, 1, 1, tzinfo=UTC)
    valid = _seed_schedule(
        migrated_to_0096.engine,
        cron="0 6 * * *",
        timezone_name="UTC",
        created_at=datetime(2026, 7, 25, 5, 30, tzinfo=UTC),
        next_run_at=original,
    )
    invalid = _seed_schedule(
        migrated_to_0096.engine,
        cron="0 0 1 1 * 2027",
        timezone_name="UTC",
        created_at=datetime(2026, 7, 25, 5, 30, tzinfo=UTC),
        next_run_at=original,
    )

    with pytest.raises(RuntimeError, match=rf"scheduled job {invalid}.*five fields"):
        command.upgrade(_migration_config(), "0097")

    assert _next_run_at(migrated_to_0096.engine, valid) == original
    assert _next_run_at(migrated_to_0096.engine, invalid) == original
    assert _revisions(migrated_to_0096.engine) == {"0096"}
