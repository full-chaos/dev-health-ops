"""PostgreSQL proof for the saved-report schedule ownership invariant."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import IntegrityError

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration() -> ModuleType:
    from importlib import import_module

    return import_module(
        "dev_health_ops.alembic.versions.0096_enforce_unique_saved_report_schedule"
    )


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def application_head_postgres(
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

    database_name = f"test_chaos_3150_{uuid.uuid4().hex}"
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

        async_url = configured_url.set(
            drivername="postgresql+asyncpg",
            database=database_name,
        )
        monkeypatch.setenv(
            "POSTGRES_URI", async_url.render_as_string(hide_password=False)
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "application_schema@head")

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


def _seed_schedule_and_report(engine: Engine) -> tuple[uuid.UUID, uuid.UUID]:
    schedule_id = uuid.uuid4()
    report_id = uuid.uuid4()
    now = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO scheduled_jobs (
                    id, org_id, name, job_type, provider, schedule_cron,
                    timezone, job_config, status, created_at, updated_at
                ) VALUES (
                    :id, :org_id, :name, 'report', '', '0 6 * * *',
                    'UTC', '{}', 0, :now, :now
                )
                """
            ),
            {
                "id": schedule_id,
                "org_id": "chaos-3150",
                "name": f"schedule-{schedule_id}",
                "now": now,
            },
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO saved_reports (
                    id, org_id, name, report_plan, is_template, schedule_id,
                    is_active, created_at, updated_at
                ) VALUES (
                    :id, :org_id, :name, '{}', FALSE, :schedule_id,
                    TRUE, :now, :now
                )
                """
            ),
            {
                "id": report_id,
                "org_id": "chaos-3150",
                "name": f"report-{report_id}",
                "schedule_id": schedule_id,
                "now": now,
            },
        )
    return schedule_id, report_id


def _apply_upgrade(connection: Connection) -> None:
    migration = _migration()
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        migration.upgrade()


def _constraint_names(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(constraint["name"])
            for constraint in sa.inspect(connection).get_unique_constraints(
                "saved_reports"
            )
        }


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def test_application_head_rejects_a_second_report_for_one_schedule(
    application_head_postgres: PostgresMigrationHarness,
) -> None:
    schedule_id, _ = _seed_schedule_and_report(application_head_postgres.engine)
    second_report_id = uuid.uuid4()

    with pytest.raises(IntegrityError) as error:
        with application_head_postgres.engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO saved_reports (
                        id, org_id, name, report_plan, is_template, schedule_id,
                        is_active, created_at, updated_at
                    ) VALUES (
                        :id, 'chaos-3150', :name, '{}', FALSE, :schedule_id,
                        TRUE, now(), now()
                    )
                    """
                ),
                {
                    "id": second_report_id,
                    "name": f"report-{second_report_id}",
                    "schedule_id": schedule_id,
                },
            )

    sqlstate = getattr(error.value.orig, "sqlstate", None) or getattr(
        error.value.orig, "pgcode", None
    )
    assert sqlstate == "23505"


def test_migration_refuses_existing_duplicates_with_exact_offending_rows(
    application_head_postgres: PostgresMigrationHarness,
) -> None:
    command.downgrade(_migration_config(), "0095")

    schedule_id, first_report_id = _seed_schedule_and_report(
        application_head_postgres.engine
    )
    second_report_id = uuid.uuid4()
    with application_head_postgres.engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO saved_reports (
                    id, org_id, name, report_plan, is_template, schedule_id,
                    is_active, created_at, updated_at
                ) VALUES (
                    :id, 'chaos-3150', :name, '{}', FALSE, :schedule_id,
                    TRUE, now(), now()
                )
                """
            ),
            {
                "id": second_report_id,
                "name": f"report-{second_report_id}",
                "schedule_id": schedule_id,
            },
        )

    with pytest.raises(RuntimeError) as error:
        command.upgrade(_migration_config(), "0096")

    message = str(error.value)
    assert "found 1 schedule_id value(s)" in message
    assert f"schedule_id={schedule_id}" in message
    assert str(first_report_id) in message
    assert str(second_report_id) in message
    assert "Detach or remove duplicate report definitions" in message
    assert "uq_saved_reports_schedule_id" not in _constraint_names(
        application_head_postgres.engine
    )
    assert _revisions(application_head_postgres.engine) == {"0095"}


def test_unique_constraint_allows_multiple_unscheduled_reports_and_downgrades(
    application_head_postgres: PostgresMigrationHarness,
) -> None:
    command.downgrade(_migration_config(), "0095")
    report_ids = (uuid.uuid4(), uuid.uuid4())
    with application_head_postgres.engine.begin() as connection:
        for report_id in report_ids:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO saved_reports (
                        id, org_id, name, report_plan, is_template, schedule_id,
                        is_active, created_at, updated_at
                    ) VALUES (
                        :id, 'chaos-3150', :name, '{}', FALSE, NULL,
                        TRUE, now(), now()
                    )
                    """
                ),
                {"id": report_id, "name": f"unscheduled-{report_id}"},
            )

    command.upgrade(_migration_config(), "0096")
    assert "uq_saved_reports_schedule_id" in _constraint_names(
        application_head_postgres.engine
    )

    command.downgrade(_migration_config(), "0095")

    assert "uq_saved_reports_schedule_id" not in _constraint_names(
        application_head_postgres.engine
    )
    schedule_id, _ = _seed_schedule_and_report(application_head_postgres.engine)
    with application_head_postgres.engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO saved_reports (
                    id, org_id, name, report_plan, is_template, schedule_id,
                    is_active, created_at, updated_at
                ) VALUES (
                    :id, 'chaos-3150', :name, '{}', FALSE, :schedule_id,
                    TRUE, now(), now()
                )
                """
            ),
            {
                "id": uuid.uuid4(),
                "name": f"duplicate-after-downgrade-{schedule_id}",
                "schedule_id": schedule_id,
            },
        )


def test_audit_lock_prevents_a_duplicate_insert_between_check_and_constraint(
    application_head_postgres: PostgresMigrationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command.downgrade(_migration_config(), "0095")
    schedule_id, _ = _seed_schedule_and_report(application_head_postgres.engine)
    migration = _migration()
    original_find_duplicates = migration._find_duplicate_schedules
    audit_started = Event()
    writer_attempted = Event()
    writer_finished = Event()

    def pause_during_audit(bind: Connection):
        audit_started.set()
        assert writer_attempted.wait(timeout=10)
        assert not writer_finished.wait(timeout=0.5), (
            "a concurrent saved_reports writer crossed the migration audit lock"
        )
        return original_find_duplicates(bind)

    monkeypatch.setattr(migration, "_find_duplicate_schedules", pause_during_audit)

    def insert_duplicate() -> str:
        assert audit_started.wait(timeout=10)
        writer_attempted.set()
        try:
            with application_head_postgres.engine.begin() as connection:
                connection.execute(
                    sa.text(
                        """
                        INSERT INTO saved_reports (
                            id, org_id, name, report_plan, is_template,
                            schedule_id, is_active, created_at, updated_at
                        ) VALUES (
                            :id, 'chaos-3150', :name, '{}', FALSE,
                            :schedule_id, TRUE, now(), now()
                        )
                        """
                    ),
                    {
                        "id": uuid.uuid4(),
                        "name": f"racing-report-{schedule_id}",
                        "schedule_id": schedule_id,
                    },
                )
        except IntegrityError as error:
            sqlstate = getattr(error.orig, "sqlstate", None) or getattr(
                error.orig, "pgcode", None
            )
            return str(sqlstate)
        finally:
            writer_finished.set()
        return "inserted"

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = executor.submit(insert_duplicate)
        with application_head_postgres.engine.begin() as connection:
            _apply_upgrade(connection)
        assert writer.result(timeout=10) == "23505"

    assert "uq_saved_reports_schedule_id" in _constraint_names(
        application_head_postgres.engine
    )
