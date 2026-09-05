"""PostgreSQL proof for the daily_metrics_runs blocked marker (0124).

CHAOS-5040/CHAOS-4970. This is the AUTHORITATIVE half of the proof: the Go
integration suite exercises the predicate against a HAND-ROLLED fixture
(``createDailyTables`` in internal/jobs/metrics/daily/postgres_integration_test.go),
which mirrors production DDL but is not the migration. A test asserting the
paired CHECK against that fixture would pass for the fixture's reasons rather
than production's. This one runs the real alembic upgrade, so the columns, the
constraint and the index are proven to exist as the migration actually creates
them -- and the downgrade is proven to remove them.

See internal/jobs/metrics/daily/blocked.go for the Go writer this schema
exists to support.
"""

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
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.exc import IntegrityError

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "daily_metrics_runs"
_PAIRED_CHECK = "ck_daily_metrics_run_blocked_marker_paired"
_INDEX = "ix_daily_metrics_run_blocked"
_RUN_ID = "33333333-3333-4333-8333-333333333333"
_ORG_ID = "44444444-4444-4444-8444-444444444444"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0123(
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

    database_name = f"test_chaos_5040_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0123")

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


def _columns(engine: Engine) -> dict[str, ReflectedColumn]:
    return {
        str(column["name"]): column for column in sa.inspect(engine).get_columns(_TABLE)
    }


def _index_names(engine: Engine) -> set[str]:
    return {str(index["name"]) for index in sa.inspect(engine).get_indexes(_TABLE)}


def _seed_running_run(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO daily_metrics_runs (
                    id, org_id, target_day, generation, status,
                    finalization_status, created_at, updated_at
                ) VALUES (
                    :run_id, :org_id, '2026-09-01', 'post-sync:test', 'running',
                    'pending', now(), now()
                )
                """
            ),
            {"run_id": _RUN_ID, "org_id": _ORG_ID},
        )


def test_upgrade_adds_the_paired_blocked_marker(
    migrated_to_0123: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0123.engine

    # Red-on-baseline: at 0123 the columns must NOT exist, or the test would
    # pass for reasons that have nothing to do with this migration.
    before = _columns(engine)
    assert "blocked_at" not in before
    assert "blocked_reason" not in before

    command.upgrade(_migration_config(), "0124")

    after = _columns(engine)
    assert "blocked_at" in after, "0124 did not add blocked_at"
    assert "blocked_reason" in after, "0124 did not add blocked_reason"
    # Nullable on purpose: an unblocked run carries neither, and the migration
    # deliberately backfills nothing.
    assert after["blocked_at"]["nullable"] is True
    assert after["blocked_reason"]["nullable"] is True
    assert _INDEX in _index_names(engine)

    _seed_running_run(engine)

    # The seeded row proves the columns default to "not blocked" rather than
    # the migration having to fill anything in.
    with engine.connect() as connection:
        marker = connection.execute(
            sa.text(
                "SELECT blocked_at, blocked_reason FROM daily_metrics_runs "
                "WHERE id = :run_id"
            ),
            {"run_id": _RUN_ID},
        ).one()
    assert marker == (None, None)


def test_the_paired_check_rejects_a_half_set_marker(
    migrated_to_0123: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0123.engine
    command.upgrade(_migration_config(), "0124")
    _seed_running_run(engine)

    for column, value in (("blocked_at", "now()"), ("blocked_reason", "'x'")):
        with pytest.raises(IntegrityError) as excinfo:  # noqa: PT012
            with engine.begin() as connection:
                connection.execute(
                    sa.text(
                        f"UPDATE daily_metrics_runs SET {column} = {value} "  # noqa: S608
                        "WHERE id = :run_id"
                    ),
                    {"run_id": _RUN_ID},
                )
        assert _PAIRED_CHECK in str(excinfo.value), (
            f"setting {column} alone was rejected by something other than "
            f"{_PAIRED_CHECK}"
        )

    # Control: the PAIRED form is accepted. Without this the two refusals
    # above could be any UPDATE failure rather than the constraint doing its
    # job.
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE daily_metrics_runs "
                "SET blocked_at = now(), blocked_reason = :reason "
                "WHERE id = :run_id"
            ),
            {"run_id": _RUN_ID, "reason": "partial_partitions_failed_permanent"},
        )
    with engine.connect() as connection:
        blocked_at, reason = connection.execute(
            sa.text(
                "SELECT blocked_at, blocked_reason FROM daily_metrics_runs "
                "WHERE id = :run_id"
            ),
            {"run_id": _RUN_ID},
        ).one()
    assert blocked_at is not None
    assert reason == "partial_partitions_failed_permanent"


def test_downgrade_removes_the_marker_without_touching_run_status(
    migrated_to_0123: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0123.engine
    command.upgrade(_migration_config(), "0124")
    _seed_running_run(engine)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE daily_metrics_runs "
                "SET blocked_at = now(), blocked_reason = :reason "
                "WHERE id = :run_id"
            ),
            {"run_id": _RUN_ID, "reason": "all_partitions_failed_permanent"},
        )

    command.downgrade(_migration_config(), "0123")

    after = _columns(engine)
    assert "blocked_at" not in after
    assert "blocked_reason" not in after
    assert _INDEX not in _index_names(engine)
    # The whole reason this is a marker and not a status value: a downgrade
    # has no run state to fold back, unlike 0113's, which must rewrite
    # failed_permanent partitions before its narrower CHECK can reapply. The
    # run is untouched and still 'running'.
    with engine.connect() as connection:
        status = connection.execute(
            sa.text("SELECT status FROM daily_metrics_runs WHERE id = :run_id"),
            {"run_id": _RUN_ID},
        ).scalar_one()
    assert status == "running"
