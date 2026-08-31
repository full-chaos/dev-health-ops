"""PostgreSQL proof for the daily_metrics_partitions failed_permanent state.

CHAOS-4319: this migration is the durable-truth half of the ambiguous_refused
fix -- see internal/jobs/metrics/daily/postgres.go's FailPartitionPermanently
for the Go writer this schema exists to support.
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
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.exc import IntegrityError

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "daily_metrics_partitions"
_COLUMN = "failure_reason"
_RUN_ID = "11111111-1111-4111-8111-111111111111"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0112(
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

    database_name = f"test_chaos_4319_{uuid.uuid4().hex}"
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
        command.upgrade(_migration_config(), "0112")

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


def _index_names(engine: Engine) -> set[str]:
    return {str(index["name"]) for index in sa.inspect(engine).get_indexes(_TABLE)}


def _seed_run_and_pending_partition(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO daily_metrics_runs (
                    id, org_id, target_day, generation, status,
                    finalization_status, created_at, updated_at
                ) VALUES (
                    :run_id, '22222222-2222-4222-8222-222222222222',
                    '2026-08-26', 'post-sync:test', 'running',
                    'pending', now(), now()
                )
                """
            ),
            {"run_id": _RUN_ID},
        )
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_TABLE} (
                    id, run_id, ordinal, repo_ids, status, attempt_count,
                    created_at, updated_at
                ) VALUES (
                    '33333333-3333-4333-8333-333333333333', :run_id, 0,
                    '[]'::jsonb, 'pending', 0, now(), now()
                )
                """
            ),
            {"run_id": _RUN_ID},
        )


def test_0113_adds_nullable_bounded_failure_reason_without_rewriting_history(
    migrated_to_0112: PostgresMigrationHarness,
) -> None:
    _seed_run_and_pending_partition(migrated_to_0112.engine)
    command.upgrade(_migration_config(), "0113")

    column = _columns(migrated_to_0112.engine)[_COLUMN]
    assert isinstance(column["type"], postgresql.VARCHAR)
    assert column["type"].length == 64
    assert column["nullable"] is True

    with migrated_to_0112.engine.connect() as connection:
        legacy_reason = connection.execute(
            sa.text(f"SELECT {_COLUMN} FROM {_TABLE} WHERE ordinal = 0")
        ).scalar_one()
    assert legacy_reason is None
    assert _revisions(migrated_to_0112.engine) == {"0113"}


def test_0113_failed_permanent_requires_a_failure_reason(
    migrated_to_0112: PostgresMigrationHarness,
) -> None:
    """failed_permanent always carries a reason; a reason may also
    accompany plain 'failed' (CHAOS-4316 writes it there), but never
    pending/running/succeeded."""
    _seed_run_and_pending_partition(migrated_to_0112.engine)
    command.upgrade(_migration_config(), "0113")

    with migrated_to_0112.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"UPDATE {_TABLE} SET status = 'failed_permanent', "
                f"{_COLUMN} = 'ambiguous_refused' WHERE ordinal = 0"
            )
        )
    with migrated_to_0112.engine.connect() as connection:
        row = connection.execute(
            sa.text(f"SELECT status, {_COLUMN} FROM {_TABLE} WHERE ordinal = 0")
        ).one()
    assert row.status == "failed_permanent"
    assert row.failure_reason == "ambiguous_refused"

    # failed_permanent with no reason must be rejected.
    with pytest.raises(IntegrityError):
        with migrated_to_0112.engine.begin() as connection:
            connection.execute(
                sa.text(f"UPDATE {_TABLE} SET {_COLUMN} = NULL WHERE ordinal = 0")
            )

    # A reason on plain 'failed' (CHAOS-4316's retryable liveness-kill path)
    # must be ALLOWED -- the shared column is not failed_permanent-exclusive.
    with migrated_to_0112.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"UPDATE {_TABLE} SET status = 'failed', "
                f"{_COLUMN} = 'progress_stalled' WHERE ordinal = 0"
            )
        )
    with migrated_to_0112.engine.connect() as connection:
        row = connection.execute(
            sa.text(f"SELECT status, {_COLUMN} FROM {_TABLE} WHERE ordinal = 0")
        ).one()
    assert row.status == "failed"
    assert row.failure_reason == "progress_stalled"

    # A reason on 'pending' (never a failed-ish status) must still be
    # rejected.
    with pytest.raises(IntegrityError):
        with migrated_to_0112.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"UPDATE {_TABLE} SET status = 'pending', "
                    f"{_COLUMN} = 'ambiguous_refused' WHERE ordinal = 0"
                )
            )


def test_0113_failed_permanent_is_excluded_from_dispatchable_partitions(
    migrated_to_0112: PostgresMigrationHarness,
) -> None:
    """DispatchablePartitions (postgres.go) selects status IN ('pending',
    'failed') -- failed_permanent must never satisfy that filter, or a
    partition that can never succeed without a human /repair call would spin
    forever back into the same stuck ledger entry."""
    _seed_run_and_pending_partition(migrated_to_0112.engine)
    command.upgrade(_migration_config(), "0113")

    with migrated_to_0112.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"UPDATE {_TABLE} SET status = 'failed_permanent', "
                f"{_COLUMN} = 'ambiguous_refused' WHERE ordinal = 0"
            )
        )
    with migrated_to_0112.engine.connect() as connection:
        dispatchable = connection.execute(
            sa.text(f"SELECT id FROM {_TABLE} WHERE status IN ('pending', 'failed')")
        ).fetchall()
    assert dispatchable == []

    with migrated_to_0112.engine.connect() as connection:
        indexed = connection.execute(
            sa.text(f"SELECT run_id FROM {_TABLE} WHERE status = 'failed_permanent'")
        ).fetchall()
    assert len(indexed) == 1


def test_0113_index_and_downgrade_reupgrade_converge(
    migrated_to_0112: PostgresMigrationHarness,
) -> None:
    _seed_run_and_pending_partition(migrated_to_0112.engine)
    command.upgrade(_migration_config(), "0113")
    assert "ix_daily_metrics_partition_failed_permanent" in _index_names(
        migrated_to_0112.engine
    )

    command.downgrade(_migration_config(), "0112")
    assert _COLUMN not in _columns(migrated_to_0112.engine)
    assert _revisions(migrated_to_0112.engine) == {"0112"}
    with migrated_to_0112.engine.connect() as connection:
        status = connection.execute(
            sa.text(f"SELECT status FROM {_TABLE} WHERE ordinal = 0")
        ).scalar_one()
    assert status == "pending"

    command.upgrade(_migration_config(), "application_schema@head")
    assert _COLUMN in _columns(migrated_to_0112.engine)
    assert _revisions(migrated_to_0112.engine) == {"0121"}


def test_0113_downgrade_folds_failed_permanent_rows_back_to_failed(
    migrated_to_0112: PostgresMigrationHarness,
) -> None:
    """A downgrade target's own status vocabulary never included
    failed_permanent -- the migration's downgrade() must fold any such row
    back to plain 'failed' before recreating the narrower check constraint,
    or the downgrade itself would fail against real data."""
    _seed_run_and_pending_partition(migrated_to_0112.engine)
    command.upgrade(_migration_config(), "0113")
    with migrated_to_0112.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"UPDATE {_TABLE} SET status = 'failed_permanent', "
                f"{_COLUMN} = 'ambiguous_refused' WHERE ordinal = 0"
            )
        )

    command.downgrade(_migration_config(), "0112")
    with migrated_to_0112.engine.connect() as connection:
        status = connection.execute(
            sa.text(f"SELECT status FROM {_TABLE} WHERE ordinal = 0")
        ).scalar_one()
    assert status == "failed"
