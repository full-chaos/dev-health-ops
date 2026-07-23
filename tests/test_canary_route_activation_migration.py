from __future__ import annotations

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _create_pre_0061_schema(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE worker_operator_audits (
                id INTEGER PRIMARY KEY,
                action TEXT NOT NULL,
                CONSTRAINT ck_worker_operator_audits_action CHECK (
                    action IN (
                        'jobs.cancel', 'jobs.retry', 'queues.pause',
                        'queues.resume', 'workers.drain'
                    )
                )
            )
            """
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE worker_job_routes (
                job_kind TEXT PRIMARY KEY,
                transport TEXT NOT NULL,
                paused BOOLEAN NOT NULL,
                generation BIGINT NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
    )


def _upgrade(migration, connection: sa.Connection) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        migration.upgrade()


def test_0061_seeds_only_the_safe_sync_baseline_and_allows_route_audits() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0061_seed_sync_provider_canary_route"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _upgrade(migration, connection)
            _upgrade(migration, connection)

            assert connection.execute(
                sa.text(
                    """
                    SELECT transport, paused, generation
                    FROM worker_job_routes
                    WHERE job_kind = 'sync.provider_unit'
                    """
                )
            ).one() == ("celery", 0, 1)

            for action in (
                "jobs.cancel",
                "job_routes.apply_checked_in",
                "job_routes.rollback",
            ):
                connection.execute(
                    sa.text(
                        "INSERT INTO worker_operator_audits (action) VALUES (:action)"
                    ),
                    {"action": action},
                )
    finally:
        engine.dispose()


def test_0061_refuses_to_replace_a_conflicting_runtime_route() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0061_seed_sync_provider_canary_route"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            connection.execute(
                sa.text(
                    """
                    INSERT INTO worker_job_routes
                        (job_kind, transport, paused, generation, updated_at)
                    VALUES ('sync.provider_unit', 'river_canary', FALSE, 1, CURRENT_TIMESTAMP)
                    """
                )
            )

            with pytest.raises(
                RuntimeError, match="conflicts with safe Celery baseline"
            ):
                _upgrade(migration, connection)
    finally:
        engine.dispose()
