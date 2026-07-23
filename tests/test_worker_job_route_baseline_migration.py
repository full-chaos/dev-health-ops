from __future__ import annotations

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.orm import Session

from dev_health_ops.workers.job_contracts import load_registry
from dev_health_ops.workers.job_routes import resolve_worker_job_route


def _create_pre_0064_schema(connection: sa.Connection) -> None:
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


def _downgrade(migration, connection: sa.Connection) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        migration.downgrade()


def test_0064_seeds_every_registry_kind_at_its_safe_celery_baseline() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0064_seed_checked_in_worker_job_route_baselines"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0064_schema(connection)
            _upgrade(migration, connection)
            _upgrade(migration, connection)

            registry_kinds = tuple(
                contract.kind for contract in load_registry().contracts
            )
            assert len(registry_kinds) == 24
            rows = connection.execute(
                sa.text(
                    """
                    SELECT job_kind, transport, paused, generation
                    FROM worker_job_routes
                    ORDER BY job_kind
                    """
                )
            ).all()
            assert tuple(row[0] for row in rows) == registry_kinds
            assert all(row[1:] == ("celery", 0, 1) for row in rows)

            with Session(bind=connection) as session:
                for kind in registry_kinds:
                    assert resolve_worker_job_route(session, kind) == "celery"

            _downgrade(migration, connection)
            assert connection.execute(
                sa.text("SELECT count(*) FROM worker_job_routes")
            ).scalar_one() == len(registry_kinds)
    finally:
        engine.dispose()


def test_0064_preserves_a_generation_bumped_sync_provider_canary() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0064_seed_checked_in_worker_job_route_baselines"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0064_schema(connection)
            connection.execute(
                sa.text(
                    """
                    INSERT INTO worker_job_routes
                        (job_kind, transport, paused, generation, updated_at)
                    VALUES
                        ('sync.provider_unit', 'river_canary', FALSE, 2, CURRENT_TIMESTAMP)
                    """
                )
            )

            _upgrade(migration, connection)

            assert connection.execute(
                sa.text(
                    """
                    SELECT transport, paused, generation
                    FROM worker_job_routes
                    WHERE job_kind = 'sync.provider_unit'
                    """
                )
            ).one() == ("river_canary", 0, 2)
    finally:
        engine.dispose()


def test_0064_rejects_conflicting_rows_without_partial_seed() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0064_seed_checked_in_worker_job_route_baselines"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0064_schema(connection)
            connection.execute(
                sa.text(
                    """
                    INSERT INTO worker_job_routes
                        (job_kind, transport, paused, generation, updated_at)
                    VALUES
                        ('sync.provider_unit', 'river', FALSE, 2, CURRENT_TIMESTAMP)
                    """
                )
            )

            with pytest.raises(RuntimeError, match="conflicts with safe baseline"):
                _upgrade(migration, connection)

            assert (
                connection.execute(
                    sa.text("SELECT count(*) FROM worker_job_routes")
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("kind", "transport", "paused", "generation"),
    (
        ("operational.billing_notification", "celery", False, 2),
        ("sync.provider_unit", "river_canary", True, 2),
    ),
)
def test_0064_rejects_invalid_operator_state(
    kind: str, transport: str, paused: bool, generation: int
) -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0064_seed_checked_in_worker_job_route_baselines"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0064_schema(connection)
            connection.execute(
                sa.text(
                    """
                    INSERT INTO worker_job_routes
                        (job_kind, transport, paused, generation, updated_at)
                    VALUES (:kind, :transport, :paused, :generation, CURRENT_TIMESTAMP)
                    """
                ),
                {
                    "kind": kind,
                    "transport": transport,
                    "paused": paused,
                    "generation": generation,
                },
            )

            with pytest.raises(RuntimeError, match="conflicts with safe baseline"):
                _upgrade(migration, connection)

            assert (
                connection.execute(
                    sa.text("SELECT count(*) FROM worker_job_routes")
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()
