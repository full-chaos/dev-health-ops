"""Fast structural proof for the 0099 delivery-abandonment revision."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _migration() -> ModuleType:
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0099_add_worker_job_delivery_abandonments"
    )


def _run(migration: ModuleType, connection: sa.Connection, operation: str) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        getattr(migration, operation)()


def test_0099_zero_attempt_history_is_valid_but_negative_counts_are_not() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            migration = _migration()
            assert migration.revision == "0099"
            assert migration.down_revision == "0098"
            _run(migration, connection, "upgrade")

            connection.execute(
                sa.text(
                    """
                    INSERT INTO worker_job_delivery_abandonments (
                        dedupe_key, job_kind, abandoned_at,
                        attempt_count, last_error_code
                    ) VALUES (
                        'report.run:zero', 'report.run', CURRENT_TIMESTAMP, 0, NULL
                    )
                    """
                )
            )
            with pytest.raises(sa.exc.IntegrityError):
                connection.execute(
                    sa.text(
                        """
                        INSERT INTO worker_job_delivery_abandonments (
                            dedupe_key, job_kind, abandoned_at,
                            attempt_count, last_error_code
                        ) VALUES (
                            'report.run:negative', 'report.run',
                            CURRENT_TIMESTAMP, -1, 'invalid'
                        )
                        """
                    )
                )

        with engine.begin() as connection:
            _run(_migration(), connection, "downgrade")
            assert not sa.inspect(connection).has_table(
                "worker_job_delivery_abandonments"
            )
    finally:
        engine.dispose()
