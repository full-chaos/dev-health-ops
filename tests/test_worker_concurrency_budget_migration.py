"""Schema contract for the durable Go worker concurrency budget."""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _operations() -> tuple[Operations, sa.engine.Connection]:
    engine = sa.create_engine("sqlite://")
    connection = engine.connect()
    context = MigrationContext.configure(connection)
    return Operations(context), connection


def test_0103_creates_payload_free_durable_lease_table() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0103_add_worker_concurrency_leases"
    )
    operations, connection = _operations()
    try:
        setattr(migration, "op", operations)
        migration.upgrade()
        inspector = sa.inspect(connection)
        assert inspector.has_table("worker_concurrency_leases")
        columns = {
            column["name"]
            for column in inspector.get_columns("worker_concurrency_leases")
        }
        assert columns == {
            "id",
            "budget_key",
            "job_kind",
            "concurrency_scope",
            "organization_id",
            "owner_token",
            "lease_expires_at",
            "created_at",
            "updated_at",
        }
        assert "payload" not in columns
        assert "credentials" not in columns
        assert "arguments" not in columns
        migration.downgrade()
        assert not sa.inspect(connection).has_table("worker_concurrency_leases")
    finally:
        connection.close()
