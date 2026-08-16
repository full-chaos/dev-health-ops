from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def test_worker_instances_migration_is_reversible() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0104_add_worker_instances"
    )
    connection = sa.create_engine("sqlite://").connect()
    operations = Operations(MigrationContext.configure(connection))
    try:
        setattr(migration, "op", operations)
        migration.upgrade()
        inspector = sa.inspect(connection)
        assert inspector.has_table("worker_instances")
        assert {
            column["name"] for column in inspector.get_columns("worker_instances")
        } == {
            "instance_id",
            "worker_group",
            "queues",
            "state",
            "started_at",
            "heartbeat_at",
            "expires_at",
        }
        migration.downgrade()
        assert not sa.inspect(connection).has_table("worker_instances")
    finally:
        connection.close()
