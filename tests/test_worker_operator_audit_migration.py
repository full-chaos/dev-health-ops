from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def test_migration_0043_chains_after_outbox_and_is_reversible():
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0043_add_worker_operator_audits"
    )
    assert migration.revision == "0043"
    assert migration.down_revision == "0042"

    engine = sa.create_engine("sqlite:///:memory:")
    metadata = sa.MetaData()
    sa.Table(
        "internal_service_credentials",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
    )
    try:
        with engine.connect() as connection:
            metadata.create_all(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
                columns = {
                    column["name"]
                    for column in sa.inspect(connection).get_columns(
                        "worker_operator_audits"
                    )
                }
                assert columns == {
                    "id",
                    "credential_id",
                    "principal_type",
                    "principal_id",
                    "action",
                    "resource_type",
                    "resource_id",
                    "reason_code",
                    "correlation_id",
                    "status",
                    "created_at",
                    "completed_at",
                }
                migration.downgrade()
                assert "worker_operator_audits" not in set(
                    sa.inspect(connection).get_table_names()
                )
    finally:
        engine.dispose()
