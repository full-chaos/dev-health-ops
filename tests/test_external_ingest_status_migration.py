"""Migration 0033 (external_ingest_batches/rejections/batch_payloads) tests.

Follows the idempotent-upgrade/downgrade harness established for migration
0031 (tests/test_rate_limit_observations.py::test_migration_0031_idempotent_upgrade)
and migration 0032 (tests/test_migration_0032_customer_push_ingest_auth.py).
"""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def _load_migration_0033():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0033_add_external_ingest_status_store"
    )


def test_migration_0033_idempotent_upgrade_downgrade():
    migration = _load_migration_0033()
    assert migration.revision == "0033"
    assert migration.down_revision == "0032"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                table_names = inspector.get_table_names()
                assert "external_ingest_batches" in table_names
                assert "external_ingest_rejections" in table_names
                assert "external_ingest_batch_payloads" in table_names

                batch_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batches")
                }
                assert batch_columns == {
                    "ingestion_id",
                    "org_id",
                    "idempotency_key",
                    "payload_hash",
                    "source_system",
                    "source_instance",
                    "producer",
                    "producer_version",
                    "schema_version",
                    "window_started_at",
                    "window_ended_at",
                    "status",
                    "attempts",
                    "items_received",
                    "items_accepted",
                    "items_rejected",
                    "record_counts",
                    "error_summary",
                    "created_at",
                    "updated_at",
                    "completed_at",
                }

                rejection_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_rejections")
                }
                assert rejection_columns == {
                    "id",
                    "org_id",
                    "ingestion_id",
                    "record_index",
                    "record_kind",
                    "external_id",
                    "code",
                    "message",
                    "path",
                    "created_at",
                }

                payload_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batch_payloads")
                }
                assert payload_columns == {
                    "ingestion_id",
                    "org_id",
                    "schema_version",
                    "payload_json",
                    "byte_size",
                    "created_at",
                }

                batch_index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("external_ingest_batches")
                }
                assert "uq_external_ingest_batches_idem" in batch_index_names
                assert "ix_external_ingest_batches_org_status" in batch_index_names
                assert "ix_external_ingest_batches_org_created" in batch_index_names
                assert "ix_external_ingest_batches_org_source" in batch_index_names

                rejection_indexes = inspector.get_indexes("external_ingest_rejections")
                rejection_index_names = {ix["name"] for ix in rejection_indexes}
                assert (
                    "uq_external_ingest_rejections_ingestion_order"
                    in rejection_index_names
                )
                assert "ix_external_ingest_rejections_org_id" in rejection_index_names
                order_index = next(
                    ix
                    for ix in rejection_indexes
                    if ix["name"] == "uq_external_ingest_rejections_ingestion_order"
                )
                assert bool(order_index["unique"])

                payload_index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("external_ingest_batch_payloads")
                }
                assert "ix_external_ingest_batch_payloads_org_id" in payload_index_names

                fks = inspector.get_foreign_keys("external_ingest_rejections")
                assert len(fks) == 1
                assert fks[0]["referred_table"] == "external_ingest_batches"
                assert fks[0]["options"].get("ondelete") == "CASCADE"

                # Re-running upgrade() must be a no-op (guarded create-if-missing).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert inspector.get_table_names().count("external_ingest_batches") == 1
                assert (
                    inspector.get_table_names().count("external_ingest_rejections") == 1
                )
                assert (
                    inspector.get_table_names().count("external_ingest_batch_payloads")
                    == 1
                )

                migration.downgrade()
                inspector = sa.inspect(conn)
                remaining = inspector.get_table_names()
                assert "external_ingest_batch_payloads" not in remaining
                assert "external_ingest_rejections" not in remaining
                assert "external_ingest_batches" not in remaining

                # downgrade() on already-absent tables is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "external_ingest_batches" in inspector.get_table_names()
                assert "external_ingest_rejections" in inspector.get_table_names()
                assert "external_ingest_batch_payloads" in inspector.get_table_names()
    finally:
        engine.dispose()
