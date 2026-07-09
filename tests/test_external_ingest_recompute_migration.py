"""Migration 0035 (external_ingest_batches recompute columns +
external_ingest_recompute_jobs) tests.

Follows the idempotent-upgrade/downgrade harness established for migration
0031 (``tests/test_rate_limit_observations.py``) and migration 0034
(``tests/test_external_ingest_status_migration.py``).
"""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def _load_migration_0035():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0035_add_external_ingest_recompute"
    )


def test_migration_0035_idempotent_upgrade_downgrade():
    migration = _load_migration_0035()
    assert migration.revision == "0035"
    assert migration.down_revision == "0034"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                # Migration 0034 ALTERs a table owned by 0033 -- create a
                # minimal stand-in so ADD COLUMN has something to target
                # (mirrors this migration's real dependency chain without
                # importing 0033's own module).
                conn.execute(
                    sa.text(
                        "CREATE TABLE external_ingest_batches (ingestion_id TEXT PRIMARY KEY)"
                    )
                )

                migration.upgrade()
                inspector = sa.inspect(conn)
                table_names = inspector.get_table_names()
                assert "external_ingest_recompute_jobs" in table_names

                batch_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batches")
                }
                assert batch_columns == {
                    "ingestion_id",
                    "recompute_status",
                    "recompute_scope",
                    "recompute_dispatched_at",
                    "recompute_completed_at",
                    "recompute_error",
                }

                job_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_recompute_jobs")
                }
                assert job_columns == {
                    "id",
                    "org_id",
                    "source_system",
                    "source_instance",
                    "celery_task_name",
                    "celery_task_id",
                    "queue",
                    "repo_id",
                    "status",
                    "dispatched_at",
                    "completed_at",
                    "error",
                }

                job_index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("external_ingest_recompute_jobs")
                }
                assert "ix_external_ingest_recompute_jobs_scope" in job_index_names

                # Re-running upgrade() must be a no-op (guarded create-if-missing).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert (
                    inspector.get_table_names().count("external_ingest_recompute_jobs")
                    == 1
                )
                batch_columns_after_rerun = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batches")
                }
                assert batch_columns_after_rerun == batch_columns

                migration.downgrade()
                inspector = sa.inspect(conn)
                remaining_tables = inspector.get_table_names()
                assert "external_ingest_recompute_jobs" not in remaining_tables
                remaining_batch_columns = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batches")
                }
                assert remaining_batch_columns == {"ingestion_id"}

                # downgrade() on an already-downgraded state is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "external_ingest_recompute_jobs" in inspector.get_table_names()
                batch_columns_final = {
                    col["name"]
                    for col in inspector.get_columns("external_ingest_batches")
                }
                assert batch_columns_final == batch_columns
    finally:
        engine.dispose()
