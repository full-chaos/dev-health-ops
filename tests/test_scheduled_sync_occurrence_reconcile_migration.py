from __future__ import annotations

import importlib
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def test_migration_0051_backfills_completed_links_and_is_reversible() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0051_add_scheduled_sync_occurrence_reconcile_state"
    )
    assert migration.revision == "0051"
    assert migration.down_revision == "0050"

    engine = sa.create_engine("sqlite:///:memory:")
    metadata = sa.MetaData()
    occurrences = sa.Table(
        "scheduled_sync_occurrences",
        metadata,
        sa.Column("occurrence_id", sa.Text(), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_config_id", sa.Text(), nullable=False),
        sa.Column("scheduled_job_id", sa.Text(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("job_run_id", sa.Text(), nullable=True),
        sa.Column("sync_run_id", sa.Text(), nullable=True),
    )
    try:
        with engine.connect() as connection:
            metadata.create_all(connection)
            connection.execute(
                occurrences.insert().values(
                    occurrence_id="linked",
                    org_id="org",
                    sync_config_id="config",
                    scheduled_job_id="job",
                    scheduled_for=datetime(2026, 7, 23, tzinfo=timezone.utc),
                    job_run_id="job-run",
                    sync_run_id="sync-run",
                )
            )
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()

                columns = {
                    column["name"]
                    for column in sa.inspect(connection).get_columns(
                        "scheduled_sync_occurrences"
                    )
                }
                assert {
                    "reconcile_attempt_count",
                    "reconcile_next_attempt_at",
                    "reconcile_error_code",
                    "reconcile_error_at",
                    "reconcile_status",
                } <= columns
                assert (
                    connection.execute(
                        sa.text(
                            "SELECT reconcile_status FROM scheduled_sync_occurrences "
                            "WHERE occurrence_id = 'linked'"
                        )
                    ).scalar_one()
                    == "completed"
                )
                index_columns = sa.inspect(connection).get_indexes(
                    "scheduled_sync_occurrences"
                )
                assert any(
                    index["name"] == "ix_scheduled_sync_occurrence_reconcile_due"
                    and index["column_names"]
                    == [
                        "reconcile_status",
                        "reconcile_next_attempt_at",
                        "org_id",
                        "sync_config_id",
                        "scheduled_job_id",
                        "scheduled_for",
                    ]
                    for index in index_columns
                )

                migration.downgrade()

            assert "reconcile_status" not in {
                column["name"]
                for column in sa.inspect(connection).get_columns(
                    "scheduled_sync_occurrences"
                )
            }
    finally:
        engine.dispose()
