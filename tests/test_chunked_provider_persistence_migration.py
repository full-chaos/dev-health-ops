from __future__ import annotations

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _run(migration, connection: sa.Connection, operation: str) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        getattr(migration, operation)()


def test_0102_creates_fenced_checkpoint_and_chunk_sidecars() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0102_add_sync_unit_chunked_provider_persistence"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("PRAGMA foreign_keys = ON"))
            connection.execute(
                sa.text(
                    "CREATE TABLE sync_run_units ("
                    "org_id TEXT NOT NULL, id TEXT NOT NULL, "
                    "PRIMARY KEY (id), UNIQUE (org_id, id))"
                )
            )
            _run(migration, connection, "upgrade")

            inspector = sa.inspect(connection)
            assert {
                "sync_run_unit_chunk_checkpoints",
                "sync_run_unit_effect_chunks",
            } <= set(inspector.get_table_names())
            assert {
                "schema_version",
                "generation",
                "next_cursor",
                # The only truthful record count at finalization: the route's
                # completion metadata rides on a final chunk carrying no rows.
                "committed_rows",
            } <= {
                column["name"]
                for column in inspector.get_columns("sync_run_unit_chunk_checkpoints")
            }
            assert {
                "schema_version",
                "generation",
                "payload",
                "ledger",
            } <= {
                column["name"]
                for column in inspector.get_columns("sync_run_unit_effect_chunks")
            }
            assert inspector.get_pk_constraint("sync_run_unit_chunk_checkpoints")[
                "constrained_columns"
            ] == ["org_id", "sync_run_unit_id", "generation"]
            assert inspector.get_pk_constraint("sync_run_unit_effect_chunks")[
                "constrained_columns"
            ] == ["org_id", "sync_run_unit_id", "generation", "ordinal"]

            connection.execute(
                sa.text(
                    "INSERT INTO sync_run_units (org_id, id) "
                    "VALUES ('org-owner', 'unit-1')"
                )
            )
            connection.execute(
                sa.text(
                    "INSERT INTO sync_run_unit_chunk_checkpoints ("
                    "org_id, sync_run_unit_id, generation, provider, dataset_key, "
                    "route_version, normalized_at, owner, lease_expires_at, "
                    "created_at, updated_at) VALUES ("
                    "'org-owner', 'unit-1', 'sync-unit:unit-1', 'github', 'cicd', "
                    "'provider-chunk.v1', CURRENT_TIMESTAMP, 'owner-1', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
            )
            connection.execute(
                sa.text(
                    "INSERT INTO sync_run_unit_effect_chunks ("
                    "org_id, sync_run_unit_id, generation, route_version, ordinal, "
                    "total_chunks, payload, ledger, payload_bytes, manifest_digest, "
                    "created_at, updated_at) VALUES ("
                    "'org-owner', 'unit-1', 'sync-unit:unit-1', 'provider-chunk.v1', "
                    "0, 1, '{\"effects\":[]}', '{}', 1, :digest, "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"digest": "a" * 64},
            )
            with pytest.raises(sa.exc.IntegrityError):
                connection.execute(
                    sa.text(
                        "INSERT INTO sync_run_unit_chunk_checkpoints ("
                        "org_id, sync_run_unit_id, generation, provider, dataset_key, "
                        "route_version, normalized_at, owner, lease_expires_at, "
                        "created_at, updated_at) VALUES ("
                        "'org-intruder', 'unit-1', 'intruder', 'github', 'cicd', "
                        "'provider-chunk.v1', CURRENT_TIMESTAMP, 'owner-1', "
                        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                    )
                )

            connection.execute(
                sa.text("DELETE FROM sync_run_units WHERE id = 'unit-1'")
            )
            assert (
                connection.scalar(
                    sa.text("SELECT count(*) FROM sync_run_unit_chunk_checkpoints")
                )
                == 0
            )
            assert (
                connection.scalar(
                    sa.text("SELECT count(*) FROM sync_run_unit_effect_chunks")
                )
                == 0
            )

            _run(migration, connection, "downgrade")
            assert (
                "sync_run_unit_chunk_checkpoints"
                not in sa.inspect(connection).get_table_names()
            )
            assert (
                "sync_run_unit_effect_chunks"
                not in sa.inspect(connection).get_table_names()
            )
    finally:
        engine.dispose()
