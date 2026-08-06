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


def test_0086_creates_bounded_tenant_generation_snapshot_with_cascade() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0086_add_sync_unit_effect_snapshots"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    unit_id = "11111111-1111-4111-8111-111111111111"
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("PRAGMA foreign_keys = ON"))
            connection.execute(
                sa.text("CREATE TABLE sync_run_units (id UUID PRIMARY KEY)")
            )
            _run(migration, connection, "upgrade")

            inspector = sa.inspect(connection)
            assert "sync_run_unit_effect_snapshots" in inspector.get_table_names()
            assert inspector.get_pk_constraint("sync_run_unit_effect_snapshots")[
                "constrained_columns"
            ] == [
                "org_id",
                "sync_run_unit_id",
                "generation",
            ]
            foreign_keys = inspector.get_foreign_keys("sync_run_unit_effect_snapshots")
            assert foreign_keys[0]["referred_table"] == "sync_run_units"
            assert foreign_keys[0]["options"]["ondelete"] == "CASCADE"

            connection.execute(
                sa.text("INSERT INTO sync_run_units (id) VALUES (:id)"),
                {"id": unit_id},
            )
            params = {
                "org_id": "org-acme",
                "unit_id": unit_id,
                "generation": f"sync-unit:{unit_id}",
                "provider": "github",
                "dataset": "work-items",
                "schema": "v1",
                "digest": "a" * 64,
                "payload": b"{}",
                "payload_bytes": 2,
            }
            connection.execute(
                sa.text(
                    """
                    INSERT INTO sync_run_unit_effect_snapshots (
                        org_id, sync_run_unit_id, generation, provider,
                        dataset_key, schema_version, content_digest,
                        payload_bytes, payload, created_at
                    ) VALUES (
                        :org_id, :unit_id, :generation, :provider,
                        :dataset, :schema, :digest,
                        :payload_bytes, :payload, CURRENT_TIMESTAMP
                    )
                    """
                ),
                params,
            )
            with pytest.raises(sa.exc.IntegrityError):
                connection.execute(
                    sa.text(
                        """
                        INSERT INTO sync_run_unit_effect_snapshots (
                            org_id, sync_run_unit_id, generation, provider,
                            dataset_key, schema_version, content_digest,
                            payload_bytes, payload, created_at
                        ) VALUES (
                            'org-other', :unit_id, 'wrong-size', 'github',
                            'work-items', 'v1', :digest, 3, :payload,
                            CURRENT_TIMESTAMP
                        )
                        """
                    ),
                    params,
                )

            connection.execute(
                sa.text("DELETE FROM sync_run_units WHERE id = :id"), {"id": unit_id}
            )
            assert (
                connection.scalar(
                    sa.text("SELECT count(*) FROM sync_run_unit_effect_snapshots")
                )
                == 0
            )

            _run(migration, connection, "downgrade")
            assert (
                "sync_run_unit_effect_snapshots"
                not in sa.inspect(connection).get_table_names()
            )
    finally:
        engine.dispose()
