from __future__ import annotations

import importlib
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_REPO_ROOT = Path(__file__).resolve().parents[1]


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


_CHECK_VIOLATIONS = {
    "schema_version": ("v2", 2, b"{}"),
    "payload_bytes_zero": ("v1", 0, b""),
    "payload_bytes_over_cap": ("v1", 67108865, b"{}"),
    "payload_length_mismatch": ("v1", 3, b"{}"),
}


@pytest.mark.parametrize("case", sorted(_CHECK_VIOLATIONS))
def test_0086_check_constraints_reject_out_of_contract_rows(case: str) -> None:
    """Each CHECK on 0086 must reject something.

    The original test inserted only well-formed rows, so deleting the
    schema_version or payload_bytes CHECK from the migration left it green --
    a constraint nothing exercises is decoration. One case per constraint means
    removing any one of them turns exactly one case red.
    """
    schema_version, payload_bytes, payload = _CHECK_VIOLATIONS[case]
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0086_add_sync_unit_effect_snapshots"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    unit_id = "11111111-1111-4111-8111-111111111111"
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("CREATE TABLE sync_run_units (id UUID PRIMARY KEY)")
            )
            _run(migration, connection, "upgrade")
            connection.execute(
                sa.text("INSERT INTO sync_run_units (id) VALUES (:id)"),
                {"id": unit_id},
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
                            'org-acme', :unit_id, 'g1', 'github',
                            'work-items', :schema_version, :digest,
                            :payload_bytes, :payload, CURRENT_TIMESTAMP
                        )
                        """
                    ),
                    {
                        "unit_id": unit_id,
                        "schema_version": schema_version,
                        "digest": "a" * 64,
                        "payload_bytes": payload_bytes,
                        "payload": payload,
                    },
                )
    finally:
        engine.dispose()


def test_integration_fixture_ddl_matches_migration_0086() -> None:
    """The Go integration fixture and migration 0086 must describe one schema.

    The fixture hand-rolls the table because the Go suite cannot run alembic.
    It previously dropped all three CHECK constraints and widened
    content_digest to text, so every Go integration test ran against a schema
    strictly more permissive than production -- proving the code works on a
    table that exists nowhere real. This compares the two directly so drift in
    either direction is caught the day it happens.
    """
    migration_source = (
        _REPO_ROOT
        / "src"
        / "dev_health_ops"
        / "alembic"
        / "versions"
        / "0086_add_sync_unit_effect_snapshots.py"
    ).read_text(encoding="utf-8")
    fixture_source = (
        _REPO_ROOT
        / "internal"
        / "providersync"
        / "repository_postgres_integration_test.go"
    ).read_text(encoding="utf-8")
    fixture_ddl = fixture_source.split("const snapshotFixtureDDL = `", maxsplit=1)[1]
    fixture_ddl = fixture_ddl.split("`", maxsplit=1)[0]

    for constraint in (
        "ck_sync_run_unit_effect_snapshots_payload_bytes",
        "ck_sync_run_unit_effect_snapshots_payload_length",
        "ck_sync_run_unit_effect_snapshots_schema_version",
    ):
        assert constraint in migration_source, constraint
        assert constraint in fixture_ddl, (
            f"{constraint} is in migration 0086 but missing from the Go "
            f"integration fixture -- the fixture is more permissive than "
            f"production"
        )
    for column in (
        "org_id",
        "sync_run_unit_id",
        "generation",
        "provider",
        "dataset_key",
        "schema_version",
        "content_digest",
        "payload_bytes",
        "payload",
        "created_at",
    ):
        assert column in fixture_ddl, column
    # The migration declares String(length=64); a fixture using unbounded text
    # accepts digests production would reject.
    assert "varchar(64)" in fixture_ddl
    assert "ON DELETE CASCADE" in fixture_ddl
