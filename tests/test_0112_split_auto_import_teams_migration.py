"""Proof for alembic 0112 (CHAOS-4323): auto_import_teams -> three flags.

Exercises the migration's actual upgrade()/downgrade() against an in-memory
SQLite ``sync_configurations`` table (same harness shape as
tests/test_worker_operator_audit_migration.py) rather than the ORM model, so
this pins the migration's OWN read/decode/write logic independent of the live
schema.
"""

from __future__ import annotations

import importlib
import json

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_MODULE = (
    "dev_health_ops.alembic.versions.0112_split_auto_import_teams_into_three_categories"
)


def _migration():
    return importlib.import_module(_MODULE)


def _table(connection: sa.engine.Connection) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "sync_configurations",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("sync_options", sa.JSON, nullable=False),
    )
    metadata.create_all(connection)
    return table


def test_module_chains_after_0111():
    migration = _migration()
    assert migration.revision == "0112"
    assert migration.down_revision == "0111"


def test_upgrade_true_flag_sets_all_three_true():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-1",
                    "sync_options": {"auto_import_teams": True, "owner": "acme"},
                }
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        row = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-1")
        ).scalar_one()
        options = json.loads(row) if isinstance(row, str) else row
        assert options["auto_import_teams"] is True
        assert options["auto_import_projects"] is True
        assert options["auto_import_members"] is True
        # Untouched keys survive.
        assert options["owner"] == "acme"


def test_upgrade_false_or_absent_flag_sets_all_three_false():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {"id": "row-false", "sync_options": {"auto_import_teams": False}},
                {"id": "row-absent", "sync_options": {"owner": "acme"}},
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        for row_id in ("row-false", "row-absent"):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is False
            assert options["auto_import_projects"] is False
            assert options["auto_import_members"] is False


def test_upgrade_is_idempotent():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [{"id": "row-1", "sync_options": {"auto_import_teams": True}}],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()
            first = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            migration.upgrade()
            second = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
        assert first == second


def test_upgrade_skips_unreadable_row_without_raising():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                # A JSON array (not an object) cannot carry the flags -- must
                # be left alone, not guessed at, mirroring 0108's UNREADABLE
                # handling.
                {"id": "row-bad", "sync_options": ["not", "a", "dict"]},
                {"id": "row-good", "sync_options": {"auto_import_teams": True}},
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        bad = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-bad")
        ).scalar_one()
        bad_options = json.loads(bad) if isinstance(bad, str) else bad
        assert bad_options == ["not", "a", "dict"]

        good = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-good")
        ).scalar_one()
        good_options = json.loads(good) if isinstance(good, str) else good
        assert good_options["auto_import_projects"] is True


def test_downgrade_drops_new_keys_and_keeps_auto_import_teams():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-1",
                    "sync_options": {"auto_import_teams": True, "owner": "acme"},
                }
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert "auto_import_projects" in options

            migration.downgrade()

            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
        assert "auto_import_projects" not in options
        assert "auto_import_members" not in options
        assert options["auto_import_teams"] is True
        assert options["owner"] == "acme"
