from __future__ import annotations

import importlib
from pathlib import Path

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from dev_health_ops.models.dev_persistence import DEV_RETENTION_DAYS


def _parent_tables(connection: sa.Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "organizations",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
    )
    sa.Table(
        "users",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
    )
    sa.Table(
        "existing_product_state",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
    )
    metadata.create_all(connection)


def test_0068_clean_install_upgrade_and_pre_release_downgrade_are_rehearsable():
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0068_add_ask_dev_persistence"
    )
    assert migration.revision == "0068"
    assert migration.down_revision == "0067"

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as connection:
            _parent_tables(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
                tables = set(sa.inspect(connection).get_table_names())
                assert {
                    "dev_conversations",
                    "dev_messages",
                    "dev_runs",
                    "dev_tool_calls",
                    "dev_feedback",
                    "dev_conversation_tombstones",
                }.issubset(tables)
                assert "existing_product_state" in tables

                migration.downgrade()
                tables = set(sa.inspect(connection).get_table_names())
                assert not any(table.startswith("dev_") for table in tables)
                assert "existing_product_state" in tables

                migration.upgrade()
                assert "dev_conversations" in set(
                    sa.inspect(connection).get_table_names()
                )
    finally:
        engine.dispose()


def test_ask_dev_schema_has_only_the_approved_retention_domain_and_safe_columns():
    assert DEV_RETENTION_DAYS == {0, 30}
    source = (
        Path(__file__).parents[1]
        / "src/dev_health_ops/alembic/versions/0068_add_ask_dev_persistence.py"
    ).read_text(encoding="utf-8")
    prohibited_columns = {
        "api_key",
        "access_token",
        "system_prompt",
        "chain_of_thought",
        "provider_request",
        "provider_response",
        "raw_tool_result",
        "source_payload",
    }
    for column in prohibited_columns:
        assert f'Column("{column}"' not in source
    assert "retention_days IN (0, 30)" in source
    assert "retention_days IN (0, 7, 30, 90)" not in source
