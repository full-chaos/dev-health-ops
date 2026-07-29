from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _organizations_table(connection: sa.Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "organizations",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
    )
    metadata.create_all(connection)


def test_0071_budget_reservation_upgrade_and_downgrade_are_rehearsable() -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0071_add_byo_llm_budget_reservations"
    )
    assert migration.revision == "0071"
    assert migration.down_revision == "0070"

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as connection:
            _organizations_table(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
                inspector = sa.inspect(connection)
                assert "byo_llm_budget_reservations" in inspector.get_table_names()
                assert {
                    "id",
                    "org_id",
                    "window_start",
                    "idempotency_key",
                    "provider",
                    "model",
                    "reserved_micro_usd",
                    "actual_micro_usd",
                    "status",
                    "pricing_version",
                    "input_tokens",
                    "cached_input_tokens",
                    "output_tokens",
                    "created_at",
                    "reconciled_at",
                } == {
                    column["name"]
                    for column in inspector.get_columns("byo_llm_budget_reservations")
                }
                assert {
                    index["name"]
                    for index in inspector.get_indexes("byo_llm_budget_reservations")
                } == {"ix_byo_llm_budget_org_window_status"}
                assert "uq_byo_llm_budget_reservation_attempt" in {
                    constraint["name"]
                    for constraint in inspector.get_unique_constraints(
                        "byo_llm_budget_reservations"
                    )
                }

                migration.downgrade()
                assert (
                    "byo_llm_budget_reservations"
                    not in sa.inspect(connection).get_table_names()
                )
                assert "organizations" in sa.inspect(connection).get_table_names()

                migration.upgrade()
                assert (
                    "byo_llm_budget_reservations"
                    in sa.inspect(connection).get_table_names()
                )
    finally:
        engine.dispose()
