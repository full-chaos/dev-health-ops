from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0040"
down_revision: str | None = "0039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "external_ingest_sources"
_BATCHES_TABLE = "external_ingest_batches"
_LEGACY_CONSTRAINT = "uq_external_ingest_sources_org_system_instance"
_FAMILY_CONSTRAINT = "uq_external_ingest_sources_org_system_instance_family"


def upgrade() -> None:
    if not _table_exists():
        return
    if "entity_family" not in _column_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.add_column(
                sa.Column(
                    "entity_family", sa.Text(), nullable=False, server_default="legacy"
                )
            )
    if _LEGACY_CONSTRAINT in _unique_constraint_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_constraint(_LEGACY_CONSTRAINT, type_="unique")
    if _FAMILY_CONSTRAINT not in _unique_constraint_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.create_unique_constraint(
                _FAMILY_CONSTRAINT, ["org_id", "system", "instance", "entity_family"]
            )
    _upgrade_batches()


def downgrade() -> None:
    if not _table_exists():
        return
    if _FAMILY_CONSTRAINT in _unique_constraint_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_constraint(_FAMILY_CONSTRAINT, type_="unique")
    if _LEGACY_CONSTRAINT not in _unique_constraint_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.create_unique_constraint(
                _LEGACY_CONSTRAINT, ["org_id", "system", "instance"]
            )
    if "entity_family" in _column_names():
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_column("entity_family")
    _downgrade_batches()


def _upgrade_batches() -> None:
    if _BATCHES_TABLE not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(_BATCHES_TABLE)
    }
    if "entity_family" not in columns:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.add_column(
                sa.Column(
                    "entity_family", sa.Text(), nullable=False, server_default="legacy"
                )
            )
    constraints = {
        constraint["name"]
        for constraint in sa.inspect(op.get_bind()).get_unique_constraints(
            _BATCHES_TABLE
        )
        if constraint["name"] is not None
    }
    if "uq_external_ingest_batches_idem" in constraints:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.drop_constraint("uq_external_ingest_batches_idem", type_="unique")
    if "uq_external_ingest_batches_idem_family" not in constraints:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.create_unique_constraint(
                "uq_external_ingest_batches_idem_family",
                [
                    "org_id",
                    "source_system",
                    "source_instance",
                    "entity_family",
                    "idempotency_key",
                ],
            )


def _downgrade_batches() -> None:
    if _BATCHES_TABLE not in sa.inspect(op.get_bind()).get_table_names():
        return
    constraints = {
        constraint["name"]
        for constraint in sa.inspect(op.get_bind()).get_unique_constraints(
            _BATCHES_TABLE
        )
        if constraint["name"] is not None
    }
    if "uq_external_ingest_batches_idem_family" in constraints:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.drop_constraint(
                "uq_external_ingest_batches_idem_family", type_="unique"
            )
    if "uq_external_ingest_batches_idem" not in constraints:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.create_unique_constraint(
                "uq_external_ingest_batches_idem",
                ["org_id", "source_system", "source_instance", "idempotency_key"],
            )
    columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(_BATCHES_TABLE)
    }
    if "entity_family" in columns:
        with op.batch_alter_table(_BATCHES_TABLE) as batch:
            batch.drop_column("entity_family")


def _table_exists() -> bool:
    return _TABLE in sa.inspect(op.get_bind()).get_table_names()


def _column_names() -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(_TABLE)}


def _unique_constraint_names() -> set[str]:
    return {
        constraint["name"]
        for constraint in sa.inspect(op.get_bind()).get_unique_constraints(_TABLE)
        if constraint["name"] is not None
    }
