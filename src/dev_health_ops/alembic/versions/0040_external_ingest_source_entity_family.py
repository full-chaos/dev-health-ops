from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0040"
down_revision: str | None = "0039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "external_ingest_sources"
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
