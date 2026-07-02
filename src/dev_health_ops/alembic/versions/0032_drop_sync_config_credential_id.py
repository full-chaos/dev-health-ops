"""Drop the legacy sync_configurations.credential_id auth surface (CHAOS-2762).

``SyncConfiguration.credential_id`` predates run-level credential stamping
(CHAOS-2755, ``0030``) and was always a second, *unfrozen* copy of the
credential selection: ``api/admin/routers/sync.py``'s planner-managed create
path wrote the same value to both ``Integration.credential_id`` (the
sanctioned surface ``resolve_run_auth``/``plan_sync_run`` actually reads) and
this column. Nothing in the auth-resolution path (``sync/planner.py``,
``workers/sync_bootstrap.py``) has ever read ``sync_configurations
.credential_id`` -- only the admin API preflight/response-building code did,
and that has been repointed to resolve through the linked ``Integration`` row
instead (``SyncConfiguration.integration_id -> Integration.credential_id``).

Dropping this column removes a second, unfrozen path by which a credential
could appear to attach to sync work, leaving exactly one (``Integration``,
frozen onto ``sync_runs`` at plan time). This does not touch dispatch
capacity: the column was never read by planning, budgeting, or dispatch.

Data note: the column's values are redundant with (and, for planner-managed
configs, always assigned from) ``Integration.credential_id`` via each config's
``integration_id`` FK, so no backfill/migration of live data is needed before
dropping it. The downgrade recreates the column (nullable, FK'd back to
``integration_credentials``) but does **not** repopulate historical values --
this is a deliberate, documented lossy downgrade of a column that was already
provably redundant; the current mapping can be reconstructed post-downgrade
via ``UPDATE sync_configurations sc SET credential_id = i.credential_id FROM
integrations i WHERE i.id = sc.integration_id`` if ever needed.

Revision ID: 0032
Revises: 0031
Create Date: 2026-07-02 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0032"
down_revision: str | None = "0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_configurations"
_COLUMN = "credential_id"
_TARGET_TABLE = "integration_credentials"


def upgrade() -> None:
    _drop_foreign_key_on_column(_TABLE, _COLUMN)
    _drop_column_if_present(_TABLE, _COLUMN)


def downgrade() -> None:
    _add_column_if_missing(
        _TABLE,
        sa.Column(_COLUMN, UUID(as_uuid=True), nullable=True),
    )
    existing_fks = {fk["name"] for fk in _foreign_keys(_TABLE)}
    fk_name = f"fk_{_TABLE}_{_COLUMN}_{_TARGET_TABLE}"
    if fk_name not in existing_fks and _COLUMN not in {
        col for fk in _foreign_keys(_TABLE) for col in fk.get("constrained_columns", [])
    }:
        op.create_foreign_key(
            fk_name,
            _TABLE,
            _TARGET_TABLE,
            [_COLUMN],
            ["id"],
            ondelete="SET NULL",
        )


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if column.name not in _column_names(table_name):
        op.add_column(table_name, column)


def _drop_column_if_present(table_name: str, column_name: str) -> None:
    if column_name in _column_names(table_name):
        op.drop_column(table_name, column_name)


def _drop_foreign_key_on_column(table_name: str, column_name: str) -> None:
    for fk in _foreign_keys(table_name):
        if column_name in fk.get("constrained_columns", []):
            fk_name = fk.get("name")
            if fk_name:
                op.drop_constraint(fk_name, table_name, type_="foreignkey")


def _foreign_keys(table_name: str) -> list[dict[str, Any]]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return []
    # SQLAlchemy types this as list[ReflectedForeignKeyConstraint] (a TypedDict);
    # widen to list[dict[str, Any]] for the plain-dict access below (fk["name"],
    # fk.get("constrained_columns")) -- structurally a dict, just not
    # list-invariance-compatible without the cast.
    return cast(list[dict[str, Any]], inspector.get_foreign_keys(table_name))


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}
