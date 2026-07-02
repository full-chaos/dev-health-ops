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
``integration_credentials``) and repopulates it from each config's linked
``Integration.credential_id`` (a correlated-subquery ``UPDATE``, run BEFORE
the FK is re-added so a stale/dangling ``integration_id`` can't violate it) --
old code reading the restored column sees the same value the sanctioned
surface would have resolved, rather than every row silently going NULL and
widening any code still reading the legacy column (codex review, CHAOS-2762).

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
    _repopulate_from_linked_integration()
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


def _repopulate_from_linked_integration() -> None:
    """Backfill the recreated column from each config's linked Integration.

    Codex review (CHAOS-2762 finding #2): a bare re-add left every row NULL on
    rollback, silently changing behavior for anything still reading the
    legacy column (e.g. a not-yet-rolled-back-deploy's ``workers
    /team_drift_sync.py``) even though the true, current mapping is fully
    recoverable from ``Integration.credential_id``. Runs BEFORE the FK is
    re-added, so a stale/dangling ``integration_id`` (no matching
    ``integrations`` row) cannot violate the new constraint -- such rows
    simply keep ``credential_id IS NULL``, matching "no linked integration"
    semantics.

    A correlated subquery (not ``UPDATE ... FROM``) so this runs unchanged on
    SQLite (used to unit-test this migration) and Postgres alike.
    """
    op.execute(
        sa.text(
            f"UPDATE {_TABLE} "
            f"SET {_COLUMN} = ("
            f"    SELECT credential_id FROM integrations "
            f"    WHERE integrations.id = {_TABLE}.integration_id"
            f") "
            f"WHERE integration_id IN ("
            f"    SELECT id FROM integrations WHERE credential_id IS NOT NULL"
            f")"
        )
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
