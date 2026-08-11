"""Fence prepared effect snapshots to their owning unit tenant.

Revision ID: 0093
Revises: 0092
Create Date: 2026-08-08 00:00:00

0092 intentionally landed first so durable prepared manifests could be
recovered, but its unit-only foreign key permits a row whose ``org_id`` does
not match the referenced unit. This revision pays that tenant-FK debt without
changing the rolling-worker payload or effect-ledger contracts.

The supporting parent key is built concurrently. The replacement foreign key
is installed ``NOT VALID`` and then validated before the legacy key is removed,
so every new write is fenced throughout the transition and existing rows are
checked under PostgreSQL's lower-lock validation path.

SQLite is a no-op: 0092's SQLite migration venue exists for shape/check tests;
the production tenant constraint and its proof are PostgreSQL-specific.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0093"
down_revision: str | None = "0092"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_SNAPSHOTS = "sync_run_unit_effect_snapshots"
_UNITS = "sync_run_units"
_LEGACY_FK = "sync_run_unit_effect_snapshots_sync_run_unit_id_fkey"
_TENANT_FK = "fk_sync_run_unit_effect_snapshots_tenant_unit"
_PARENT_UNIQUE = "uq_sync_run_units_org_id_id_effect_snapshots"
_PARENT_INDEX = "ux_sync_run_units_org_id_id_effect_snapshots"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # A UNIQUE INDEX built concurrently avoids blocking sync-unit writers
    # while PostgreSQL establishes the parent key required by the composite FK.
    # Drop first because a cancelled CREATE INDEX CONCURRENTLY leaves an
    # INVALID index that IF NOT EXISTS would silently retain forever (the same
    # rerun fence used by 0019's lease index migration).
    with op.get_context().autocommit_block():
        op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"DROP INDEX CONCURRENTLY IF EXISTS {_PARENT_INDEX}"
        )
        op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"CREATE UNIQUE INDEX CONCURRENTLY {_PARENT_INDEX} ON {_UNITS} (org_id, id)"
        )
    op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_UNITS} ADD CONSTRAINT {_PARENT_UNIQUE} "
        f"UNIQUE USING INDEX {_PARENT_INDEX}"
    )
    op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_SNAPSHOTS} ADD CONSTRAINT {_TENANT_FK} "
        "FOREIGN KEY (org_id, sync_run_unit_id) "
        f"REFERENCES {_UNITS} (org_id, id) ON DELETE CASCADE NOT VALID"
    )
    op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_SNAPSHOTS} VALIDATE CONSTRAINT {_TENANT_FK}"
    )
    op.drop_constraint(_LEGACY_FK, _SNAPSHOTS, type_="foreignkey")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Restore 0092's lifecycle cascade before removing the tenant key, so the
    # downgrade never exposes a window with no unit ownership constraint.
    op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_SNAPSHOTS} ADD CONSTRAINT {_LEGACY_FK} "
        "FOREIGN KEY (sync_run_unit_id) "
        f"REFERENCES {_UNITS} (id) ON DELETE CASCADE NOT VALID"
    )
    op.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_SNAPSHOTS} VALIDATE CONSTRAINT {_LEGACY_FK}"
    )
    op.drop_constraint(_TENANT_FK, _SNAPSHOTS, type_="foreignkey")
    op.drop_constraint(_PARENT_UNIQUE, _UNITS, type_="unique")
