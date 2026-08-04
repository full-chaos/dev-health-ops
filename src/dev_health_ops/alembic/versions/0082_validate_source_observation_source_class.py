"""Validate the NOT VALID dev_run_source_observations CHECK constraint added in 0081.

Revision ID: 0082
Revises: 0081
Create Date: 2026-08-03 00:00:01

``VALIDATE CONSTRAINT`` takes only a ``SHARE UPDATE EXCLUSIVE`` lock (does
not block concurrent reads/writes) while it scans existing rows. Every
pre-existing row in ``dev_run_source_observations`` already satisfies the
widened constraint (it satisfied the NARROWER pre-0081 constraint, which
0081's widened version is a strict superset of), so this step is expected
to be fast -- but is kept as its own migration, mirroring 0075's own
precedent for the 0074 ``dev_runs`` CHECK constraints, so a slow validation
on an unexpectedly large table cannot block 0081's constraint install.

No-op on non-PostgreSQL backends: SQLite's ``batch_alter_table`` (0081's
SQLite arm) already fully validates a CHECK constraint at creation time.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0082"
down_revision: str | None = "0081"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_run_source_observations_source_class"
_TABLE = "dev_run_source_observations"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    # Constraint/table names are module-level literals; DDL cannot take bound parameters.
    op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE {_TABLE} VALIDATE CONSTRAINT {_CONSTRAINT}"
    )


def downgrade() -> None:
    # VALIDATE CONSTRAINT has no reversible state: the constraint remains
    # installed (and valid) from 0081 either way. Nothing to undo here.
    pass
