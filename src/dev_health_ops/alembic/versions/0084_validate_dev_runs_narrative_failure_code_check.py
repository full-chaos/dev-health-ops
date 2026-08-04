"""Validate the NOT VALID dev_runs.narrative_failure_code CHECK constraint added in 0083.

Revision ID: 0084
Revises: 0083
Create Date: 2026-08-02 00:00:02

``VALIDATE CONSTRAINT`` takes only a ``SHARE UPDATE EXCLUSIVE`` lock (does
not block concurrent reads/writes) while it scans existing rows.
``narrative_failure_code`` has had no producer since 0078 introduced the
column, so it is NULL on every pre-existing row -- trivially satisfying
the constraint, which explicitly allows NULL. This step is therefore
expected to be fast, but is kept as its own migration so a slow
validation on an unexpectedly large ``dev_runs`` table cannot block
0083's constraint install, mirroring 0074/0075's and 0081/0082's own
precedent.

No-op on non-PostgreSQL backends: SQLite's ``batch_alter_table`` (0083's
SQLite arm) already fully validates a CHECK constraint at creation time.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0084"
down_revision: str | None = "0083"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "dev_runs"
_CONSTRAINT = "ck_dev_runs_narrative_failure_code"


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
    # installed (and valid) from 0083 either way. Nothing to undo here.
    pass
