"""Validate the NOT VALID dev_answer_frames.public_outcome CHECK constraint added in 0090.

Revision ID: 0091
Revises: 0090
Create Date: 2026-08-07 00:00:03

``VALIDATE CONSTRAINT`` takes only a ``SHARE UPDATE EXCLUSIVE`` lock (does
not block concurrent reads/writes) while it scans existing rows. Every
pre-existing row in ``dev_answer_frames`` already satisfies the widened
constraint (it satisfied the NARROWER pre-0090 constraint, which 0090's
widened version is a strict superset of), so this step is expected to be
fast -- but is kept as its own migration, mirroring 0075's/0082's/0089's own
precedent, so a slow validation on an unexpectedly large table cannot block
0090's constraint install.

No-op on non-PostgreSQL backends: SQLite's ``batch_alter_table`` (0090's
SQLite arm) already fully validates a CHECK constraint at creation time.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0091"
down_revision: str | None = "0090"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_answer_frames_public_outcome"
_TABLE = "dev_answer_frames"


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
    # installed (and valid) from 0090 either way. Nothing to undo here.
    pass
