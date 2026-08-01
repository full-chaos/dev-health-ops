"""Validate the NOT VALID dev_runs CHECK constraints added in 0074.

Revision ID: 0075
Revises: 0074
Create Date: 2026-07-31 00:00:01

``VALIDATE CONSTRAINT`` takes only a ``SHARE UPDATE EXCLUSIVE`` lock (does not
block concurrent reads/writes) while it scans existing rows. Both
constraints are trivially satisfiable by every pre-existing row: 0074's
``ADD COLUMN ... DEFAULT 'v1'`` makes every row already ``contract_generation
= 'v1'``, and ``public_outcome`` is NULL on every pre-existing row (the
constraint explicitly allows NULL). This step is therefore expected to be
fast, but is kept as its own migration -- rather than folded into 0074 -- so
a slow validation on an unexpectedly large table cannot block table
creation, matching the 0068/0069 schema-then-index precedent.

No-op on non-PostgreSQL backends: SQLite's ``batch_alter_table`` already
fully validates a CHECK constraint at creation time in 0074.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0075"
down_revision: str | None = "0074"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONTRACT_GENERATION_CK = "ck_dev_runs_contract_generation"
_PUBLIC_OUTCOME_CK = "ck_dev_runs_public_outcome"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    # Constraint name is a module-level literal; DDL identifiers cannot be bound parameters.
    op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE dev_runs VALIDATE CONSTRAINT {_CONTRACT_GENERATION_CK}"
    )
    op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"ALTER TABLE dev_runs VALIDATE CONSTRAINT {_PUBLIC_OUTCOME_CK}"
    )


def downgrade() -> None:
    # VALIDATE CONSTRAINT has no reversible state: the constraint remains
    # installed (and valid) from 0074 either way. Nothing to undo here.
    pass
