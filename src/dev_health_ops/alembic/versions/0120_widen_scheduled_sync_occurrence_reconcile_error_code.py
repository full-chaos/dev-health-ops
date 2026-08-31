"""Widen scheduled_sync_occurrences.reconcile_error_code for invalid_plan (CHAOS-4602, install, NOT VALID).

Revision ID: 0120
Revises: 0119
Create Date: 2026-08-30 00:00:01

Codex review (CHAOS-4602 gate round 6): occurrence_reconciler.go now
quarantines an occurrence immediately (never through deferOccurrence's
retry-with-backoff) when Materialize returns ErrInvalidPlan -- a
deterministic failure of the occurrence's own data (an unsupported mode, a
unit-cap overflow, a malformed manual selector) that reproduces identically
on every future attempt, the same reasoning identity_conflict already gets.
The closed vocabulary 0051 installed for reconcile_error_code
(ck_scheduled_sync_occurrence_reconcile_error_code) predates this code and
rejects the new 'invalid_plan' value outright.

Same NOT VALID / separate-VALIDATE split as 0074/0075, 0081/0082, 0088/0089,
0090/(next) -- scheduled_sync_occurrences grows one row per scheduled
occurrence, an unbounded-growth, actively-written table.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0120"
down_revision: str | None = "0119"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_scheduled_sync_occurrence_reconcile_error_code"
_TABLE = "scheduled_sync_occurrences"

_PRIOR_CODES = (
    "identity_conflict",
    "ineligible",
    "planner_error",
    "retry_exhausted",
)
_NEW_CODES = ("invalid_plan",)


def _error_code_check(codes: Sequence[str]) -> str:
    values = ", ".join(f"'{code}'" for code in codes)
    return f"reconcile_error_code IN ({values}) OR reconcile_error_code IS NULL"


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    if is_postgres:
        # Constraint name/body are module-level literals; DDL cannot take bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_CONSTRAINT} CHECK "
            f"({_error_code_check((*_PRIOR_CODES, *_NEW_CODES))}) "
            "NOT VALID"
        )
    else:
        op.create_check_constraint(
            _CONSTRAINT,
            _TABLE,
            _error_code_check((*_PRIOR_CODES, *_NEW_CODES)),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.has_table(bind, _TABLE):
        # Built from SQLAlchemy operators rather than a formatted string. Same
        # statement as the DDL in upgrade(), but the identifier is quoted by
        # the compiler and the codes travel as bound parameters instead of
        # being spliced into SQL text -- a SELECT can take bound parameters
        # (unlike upgrade()'s DDL, which cannot and carries a justified
        # nosemgrep), so this needs no suppression: the rule is satisfied,
        # not silenced.
        occurrences = sa.table(_TABLE, sa.column("reconcile_error_code"))
        new_code_rows = bind.execute(
            sa.select(sa.func.count())
            .select_from(occurrences)
            .where(occurrences.c.reconcile_error_code.in_(_NEW_CODES))
        ).scalar()
        if new_code_rows:
            raise RuntimeError(
                f"refusing to downgrade {revision}: {_TABLE} has "
                f"{new_code_rows} row(s) with reconcile_error_code in "
                f"{_NEW_CODES} -- narrowing the CHECK constraint back "
                "would either abort on a violation or require this "
                "migration to delete/mutate that data, which is not this "
                "migration's call to make"
            )
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(_CONSTRAINT, _TABLE, _error_code_check(_PRIOR_CODES))
