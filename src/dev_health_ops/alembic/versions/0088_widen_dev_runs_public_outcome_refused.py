"""Widen dev_runs.public_outcome for CHAOS-3541 (install, NOT VALID).

Revision ID: 0088
Revises: 0087
Create Date: 2026-08-07 00:00:00

CHAOS-3541: adds ``PublicOutcome.REFUSED`` -- a genuinely prohibited request
(arbitrary execution, a write), distinct from ``denied`` (an authorization
claim) and from every evidence-gap outcome. ``ck_dev_runs_public_outcome``
(``models/dev_persistence.py``) is a hand-maintained mirror of the closed
``PublicOutcome`` enum and must widen in lockstep, or the first run that
reaches the new orchestrator branch fails at INSERT with a CHECK violation
the Python-level enum never saw coming -- the same defect class 0081 fixed
for ``dev_run_source_observations.source_class``.

Mirrors 0081/0082's own precedent (itself following 0074/0075's for this
exact constraint): install the widened CHECK ``NOT VALID`` here (metadata-
only, brief ``ACCESS EXCLUSIVE`` lock, no table scan) and validate it in a
separate migration (0089) via ``VALIDATE CONSTRAINT``, which takes only a
``SHARE UPDATE EXCLUSIVE`` lock and does not block concurrent reads/writes
on ``dev_runs`` -- an actively written, unbounded-growth table -- while it
scans. SQLite has no ``NOT VALID``/``VALIDATE CONSTRAINT`` concept --
``batch_alter_table`` already fully validates a CHECK constraint at
creation time, matching 0074's/0081's own SQLite arm.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0088"
down_revision: str | None = "0087"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_runs_public_outcome"
_TABLE = "dev_runs"

_PRIOR_OUTCOMES = (
    "answered",
    "answered_with_gaps",
    "needs_clarification",
    "not_found",
    "temporarily_unavailable",
    "unsupported",
    "denied",
    "failed",
)
_NEW_OUTCOMES = ("refused",)


def _public_outcome_check(outcomes: Sequence[str]) -> str:
    values = ", ".join(f"'{outcome}'" for outcome in outcomes)
    return f"public_outcome IS NULL OR public_outcome IN ({values})"


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    if is_postgres:
        # Constraint name/body are module-level literals; DDL cannot take bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_CONSTRAINT} CHECK "
            f"({_public_outcome_check((*_PRIOR_OUTCOMES, *_NEW_OUTCOMES))}) "
            "NOT VALID"
        )
    else:
        op.create_check_constraint(
            _CONSTRAINT,
            _TABLE,
            _public_outcome_check((*_PRIOR_OUTCOMES, *_NEW_OUTCOMES)),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.has_table(bind, _TABLE):
        new_outcomes_list = ", ".join(f"'{outcome}'" for outcome in _NEW_OUTCOMES)
        new_outcome_rows = bind.execute(
            sa.text(
                f"SELECT count(*) FROM {_TABLE} WHERE public_outcome IN "
                f"({new_outcomes_list})"
            )
        ).scalar()
        if new_outcome_rows:
            raise RuntimeError(
                f"refusing to downgrade {revision}: {_TABLE} has "
                f"{new_outcome_rows} row(s) with public_outcome in "
                f"{_NEW_OUTCOMES} -- narrowing the CHECK constraint back "
                "would either abort on a violation or require this "
                "migration to delete/mutate that data, which is not this "
                "migration's call to make"
            )
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT, _TABLE, _public_outcome_check(_PRIOR_OUTCOMES)
    )
