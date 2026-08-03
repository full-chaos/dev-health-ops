"""DB-enforced closed vocabulary for dev_runs.narrative_failure_code (CHAOS-3297 codex NO-SHIP finding round 1, MEDIUM #3).

Revision ID: 0083
Revises: 0082
Create Date: 2026-08-02 00:00:01

``narrative_failure_code`` (added nullable, unconstrained, in 0078 -- no
producer existed yet) is now written by ``persistence.service.update_run``
whenever the deterministic-fallback narrative path records why provider
synthesis fell back. Its vocabulary is
``contracts_v2.base.NarrativeFailureCode``, the same closed set the
``answer.narrative_fallback`` stream event and the persistence-layer
Python check enforce (see both call sites' comments). Python-only
enforcement leaves an ORM write that bypasses ``update_run``, a bulk
write, or a raw connection free to persist an invented code -- exactly the
write-shape gap migration 0080's docstring describes for the two payload
columns, addressed here the same way: at the database boundary every
write path must cross.

A CHECK constraint (not a trigger, unlike 0080's JSONB payload
cross-checks) suffices: this is a scalar column compared against a fixed
list, not a nested-key structural validation.

Every pre-existing row is guaranteed to satisfy this constraint --
``narrative_failure_code`` has had no producer since 0078 introduced the
column, so it is NULL on every row in every database this migration runs
against, and the constraint explicitly allows NULL. Installed ``NOT
VALID`` anyway, validated in 0084, mirroring 0074/0075's and 0081/0082's
own precedent: a direct (non-``NOT VALID``) ``ADD CONSTRAINT`` still takes
an ``ACCESS EXCLUSIVE`` lock for the scan's duration regardless of whether
a violation is possible, so the split avoids blocking concurrent writes on
``dev_runs`` even though the outcome is a foregone conclusion.

SQLite's ``batch_alter_table`` fully validates a CHECK constraint at
creation time (no ``NOT VALID`` concept), so its arm below installs the
constraint outright.

Additive; the downgrade drops the constraint, leaving the column exactly
as 0078 left it.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0083"
down_revision: str | None = "0082"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "dev_runs"
_CONSTRAINT = "ck_dev_runs_narrative_failure_code"
_VALUES = (
    "'provider_timeout', 'provider_refused', 'provider_empty_content', "
    "'provider_schema_violation', 'provider_output_budget_exceeded', "
    "'provider_unsafe_content', 'narrative_grounding_failed', "
    "'provider_unknown_failure'"
)
_CHECK_SQL = f"narrative_failure_code IS NULL OR narrative_failure_code IN ({_VALUES})"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # Constraint/table names and the CHECK body are module-level
        # literals; DDL identifiers/bodies cannot take bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_CONSTRAINT} "
            f"CHECK ({_CHECK_SQL}) NOT VALID"
        )
    else:
        with op.batch_alter_table(_TABLE) as batch:
            batch.create_check_constraint(_CONSTRAINT, _CHECK_SQL)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            f"ALTER TABLE {_TABLE} DROP CONSTRAINT {_CONSTRAINT}"
        )  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
    else:
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_constraint(_CONSTRAINT, type_="check")
