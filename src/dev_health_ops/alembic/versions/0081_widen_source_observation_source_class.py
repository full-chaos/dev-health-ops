"""Widen dev_run_source_observations.source_class for CHAOS-3297 stack #3 (install, NOT VALID).

Revision ID: 0081
Revises: 0080
Create Date: 2026-08-03 00:00:00

CHAOS-3337: CHAOS-3297 stack #3 (merged #1387) wires four new plan-governed
intents that emit ``SourceClass.HEALTH_PROFILE``/``SourceClass.
DEFICIENCY_INVENTORY`` observations, but ``ck_dev_run_source_observations_
source_class`` -- a hand-maintained mirror of the closed ``SourceClass``
enum, separate from ``persistence.service._SOURCE_CLASSES`` (itself also
missing these two values, fixed in the same changeset) -- was never
updated. Every one of those four intents' plan-governed runs crashed the
instant ``DevPersistenceService.append_source_observation`` tried to
INSERT: the database rejected the row with this CHECK constraint before
the Python-level allowlist even got a chance to raise its own (also
missing) validation error.

Codex full-branch review (CHAOS-3337, 2026-08-03) HIGH finding: a plain
``drop_constraint`` + ``create_check_constraint`` validates the replacement
CHECK immediately, under the ``ACCESS EXCLUSIVE`` lock the drop+add already
holds -- a full-table scan of ``dev_run_source_observations`` (an actively
written, unbounded-growth table) blocking every concurrent read and write
for the scan's duration. 0074/0075 already establish this repo's own
precedent for exactly this shape (``ck_dev_runs_contract_generation``/
``ck_dev_runs_public_outcome``): install the widened constraint ``NOT
VALID`` here (metadata-only, no scan, brief lock), and validate it in a
SEPARATE migration (0082) via ``VALIDATE CONSTRAINT``, which takes only a
``SHARE UPDATE EXCLUSIVE`` lock and does not block concurrent reads/writes
while it scans. SQLite has no ``NOT VALID``/``VALIDATE CONSTRAINT``
concept -- ``batch_alter_table`` already fully validates a CHECK
constraint at creation time, matching 0074's own SQLite arm.

Codex HIGH finding 2: the original downgrade recreated the pre-widened
constraint blind. That is only safe BEFORE any affected intent has ever
run; once one has, rows carrying ``health_profile``/``deficiency_inventory``
exist, and narrowing the CHECK back would either abort the migration (a
CHECK violation on existing data) or -- worse -- require this migration to
silently delete/mutate that data to make room for its own rollback, which
is not this migration's call to make. Mirrors 0074's own downgrade posture
exactly: preflight-and-refuse (raise) if any such row exists, rather than
an authorized cleanup baked into a schema migration.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0081"
down_revision: str | None = "0080"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_CONSTRAINT = "ck_dev_run_source_observations_source_class"
_TABLE = "dev_run_source_observations"

_PRIOR_SOURCE_CLASSES = (
    "status_change",
    "work_item",
    "work_graph",
    "pull_request",
    "code_change",
    "review",
    "ci_run",
    "test_report",
    "deployment",
    "incident",
    "operational_control",
    "source_health",
)
_NEW_SOURCE_CLASSES = ("health_profile", "deficiency_inventory")


def _source_class_check(source_classes: Sequence[str]) -> str:
    values = ", ".join(f"'{source_class}'" for source_class in source_classes)
    return f"source_class IN ({values})"


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    if is_postgres:
        # Constraint name/body are module-level literals; DDL cannot take bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_CONSTRAINT} CHECK "
            f"({_source_class_check((*_PRIOR_SOURCE_CLASSES, *_NEW_SOURCE_CLASSES))}) "
            "NOT VALID"
        )
    else:
        op.create_check_constraint(
            _CONSTRAINT,
            _TABLE,
            _source_class_check((*_PRIOR_SOURCE_CLASSES, *_NEW_SOURCE_CLASSES)),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.has_table(bind, _TABLE):
        new_classes_list = ", ".join(f"'{c}'" for c in _NEW_SOURCE_CLASSES)
        new_class_rows = bind.execute(
            sa.text(
                f"SELECT count(*) FROM {_TABLE} WHERE source_class IN "
                f"({new_classes_list})"
            )
        ).scalar()
        if new_class_rows:
            raise RuntimeError(
                f"refusing to downgrade {revision}: {_TABLE} has "
                f"{new_class_rows} row(s) with source_class in "
                f"{_NEW_SOURCE_CLASSES} -- narrowing the CHECK constraint "
                "back would either abort on a violation or require this "
                "migration to delete/mutate that data, which is not this "
                "migration's call to make"
            )
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT, _TABLE, _source_class_check(_PRIOR_SOURCE_CLASSES)
    )
