"""Widen dev_run_source_observations.source_class for CHAOS-3297 stack #3.

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

No existing row can carry either new value (every attempt to write one
failed at INSERT, by construction of the bug this migration closes), so
the downgrade needs no data cleanup -- unlike 0072's widening of
``ck_dev_runs_state``, which had to migrate live rows in the new states
before narrowing back.
"""

from __future__ import annotations

from collections.abc import Sequence

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
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT,
        _TABLE,
        _source_class_check((*_PRIOR_SOURCE_CLASSES, *_NEW_SOURCE_CLASSES)),
    )


def downgrade() -> None:
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.create_check_constraint(
        _CONSTRAINT, _TABLE, _source_class_check(_PRIOR_SOURCE_CLASSES)
    )
