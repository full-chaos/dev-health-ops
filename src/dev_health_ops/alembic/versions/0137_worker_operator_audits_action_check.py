"""Allow every audited operator action in worker_operator_audits.

Revision ID: 0137
Revises: 0136

The action check (0061) allowed seven actions, but the operator service also
audits ``routes.pause``, ``routes.drain``, ``routes.resume`` and
``workers.undrain``. Their audit insert was refused, so those commands failed
with ``audit_unavailable`` before reaching the route controller. This widens
the check to exactly the Go service's ``joboperator.AuditedActions``; a Go
test reads ``_ACTIONS`` below and fails if the two differ. Check constraint
only; no data changes.

Downgrade restores the 0061 check. It fails while rows with one of the four
added actions exist.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0137"
down_revision: str | None = "0136"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_worker_operator_audits_action"

_PREVIOUS_ACTIONS = (
    "jobs.cancel",
    "jobs.retry",
    "queues.pause",
    "queues.resume",
    "workers.drain",
    "job_routes.apply_checked_in",
    "job_routes.rollback",
)

_ACTIONS = (
    "jobs.cancel",
    "jobs.retry",
    "queues.pause",
    "queues.resume",
    "workers.drain",
    "workers.undrain",
    "job_routes.apply_checked_in",
    "job_routes.rollback",
    "routes.pause",
    "routes.drain",
    "routes.resume",
)


def _replace(actions: tuple[str, ...]) -> None:
    quoted = ", ".join(f"'{action}'" for action in actions)
    with op.batch_alter_table("worker_operator_audits") as batch_op:
        batch_op.drop_constraint(_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(_CONSTRAINT, f"action IN ({quoted})")


def upgrade() -> None:
    _replace(_ACTIONS)


def downgrade() -> None:
    _replace(_PREVIOUS_ACTIONS)
