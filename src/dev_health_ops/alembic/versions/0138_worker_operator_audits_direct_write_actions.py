"""Audit the direct-write operator verbs in worker_operator_audits.

Revision ID: 0138
Revises: 0137

The ``dho workers`` verbs whose write does not go through the operator
service's own backends (the metrics, workgraph, investment, providersync and
sync-dispatch-outbox writers) now write one audit row each, through the same
audited path as the service's own verbs. This widens the action check to
exactly the Go service's ``joboperator.AuditedActions``; a Go test reads
``_ACTIONS`` below and fails if the two differ. Some of the new action names
are longer than 32 characters, so ``action`` widens from varchar(32) to
varchar(64). No data changes.

Downgrade restores the 0137 check and varchar(32). It fails while rows with
one of the added actions exist.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0138"
down_revision: str | None = "0137"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_worker_operator_audits_action"

_PREVIOUS_ACTIONS = (
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
    "providersync.retire_linear_pseudo_projects",
    "providersync.retire_stale_linear_project_ownership",
    "sync_dispatch_outbox.close_terminal_backlog",
    "workgraph.manual_trigger",
    "investment.manual_trigger",
    "ledger.repair",
    "metrics.daily_start",
    "metrics.daily_redrive",
    "metrics.daily_finalize",
    "metrics.finalize_redrive",
    "metrics.partition_recompute",
    "metrics.remaining_start",
    "metrics.remaining_trigger_backstop",
    "metrics.remaining_redrive",
    "external_recompute.replay",
)


def _replace(actions: tuple[str, ...], length: int) -> None:
    quoted = ", ".join(f"'{action}'" for action in actions)
    with op.batch_alter_table("worker_operator_audits") as batch_op:
        batch_op.drop_constraint(_CONSTRAINT, type_="check")
        batch_op.alter_column(
            "action", type_=sa.String(length=length), existing_nullable=False
        )
        batch_op.create_check_constraint(_CONSTRAINT, f"action IN ({quoted})")


def upgrade() -> None:
    _replace(_ACTIONS, 64)


def downgrade() -> None:
    _replace(_PREVIOUS_ACTIONS, 32)
