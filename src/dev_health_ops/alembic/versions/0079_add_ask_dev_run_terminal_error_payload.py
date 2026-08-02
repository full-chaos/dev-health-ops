"""Persist the exact terminal v1 DevError payload on dev_runs (CHAOS-3297 Codex review HIGH #1).

Revision ID: 0079
Revises: 0078
Create Date: 2026-08-02 00:00:00

Before this revision, an idempotent replay of a terminal error run had to
*reconstruct* a v1 ``DevError`` from the persisted ``dev_answer_frame.v1``
row (``preflight_outcomes.project_preflight_error``) whenever no answer
message existed. That reconstruction is a **fixed, outcome-keyed** table
(``compat._ERROR_OUTCOME_CODES``): it is correct for a genuinely
preflight-sourced termination (the same table built the live copy), but
``orchestrator.run()``'s own ~30 non-preflight terminal calls carry today's
richer v1 code taxonomy verbatim in ``run.safe_error_code`` -- and, more
than the *code*, their live ``safe_message``/``remediation`` copy is
producer-authored at the call site, not one of the five canonical no-answer
sentences the reconstruction table can produce. When the reconstructed code
happened to equal ``run.safe_error_code`` (e.g. ``scope_not_found`` on both
sides), the router's CHAOS-3297 coherence guard let the *reconstructed*
frame-projected copy through unchanged even though its ``safe_message`` and
``remediation`` differ from what the live run actually streamed --
serving different bytes for the identical idempotency key.

``terminal_error_payload`` is the actual fix: the exact validated v1
``DevError`` object (``PersistenceRunRecorder.terminal``) is now persisted
once, at terminal time, alongside the frame. A replay for any run created
after this revision reads it back and reuses it verbatim -- no
reconstruction, no divergence, regardless of whether the terminating code
came from the preflight or from the orchestrator's own ``error()``/
``_provider_error`` producers. The frame-reconstruction path
(``router._replayed_result``) is kept as a compatibility fallback for rows
persisted before this column existed, which never got a value written here.

Nullable JSON, no CHECK constraint -- mirrors 0078's precedent for a
diagnostic-only column added without back-filling every prior row. NULL for
every run that predates this revision and for every non-error terminal
(``answer_id`` is set instead).

Additive in both directions.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0079"
down_revision: str | None = "0078"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "dev_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("terminal_error_payload", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "terminal_error_payload")
