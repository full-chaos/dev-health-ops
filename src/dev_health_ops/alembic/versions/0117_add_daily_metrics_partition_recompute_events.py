"""Add the partition-recompute provenance ledger (CHAOS-4459).

Revision ID: 0117
Revises: 0116
Create Date: 2026-08-29 00:00:00

``dev-health-workerctl metrics partition-recompute`` (CHAOS-4459) is the
partition-level counterpart to 0116's finalize-redrive ledger: a
``daily_metrics_run`` whose partitions are ALL ``status='succeeded'`` can
still have wrong family output if the writer that computed it has since
been fixed (CHAOS-4341: the native ``repo_user_commit`` executor wrote
``org_id=""`` on ``repo_metrics_daily``/``user_metrics_daily``/
``commit_metrics`` before PR #1960 -- historical partitions computed under
the old writer stay wrong forever, because ``daily-redrive``'s own
eligibility query only ever targets stranded/failed partitions, never a
'succeeded' one).

This table is 0116's condition (1) applied to the partition reset instead
of the finalize reset: a reset of a terminal (``status='succeeded'``) run
is itself a state write and must be traceable independent of River/outbox
history, which only shows a fresh dedupe key -- never WHY a run was reset
or by whom. One row is written in the SAME transaction as the reset and the
fresh publish (``PostgresStore.redriveOnePartitionsForRange``,
``internal/jobs/metrics/daily/partition_recompute.go``), so a reset can
never land without its own provenance row -- either both commit, or the
whole transaction (reset + provenance + publish) rolls back and neither
does.

Deliberately simpler than 0116's table: append-only, no ``status``/
``closed_at`` open/closed lifecycle. 0116 needed that lifecycle so
``FindStrandedFinalizeRuns``'s unattended sweep could tell "this run's
redriven finalize is plausibly still in flight" apart from "it settled" and
avoid double-dispatching it. This verb has no equivalent unattended sweep
today -- every invocation is an explicit, human-authorized ``--org --from
--to`` call, and the transaction that resets a run's partitions back to
'pending'/'running' makes it immediately ineligible for a second reset by
the SAME query (``RedrivePartitionsForRange``'s own candidate scan requires
``run.status = 'succeeded'``, which the reset just changed) -- so the open/
closed lifecycle's whole purpose does not apply yet. Tracked as follow-up
scope if/when an unattended reconciler is added for this class, same as
0116's own history (its first version was open-only too; the lifecycle was
added later, escalated once a concrete double-dispatch risk existed).

Rows are append-only: no delete path anywhere in this codebase, and no
column is ever updated after insert. ``actor`` is a closed, bounded set
(today just ``'partition-recompute'``) rather than free text, matching
0116's own ``actor`` column; ``reason`` carries the operator's own
``--review-evidence`` text verbatim.

Also widens ``daily_metrics_runs.generation`` from ``varchar(64)`` to
``text`` (codex review, P2, on PR #1990). The reset this ticket's verb
performs needs a genuinely fresh compatibility-bridge execution-ledger
identity (``uuid5(run_id, family, generation, scope_digest)``), which means
``generation`` itself must change -- but Python's
``_LATEST_DAILY_METRICS_RUN_SQL`` (``workers/recommendations_tasks.py``)
finds a day's AUTHORITATIVE run by ``generation LIKE
'fixed-schedule:daily_metrics_fanout:%'``, a classification prefix a fresh
value must still match. ``PostgresStore.redriveOnePartitionsForRange``
therefore APPENDS rather than replaces:
``<original generation>#recompute:<nonce>``. The widest original value
(the fan-out prefix, ~57 bytes) plus the appended suffix (``#recompute:`` +
a 36-byte UUID) does not fit ``varchar(64)`` -- confirmed by direct
arithmetic, not assumed.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0117"
down_revision: str | None = "0116"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_UUID = postgresql.UUID(as_uuid=True)
_TABLE = "daily_metrics_partition_recompute_events"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", _UUID, nullable=False),
        # Deliberately no foreign key to daily_metrics_runs, matching 0116's
        # finalize_redrive_events precedent: this ledger must outlive
        # whatever the run row's own state does next.
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("target_day", sa.Date(), nullable=False),
        # Which metrics.daily family this invocation was recomputing for
        # (audit/intent only -- see SupportedPartitionRecomputeFamilies'
        # doc comment on why the reset itself is not narrower than "every
        # family in the partition").
        sa.Column("family", sa.String(length=64), nullable=False),
        # The run's own columns, captured BEFORE the reset this row
        # records. daily_metrics_runs.status is varchar(16) -- see
        # CHAOS-4043 (ops/AGENTS.md) for why a mismatched column width here
        # would silently defeat this table's whole purpose at parse time
        # against the real schema. prior_generation is `text`, matching
        # daily_metrics_runs.generation's own widened type below: a day
        # recomputed more than once captures the PREVIOUS pass's own
        # "<base>#recompute:<nonce>" value as this pass's prior_generation,
        # which already exceeds 64 bytes (codex review round 2, P2).
        sa.Column("prior_status", sa.String(length=16), nullable=False),
        sa.Column("prior_generation", sa.Text(), nullable=False),
        sa.Column("actor", sa.String(length=32), nullable=False),
        # The operator's own --review-evidence text, verbatim.
        sa.Column("reason", sa.Text(), nullable=False),
        # The nonce PublishRedrivePartitionTx used for the SAME
        # transaction's publish, so this row and its outbox rows are
        # correlatable without a timestamp-proximity guess.
        sa.Column("nonce", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
        # Short "dpre" prefix (daily_partition_recompute_events), matching
        # 0116's "dfre" precedent and its identical 63-byte identifier-limit
        # reasoning.
        sa.CheckConstraint(
            "actor IN ('partition-recompute')",
            name="ck_dpre_actor",
        ),
        sa.CheckConstraint("family <> ''", name="ck_dpre_family"),
        sa.CheckConstraint("prior_status <> ''", name="ck_dpre_prior_status"),
        sa.CheckConstraint("prior_generation <> ''", name="ck_dpre_prior_generation"),
        sa.CheckConstraint("reason <> ''", name="ck_dpre_reason"),
    )
    op.create_index("ix_dpre_run_id", _TABLE, ["run_id"])

    # See the module doc comment (codex review, P2): a partition-recompute
    # reset appends "#recompute:<nonce>" to the run's existing generation
    # rather than replacing it, so classification prefixes
    # (_LATEST_DAILY_METRICS_RUN_SQL's `LIKE 'fixed-schedule:...'`) keep
    # matching. The widest original value plus that suffix does not fit
    # varchar(64).
    op.alter_column(
        "daily_metrics_runs",
        "generation",
        type_=sa.Text(),
        existing_type=sa.String(length=64),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Deliberately does NOT narrow daily_metrics_runs.generation back to
    # varchar(64) (codex review round 2, P1): once the partition-recompute
    # verb has actually run against this database, live rows carry a
    # "<original>#recompute:<nonce>" value that already exceeds 64 bytes --
    # narrowing the column would fail outright with "value too long for
    # type character varying(64)" before it ever reached dropping this
    # migration's own table, blocking rollback in exactly the environment
    # (post-use) where a downgrade is most likely to be run. A widened
    # text column is a strict superset of varchar(64) for every existing
    # reader (none of them assumes a fixed width -- see this ticket's own
    # PR body for the full Go/Python audit), so leaving it widened is safe
    # and forward-compatible; only this migration's own new table is
    # reverted.
    op.drop_index("ix_dpre_run_id", table_name=_TABLE)
    op.drop_table(_TABLE)
