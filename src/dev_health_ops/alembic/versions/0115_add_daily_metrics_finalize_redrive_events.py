"""Add the finalize-redrive provenance ledger (CHAOS-4405).

Revision ID: 0115
Revises: 0114
Create Date: 2026-08-28 00:00:00

``dev-health-workerctl metrics finalize-redrive`` (CHAOS-4405) is the first
operator verb in this codebase that mutates a TERMINAL ``daily_metrics_runs``
row (``status='succeeded'`` -> ``'running'``) rather than only ever repairing
a non-terminal one -- it exists to re-run ``run_daily_metrics_finalize`` for
an already-completed historical day, backfilling the team-scope aggregates
(CHAOS-4399, #1963) a day finalized before that landed never wrote.

Team-lead's approval of that design (2026-08-28) was conditional. This table
is condition (1): a reset of a succeeded run is itself a state write and
must be traceable independent of River/outbox history, which only shows a
fresh dedupe key -- never WHY a run was reset or by whom. One row is written
in the SAME transaction as the reset and the fresh publish
(``PostgresStore.redriveOneFinalizeForRange``,
``internal/jobs/metrics/daily/redrive.go``), so a reset can never land
without its own provenance row -- either both commit, or the whole
transaction (reset + provenance + publish) rolls back and neither does.

This table also backs conditions (2) and (3): ``FindStrandedFinalizeRuns``
(CHAOS-4389's stranded-finalize detection) excludes a run with an ``'open'``
row here (see the redrive.go doc comment) -- while finalize-redrive's own
redriven job is plausibly still in flight, an unattended
``daily-finalize --all-complete`` sweep running concurrently must not
double-dispatch it. ``daily-redrive``'s own partition-level query is
unaffected by construction, independent of this table: the reset never
touches ``daily_metrics_partitions``, so a redriven run's partitions stay
100% 'succeeded' throughout and never match that command's own
``pending``/``failed`` eligibility.

``status``/``closed_at`` (team-lead escalation, 2026-08-28, on conditions
(2)/(3)): the FIRST version of this table made the exclusion permanent for
any run finalize-redrive ever touched -- a silent-failure shape: a redriven
finalize that fails would leave the run ``running`` with a ``failed``
finalization forever invisible to every recovery tool, including
``daily-finalize`` itself. ``CompleteFinalize``/``ReleaseFinalize``
(``internal/jobs/metrics/daily/postgres.go``) now close out this run's own
open row -- to ``'closed_succeeded'`` or ``'closed_failed'`` -- in the SAME
transaction as the run's own completion/failure transition, the moment that
SPECIFIC claim resolves. A closed row keeps the row (this table is still an
append-only audit trail: ``status``/``closed_at`` are the only columns any
code path ever updates, and only once, from ``'open'``), but
``FindStrandedFinalizeRuns`` stops excluding the run the instant it closes
``'closed_failed'`` -- an ordinary CHAOS-4389 sweep can pick it back up like
any other stranded run, exactly the recovery path that was missing.

Rows are otherwise append-only and narrow: no delete path anywhere in this
codebase, and no column but ``status``/``closed_at`` is ever updated after
insert. ``actor`` is a closed, bounded set (today just
``'finalize-redrive'``) rather than free text, matching every other bounded
column in this schema; ``reason`` carries the operator's own
``--review-evidence`` text verbatim, same as the CLI already requires for
every other operator-authorized repeat execution.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0115"
down_revision: str | None = "0114"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_UUID = postgresql.UUID(as_uuid=True)
_TABLE = "daily_metrics_finalize_redrive_events"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", _UUID, nullable=False),
        # The run this event was recorded for. Deliberately no foreign key to
        # daily_metrics_runs: this ledger's whole point is to outlive
        # whatever that row's own state does next (including, per condition
        # (2), a failed redriven finalize) -- it must never be at risk of
        # cascading away with it.
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("target_day", sa.Date(), nullable=False),
        # The run's own columns, captured BEFORE the reset this row records.
        # daily_metrics_runs.status/finalization_status are varchar(16); see
        # CHAOS-4043 (ops/AGENTS.md) for why a mismatched column width here
        # would silently defeat this table's whole purpose at parse time
        # against the real schema.
        sa.Column("prior_status", sa.String(length=16), nullable=False),
        sa.Column("prior_finalization_status", sa.String(length=16), nullable=False),
        # Closed vocabulary, not free text -- every other actor-identifying
        # column in this schema is bounded, and a typo'd actor string here
        # would silently break FindStrandedFinalizeRuns's exclusion query.
        sa.Column("actor", sa.String(length=32), nullable=False),
        # The operator's own --review-evidence text, verbatim -- never a
        # generic hardcoded string, matching this CLI's existing bar for
        # every other bulk operator action.
        sa.Column("reason", sa.Text(), nullable=False),
        # The nonce PublishRedriveFinalizeTx used for the SAME transaction's
        # publish, so this row and its outbox row are correlatable without a
        # timestamp-proximity guess.
        sa.Column("nonce", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        # Team-lead escalation on conditions (2)/(3): closed the instant the
        # SPECIFIC claim this redrive published resolves (CompleteFinalize
        # or ReleaseFinalize, internal/jobs/metrics/daily/postgres.go) --
        # never updated any other time, by any other code path. 'open' is
        # the only state FindStrandedFinalizeRuns' exclusion checks for.
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="open",
        ),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        # Short "dfre" prefix (daily_finalize_redrive_events), not the full
        # table name: Postgres identifiers cap at 63 bytes and
        # "ck_daily_metrics_finalize_redrive_events_prior_finalization_status"
        # alone overflows that -- confirmed against a real migrated schema,
        # not just SQL text (see ops/AGENTS.md's CHAOS-4043 note on exactly
        # this class of migration bug hiding behind a hand-authored test
        # schema that never enforces real identifier limits).
        sa.CheckConstraint(
            "actor IN ('finalize-redrive')",
            name="ck_dfre_actor",
        ),
        sa.CheckConstraint("prior_status <> ''", name="ck_dfre_prior_status"),
        sa.CheckConstraint(
            "prior_finalization_status <> ''",
            name="ck_dfre_prior_finalization_status",
        ),
        sa.CheckConstraint("reason <> ''", name="ck_dfre_reason"),
        sa.CheckConstraint(
            "status IN ('open', 'closed_succeeded', 'closed_failed')",
            name="ck_dfre_status",
        ),
        sa.CheckConstraint(
            "(status = 'open') = (closed_at IS NULL)",
            name="ck_dfre_closed_at_matches_status",
        ),
    )
    # FindStrandedFinalizeRuns's exclusion check (CHAOS-4389) is
    # `NOT EXISTS (SELECT 1 FROM daily_metrics_finalize_redrive_events WHERE
    # run_id = run.id AND status = 'open')`, run once per candidate row in
    # that scan; CompleteFinalize/ReleaseFinalize's own close-out UPDATE
    # looks up "the open row for this run_id" the same way. A partial index
    # on the 'open' subset keeps both an index lookup, not a sequential
    # scan, as the ledger grows -- closed rows (the overwhelming majority
    # once a run's redrive concludes either way) never need to be scanned
    # by either.
    op.create_index(
        "ix_dfre_run_id_open",
        _TABLE,
        ["run_id"],
        postgresql_where=sa.text("status = 'open'"),
    )


def downgrade() -> None:
    op.drop_index("ix_dfre_run_id_open", table_name=_TABLE)
    op.drop_table(_TABLE)
