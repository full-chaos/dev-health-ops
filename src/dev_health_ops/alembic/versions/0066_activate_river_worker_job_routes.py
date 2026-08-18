"""Move the checked-in worker job kinds to the River transport.

Revision ID: 0066
Revises: 0065

This is the CHAOS-3033 Celery-to-Go cutover.  23 of the 24 checked-in kinds move
to ``go_default`` in the same release, which makes ``river`` the checked-in route
and leaves ``celery`` as the declared rollback route.

``sync.provider_unit`` is the exception and stays on its canary route.  See
``_PROMOTABLE_TRANSPORTS`` below for why.  The canary route lets
``ProviderUnitRouteSwitches`` confine River to the pairs whose per-route
``WORKER_*_ENABLED`` switch is on, while every other pair still dispatches
through Celery; promoting the kind to plain ``river`` would route all 59 pairs
at once, regardless of those switches.

Why the route rows are seeded here rather than promoted one at a time with
``dev-health-workerctl job-routes apply``:

``jobroute.Controller.ApplyCheckedIn`` consults a Celery quiescer whenever it
moves a row off its rollback route, and the only production implementation
(``PostgresCelerySyncProviderQuiescer``) accepts ``sync.provider_unit`` and
rejects every other kind with ``ErrInvalidConfiguration``.  A per-kind
quiescence prover would be required to walk 24 kinds forward individually.  It
is not required for a wholesale cutover: ``ApplyCheckedIn`` returns early when a
row already sits on its checked-in route and is not paused, before the quiescer
branch is reached, so after this migration that command is a verifying no-op for
every kind.  Quiescence exists to sequence a *live* handoff between two running
execution owners; this cutover stops Celery instead of interleaving with it.

The producer-side interlock is preserved, not bypassed.
``resolve_worker_job_route`` rejects any row whose transport is outside
``{policy.route, 'celery'}``, so ``river`` only becomes a legal value because
the migration-state change ships in this same release.  A database that runs
this migration without that code still fails closed rather than dispatching to
a transport its producer does not recognise.

Ordering requirement: Go consumers must be running before this commits.  A
kind routed to River stages envelopes in ``worker_job_outbox`` for the
reconciler's relay to insert into River; with no reconciler and no worker for
the owning queue, work accumulates unexecuted.  Celery must also be drained
before this runs, because a Celery message already in the broker is invisible
to every check here.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Final

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import RowMapping
from sqlalchemy.sql.selectable import TableClause

revision: str = "0066"
down_revision: str | None = "0065"
branch_labels = ("river_cutover",)
depends_on = None

_RIVER_TRANSPORT = "river"
_ROLLBACK_TRANSPORT = "celery"
_PROMOTABLE_TRANSPORTS = frozenset({_ROLLBACK_TRANSPORT})
_CUTOVER_OPT_IN_ENV: Final = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"
_CUTOVER_OPT_IN_VALUE: Final = "1"

# sync.provider_unit is deliberately NOT migrated here and keeps the
# river_canary row that 0061 seeded.
#
# When this migration was written, launchdarkly/feature-flags was the only
# entry in contracts/provider-matrix/v1/matrix.json with route_ready true. That
# is no longer the case -- all 59 pairs are now route_ready -- but the reason
# this kind stays on canary is unchanged: route readiness is not the same as
# being enabled. The canary route is what lets ProviderUnitRouteSwitches
# confine River to the pairs whose per-route WORKER_*_ENABLED switch is on,
# every one of which still defaults off.
#
# Promoting this kind to plain river would route all 59 pairs to the Go handler
# in one step, bypassing those switches and the per-route parity evidence they
# gate. That is a separate reviewed decision, not part of a wholesale cutover.
# Leaving it at river_canary is not an oversight or a deferral of cleanup: it is
# the only state in which this kind currently works.
#
# Its durable row is never read here, so whatever an operator decided stays
# intact. On a fresh database 0061 and 0064 leave it on celery at generation 1
# precisely so the canary is an explicit operator decision, and this kind is the
# one that ``dev-health-workerctl job-routes apply`` can actually promote:
# PostgresCelerySyncProviderQuiescer serves sync.provider_unit and rejects every
# other kind, which is the same limitation that makes the other 23 rows worth
# seeding here rather than promoting one at a time.

# Pinned rather than derived from the contract tree: a migration records the
# decision taken at one revision, so a later contract edit must not silently
# change what this revision did.  These are the 24 kinds seeded by 0064 minus
# sync.provider_unit, which stays on its canary route for the reason above.
_KINDS = (
    "investment.chunk",
    "investment.dispatch",
    "investment.finalize",
    "investment.materialize",
    "metrics.daily_dispatch",
    "metrics.daily_finalize",
    "metrics.daily_partition",
    "metrics.remaining.capacity",
    "metrics.remaining.complexity",
    "metrics.remaining.dora",
    "metrics.remaining.extra_metrics",
    "metrics.remaining.membership_backfill",
    "metrics.remaining.recommendations",
    "metrics.remaining.release_impact",
    "metrics.remaining.team_metrics",
    "operational.billing_notification",
    "operational.webhook_delivery",
    "report.execute_on_demand",
    "report.execute_scheduled",
    "sync.team_autoimport",
    "system.heartbeat",
    "system.retention_cleanup",
    "workgraph.build",
)


def _routes() -> TableClause:
    return sa.table(
        "worker_job_routes",
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def _existing_rows(routes: TableClause) -> dict[str, RowMapping]:
    bind = op.get_bind()
    return {
        row["job_kind"]: row
        for row in bind.execute(
            sa.select(
                routes.c.job_kind,
                routes.c.transport,
                routes.c.paused,
                routes.c.generation,
            )
            .where(routes.c.job_kind.in_(_KINDS))
            .with_for_update()
        ).mappings()
    }


def _retarget(target: str, accepted: frozenset[str]) -> None:
    """Point every pinned kind at ``target``, bumping the operator generation.

    Validation runs to completion before the first write so a conflicting row
    cannot leave the table half-migrated with two execution owners across
    different kinds.  The UPDATE takes the same row locks
    ``resolve_worker_job_route`` waits on with ``FOR SHARE``, so a producer that
    already read the old transport finishes staging before this commits.
    """

    routes = _routes()
    existing = _existing_rows(routes)

    missing = [kind for kind in _KINDS if kind not in existing]
    if missing:
        # 0064 seeds a row for every checked-in kind, including deferred ones.
        # A gap here means the baseline never ran or a row was deleted, and
        # inserting one now would guess at an operator decision.
        raise RuntimeError(f"worker job routes are not seeded: {sorted(missing)}")

    conflicting = sorted(
        kind
        for kind, row in existing.items()
        if row["transport"] not in accepted and row["transport"] != target
    )
    if conflicting:
        raise RuntimeError(f"worker job routes are not safe to retarget: {conflicting}")

    pending = [
        kind
        for kind, row in existing.items()
        if row["transport"] != target or bool(row["paused"])
    ]
    if not pending:
        return

    op.get_bind().execute(
        routes.update()
        .where(routes.c.job_kind.in_(sorted(pending)))
        .values(
            transport=target,
            paused=False,
            generation=routes.c.generation + 1,
            updated_at=datetime.now(UTC),
        )
    )


def _cutover_is_authorized() -> bool:
    return os.getenv(_CUTOVER_OPT_IN_ENV) == _CUTOVER_OPT_IN_VALUE


def upgrade() -> None:
    if not _cutover_is_authorized():
        raise RuntimeError(
            f"refusing the Celery-to-River cutover; set {_CUTOVER_OPT_IN_ENV}=1 "
            "only after every retargeted job kind has route-readiness and parity evidence"
        )
    _retarget(_RIVER_TRANSPORT, _PROMOTABLE_TRANSPORTS)


def downgrade() -> None:
    """Return every kind to its declared Celery rollback route.

    This does NOT prove quiescence.  ``jobroute.Controller.Rollback`` refuses to
    move a route while ``worker_job_outbox`` holds pending or claimed rows for
    the kind or ``worker_job_runs`` shows it running, so that two runtimes never
    own the same work; plain SQL cannot make that guarantee.  Stop the Go
    workers and the reconciler before downgrading, or use
    ``dev-health-workerctl job-routes rollback`` per kind to get the proof.
    """

    _retarget(_ROLLBACK_TRANSPORT, frozenset({_RIVER_TRANSPORT, "river_canary"}))
