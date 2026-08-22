"""Fail-closed durable job transport selection for transitional producers."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dev_health_ops.models.worker_job_route import WorkerJobRoute

from .job_contracts import ContractDecodeError, load_migration_jobs

CELERY_ROUTE = "celery"
SHADOW_ROUTE = "shadow"
RIVER_CANARY_ROUTE = "river_canary"
RIVER_ROUTE = "river"
_ROUTES = frozenset({CELERY_ROUTE, SHADOW_ROUTE, RIVER_CANARY_ROUTE, RIVER_ROUTE})

#: The durable ``sync.provider_unit`` routes under which River owns provider
#: units. Any other legal value -- in practice ``celery``, the declared
#: rollback route -- means River does NOT own them.
#:
#: CHAOS-4054 step 4 deleted the Celery dispatch plane, so "River does not own
#: them" no longer has a second runtime to mean. A producer that sees one of
#: those routes must therefore fail closed rather than stage work: the Go
#: outbox relay resolves this same row every step and RELEASES the claim for a
#: Celery route (``internal/joboutbox/relay.go:253-263``), so staging anyway
#: parks the unit in ``dispatching`` behind an outbox row nothing will ever
#: deliver, holding a DispatchGuard slot with no lease to expire.
PROVIDER_UNIT_OUTBOX_ROUTES = frozenset({RIVER_CANARY_ROUTE, RIVER_ROUTE})


class WorkerJobRouteError(RuntimeError):
    """Value-free route rejection safe for provider-facing logs."""


def _locked_route_statement(kind: str):
    return (
        select(WorkerJobRoute)
        .where(WorkerJobRoute.job_kind == kind)
        .with_for_update(read=True)
    )


def resolve_worker_job_route(session: Session, kind: str) -> str:
    """Resolve one row against checked-in migration policy.

    A missing, paused, duplicated, or drifted row never falls back to Celery:
    that would allow concurrent execution owners during a control-plane fault.
    """

    policies = tuple(job for job in load_migration_jobs() if job.kind == kind)
    if len(policies) != 1:
        raise WorkerJobRouteError("worker job route policy is unavailable")
    try:
        # Keep this shared row lock in the producer's outbox transaction. A
        # rollback takes FOR UPDATE on the same row, so it cannot report
        # quiescence while a producer that observed the old route can still
        # stage work after the route change commits.
        route = session.scalar(_locked_route_statement(kind))
    except Exception as error:
        raise WorkerJobRouteError("worker job route store is unavailable") from error
    if route is None or route.job_kind != kind or route.transport not in _ROUTES:
        raise WorkerJobRouteError("worker job route is unavailable")
    policy = policies[0]
    # MigrationJob intentionally exposes only the current route. During every
    # coexistence state its rollback route is Celery; terminal Celery removal
    # never reaches this producer module.
    if route.transport not in {policy.route, CELERY_ROUTE}:
        raise WorkerJobRouteError("worker job route drifts from checked-in policy")
    if route.paused:
        raise WorkerJobRouteError("worker job route is paused")
    return route.transport


def route_requires_outbox(route: str) -> bool:
    if route not in _ROUTES:
        raise ContractDecodeError("worker job route is unsupported")
    return route != CELERY_ROUTE


def route_requires_celery(route: str) -> bool:
    if route not in _ROUTES:
        raise ContractDecodeError("worker job route is unsupported")
    return route in {CELERY_ROUTE, SHADOW_ROUTE}
