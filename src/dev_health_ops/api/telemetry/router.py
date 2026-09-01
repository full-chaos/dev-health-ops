"""/api/v1/telemetry/* -- org opt-in state and instance usage reporting.

CHAOS-4722: prior to this fix, ``get_org_id`` returned the caller-supplied
``X-Org-Id`` header verbatim, with no authentication dependency of any kind
(guardrail G-4 violated literally: the header was authoritative merely
because the caller supplied it). ``OrgIdMiddleware`` does not save this --
it deliberately passes anonymous requests through, trusting that a route
requiring auth will 401 via its own dependency (see its docstring). This
router had no such dependency, so an unauthenticated caller could read and
mutate ANY org's telemetry state, then (by opting that org in itself)
retrieve instance-wide usage aggregates via ``/report``.

The fix mirrors ``OrgIdMiddleware``'s own IDOR check (same enforcement
shape, reusing ``user_is_member_of_org`` rather than a second membership
idiom): every route requires a real authenticated caller
(``get_current_user``, 401 if absent/invalid), and the acting org is
resolved from that authenticated state -- the caller's own org, an org they
have a Membership row for, or any org if they are a superuser -- never from
the raw header alone (G-25). ``/report``'s instance-wide aggregates are
additionally gated to a platform-role (superuser) principal (G-32): that is
a platform-operator capability, not an org-level one, so org membership is
the wrong axis to gate it on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops import __version__
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dependencies import get_postgres_session_dep as get_session
from dev_health_ops.api.middleware import user_is_member_of_org
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.telemetry import TelemetryService
from dev_health_ops.metrics.prometheus import record_telemetry_org_id_rejected

from .schemas import TelemetryReport, TelemetryStatus

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


async def get_org_id(
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    x_org_id: Annotated[str | None, Header(alias="X-Org-Id")] = None,
) -> str:
    """Resolve and verify the acting org from AUTHENTICATED state (CHAOS-4722).

    ``get_current_user`` already fails closed with 401 for any request that
    is not carrying a valid access token -- an anonymous caller never
    reaches the logic below. Once authenticated, the requested org (the
    header if present, else the caller's own JWT org) must be one the
    caller is actually scoped to: their own org, a Membership row, or
    superuser. Anything else is 403, and never mutates state (the route
    handlers only run after this dependency resolves).
    """
    requested_org_id = x_org_id or current_user.org_id
    if not requested_org_id:
        record_telemetry_org_id_rejected(reason="no_org_context")
        raise HTTPException(status_code=403, detail="Organization context required")

    if current_user.is_superuser:
        return requested_org_id
    if requested_org_id == current_user.org_id:
        return requested_org_id
    if await user_is_member_of_org(current_user.user_id, requested_org_id):
        return requested_org_id

    record_telemetry_org_id_rejected(reason="not_a_member")
    raise HTTPException(status_code=403, detail="X-Org-Id not permitted for this user")


async def require_platform_role(
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> AuthenticatedUser:
    """Gate instance-wide telemetry aggregates to a platform-role principal.

    CHAOS-4722: ``TelemetryService.collect_usage_stats()`` (called from
    ``/report``) takes no ``org_id`` -- it is instance-wide, not
    tenant-scoped. Org membership is therefore the wrong axis to gate it on;
    require superuser instead (G-32).
    """
    if not current_user.is_superuser:
        record_telemetry_org_id_rejected(reason="report_not_platform_role")
        raise HTTPException(
            status_code=403,
            detail="Instance-wide telemetry statistics require a platform role",
        )
    return current_user


@router.get("/status", response_model=TelemetryStatus)
async def telemetry_status(
    org_id: str = Depends(get_org_id),
    session: AsyncSession = Depends(get_session),
) -> TelemetryStatus:
    service = TelemetryService(session)
    return TelemetryStatus(
        opted_in=await service.get_opt_in_status(org_id),
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/opt-in", response_model=TelemetryStatus)
async def telemetry_opt_in(
    org_id: str = Depends(get_org_id),
    session: AsyncSession = Depends(get_session),
) -> TelemetryStatus:
    service = TelemetryService(session)
    await service.set_opt_in(org_id, True)
    return TelemetryStatus(
        opted_in=True,
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/opt-out", response_model=TelemetryStatus)
async def telemetry_opt_out(
    org_id: str = Depends(get_org_id),
    session: AsyncSession = Depends(get_session),
) -> TelemetryStatus:
    service = TelemetryService(session)
    await service.set_opt_in(org_id, False)
    return TelemetryStatus(
        opted_in=False,
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/report", response_model=TelemetryReport)
async def telemetry_report(
    _platform_user: AuthenticatedUser = Depends(require_platform_role),
    org_id: str = Depends(get_org_id),
    session: AsyncSession = Depends(get_session),
) -> TelemetryReport:
    service = TelemetryService(session)
    if not await service.get_opt_in_status(org_id):
        raise HTTPException(
            status_code=403, detail="Telemetry is not enabled for this org"
        )

    collected_at = datetime.now(timezone.utc)
    usage_stats = await service.collect_usage_stats()
    report = TelemetryReport(
        total_organizations=usage_stats["total_organizations"],
        active_organizations=usage_stats["active_organizations"],
        total_users=usage_stats["total_users"],
        active_users=usage_stats["active_users"],
        total_repos=usage_stats["total_repos"],
        total_sync_configs=usage_stats["total_sync_configs"],
        active_syncs_24h=usage_stats["active_syncs_24h"],
        tier_distribution=usage_stats["tier_distribution"],
        feature_usage=usage_stats["feature_usage"],
        version=__version__,
        collected_at=collected_at,
    )

    status_code = await service.send_report(report.model_dump(mode="json"))
    await service.set_last_report_at(org_id, collected_at)
    await service.record_heartbeat(
        {
            "event": "telemetry_report",
            "status_code": status_code,
            "collected_at": collected_at.isoformat(),
            "org_id": org_id,
        },
        org_id=org_id,
    )

    return report
