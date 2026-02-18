from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops import __version__
from dev_health_ops.api.services.telemetry import TelemetryService
from dev_health_ops.db import get_postgres_session

from .schemas import TelemetryReport, TelemetryStatus

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_postgres_session() as session:
        yield session


def get_org_id(x_org_id: Annotated[str, Header(alias="X-Org-Id")] = "default") -> str:
    return x_org_id


@router.get("/status", response_model=TelemetryStatus)
async def telemetry_status(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> TelemetryStatus:
    service = TelemetryService(session)
    return TelemetryStatus(
        opted_in=await service.get_opt_in_status(org_id),
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/opt-in", response_model=TelemetryStatus)
async def telemetry_opt_in(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> TelemetryStatus:
    service = TelemetryService(session)
    await service.set_opt_in(org_id, True)
    return TelemetryStatus(
        opted_in=True,
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/opt-out", response_model=TelemetryStatus)
async def telemetry_opt_out(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> TelemetryStatus:
    service = TelemetryService(session)
    await service.set_opt_in(org_id, False)
    return TelemetryStatus(
        opted_in=False,
        last_report_at=await service.get_last_report_at(org_id),
    )


@router.post("/report", response_model=TelemetryReport)
async def telemetry_report(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
