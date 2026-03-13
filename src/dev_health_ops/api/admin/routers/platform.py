from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_superuser
from dev_health_ops.api.admin.schemas import PlatformStatsResponse
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.users import Membership, Organization, User

from .common import get_session

router = APIRouter()


@router.get("/platform/stats", response_model=PlatformStatsResponse)
async def platform_stats(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> PlatformStatsResponse:
    total_organizations = (
        await session.execute(select(func.count()).select_from(Organization))
    ).scalar_one()
    active_organizations = (
        await session.execute(
            select(func.count())
            .select_from(Organization)
            .where(Organization.is_active.is_(True))
        )
    ).scalar_one()
    total_users = (
        await session.execute(select(func.count()).select_from(User))
    ).scalar_one()
    active_users = (
        await session.execute(
            select(func.count()).select_from(User).where(User.is_active.is_(True))
        )
    ).scalar_one()
    superuser_count = (
        await session.execute(
            select(func.count()).select_from(User).where(User.is_superuser.is_(True))
        )
    ).scalar_one()
    total_memberships = (
        await session.execute(select(func.count()).select_from(Membership))
    ).scalar_one()

    tier_rows = (
        await session.execute(
            select(Organization.tier, func.count()).group_by(Organization.tier)
        )
    ).all()
    tier_distribution = {str(tier): int(count) for tier, count in tier_rows}

    total_sync_configs = (
        await session.execute(select(func.count()).select_from(SyncConfiguration))
    ).scalar_one()
    active_sync_configs = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(SyncConfiguration.is_active.is_(True))
        )
    ).scalar_one()

    since = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_sync_filter = (
        SyncConfiguration.last_sync_at.is_not(None),
        SyncConfiguration.last_sync_at >= since,
    )

    recent_syncs_success = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(
                *recent_sync_filter,
                SyncConfiguration.last_sync_success.is_(True),
            )
        )
    ).scalar_one()
    recent_syncs_failed = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(
                *recent_sync_filter,
                SyncConfiguration.last_sync_success.is_(False),
            )
        )
    ).scalar_one()

    return PlatformStatsResponse(
        total_organizations=int(total_organizations),
        active_organizations=int(active_organizations),
        total_users=int(total_users),
        active_users=int(active_users),
        superuser_count=int(superuser_count),
        total_memberships=int(total_memberships),
        tier_distribution=tier_distribution,
        total_sync_configs=int(total_sync_configs),
        active_sync_configs=int(active_sync_configs),
        recent_syncs_success=int(recent_syncs_success),
        recent_syncs_failed=int(recent_syncs_failed),
    )
