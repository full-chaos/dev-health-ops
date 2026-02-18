from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.settings import SettingsService
from dev_health_ops.models.audit import AuditAction, AuditLog, AuditResourceType
from dev_health_ops.models.git import Repo
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.users import Membership, Organization, User

logger = logging.getLogger(__name__)

TELEMETRY_CATEGORY = "telemetry"
TELEMETRY_OPT_IN_KEY = "telemetry_opt_in"
TELEMETRY_LAST_REPORT_AT_KEY = "telemetry_last_report_at"


class TelemetryService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def collect_usage_stats(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        last_24h = now - timedelta(hours=24)

        totals_row = await self.session.execute(
            select(
                func.count(Organization.id),
                func.count(Organization.id).filter(Organization.is_active.is_(True)),
                func.count(User.id),
                func.count(User.id).filter(User.is_active.is_(True)),
                func.count(Repo.id),
                func.count(Membership.id),
                func.count(SyncConfiguration.id),
                func.count(SyncConfiguration.id).filter(
                    and_(
                        SyncConfiguration.is_active.is_(True),
                        SyncConfiguration.last_sync_at >= last_24h,
                    )
                ),
            )
        )
        totals = totals_row.one()

        tier_rows = await self.session.execute(
            select(Organization.tier, func.count(Organization.id)).group_by(
                Organization.tier
            )
        )
        tier_distribution = {
            str(tier or "unknown"): int(count or 0) for tier, count in tier_rows.all()
        }

        feature_rows = await self.session.execute(
            select(FeatureFlag.key, func.count(OrgFeatureOverride.id))
            .select_from(FeatureFlag)
            .join(OrgFeatureOverride, OrgFeatureOverride.feature_id == FeatureFlag.id)
            .where(
                OrgFeatureOverride.is_enabled.is_(True),
                or_(
                    OrgFeatureOverride.expires_at.is_(None),
                    OrgFeatureOverride.expires_at >= now,
                ),
            )
            .group_by(FeatureFlag.key)
        )
        feature_usage = {
            str(feature_key): int(count or 0)
            for feature_key, count in feature_rows.all()
            if feature_key
        }

        return {
            "total_organizations": int(totals[0] or 0),
            "active_organizations": int(totals[1] or 0),
            "total_users": int(totals[2] or 0),
            "active_users": int(totals[3] or 0),
            "total_repos": int(totals[4] or 0),
            "total_memberships": int(totals[5] or 0),
            "total_sync_configs": int(totals[6] or 0),
            "active_syncs_24h": int(totals[7] or 0),
            "tier_distribution": tier_distribution,
            "feature_usage": feature_usage,
        }

    async def get_opt_in_status(self, org_id: str) -> bool:
        svc = SettingsService(self.session, org_id)
        value = await svc.get(TELEMETRY_OPT_IN_KEY, TELEMETRY_CATEGORY, default="false")
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    async def get_last_report_at(self, org_id: str) -> datetime | None:
        svc = SettingsService(self.session, org_id)
        value = await svc.get(TELEMETRY_LAST_REPORT_AT_KEY, TELEMETRY_CATEGORY)
        if not value:
            return None
        normalized = str(value).replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            logger.warning("Invalid telemetry_last_report_at value for org %s", org_id)
            return None

    async def set_opt_in(self, org_id: str, enabled: bool) -> None:
        svc = SettingsService(self.session, org_id)
        await svc.set(
            key=TELEMETRY_OPT_IN_KEY,
            value="true" if enabled else "false",
            category=TELEMETRY_CATEGORY,
            encrypt=False,
            description="Controls voluntary telemetry reporting.",
        )

    async def set_last_report_at(self, org_id: str, at: datetime) -> None:
        svc = SettingsService(self.session, org_id)
        await svc.set(
            key=TELEMETRY_LAST_REPORT_AT_KEY,
            value=at.isoformat(),
            category=TELEMETRY_CATEGORY,
            encrypt=False,
            description="Timestamp of the last voluntary telemetry report.",
        )

    async def send_report(
        self, data: dict[str, Any], endpoint: str | None = None
    ) -> int | None:
        url = endpoint or os.getenv("TELEMETRY_ENDPOINT")
        if not url:
            return None

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=data)
            return int(response.status_code)
        except Exception as exc:
            logger.warning("Telemetry report send failed: %s", exc)
            return None

    async def record_heartbeat(
        self, data: dict[str, Any], org_id: str = "default"
    ) -> None:
        resolved_org_id = await self._resolve_org_uuid(org_id)
        if resolved_org_id is None:
            logger.debug("Skipping telemetry audit record; org %s not found", org_id)
            return

        entry = AuditLog(
            org_id=resolved_org_id,
            action=AuditAction.OTHER.value,
            resource_type=AuditResourceType.OTHER.value,
            resource_id="telemetry",
            description="Telemetry heartbeat/report recorded",
            changes=data,
            request_metadata={"source": "telemetry"},
        )
        self.session.add(entry)
        await self.session.flush()

    async def _resolve_org_uuid(self, org_id: str) -> uuid.UUID | None:
        try:
            return uuid.UUID(org_id)
        except (TypeError, ValueError):
            pass

        org_row = await self.session.execute(
            select(Organization.id).where(Organization.slug == org_id)
        )
        slug_match = org_row.scalar_one_or_none()
        if slug_match is not None:
            return slug_match

        if org_id == "default":
            first_org = await self.session.execute(select(Organization.id).limit(1))
            return first_org.scalar_one_or_none()

        return None
