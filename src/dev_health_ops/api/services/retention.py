from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from sqlalchemy import and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.retention import OrgRetentionPolicy, RetentionResourceType
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.api.utils.logging import sanitize_for_log

logger = logging.getLogger(__name__)

TABLE_MAP = {
    RetentionResourceType.AUDIT_LOGS.value: ("audit_log", "created_at"),
}


class RetentionService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_policy(
        self,
        org_id: uuid.UUID,
        resource_type: str,
        retention_days: int = 90,
        description: Optional[str] = None,
        created_by_id: Optional[uuid.UUID] = None,
    ) -> OrgRetentionPolicy:
        if resource_type not in [r.value for r in RetentionResourceType]:
            raise ValueError(f"Invalid resource type: {resource_type}")

        existing = await self.get_policy_by_resource_type(org_id, resource_type)
        if existing:
            raise ValueError(
                f"Policy for resource type '{resource_type}' already exists"
            )

        policy = OrgRetentionPolicy(
            org_id=org_id,
            resource_type=resource_type,
            retention_days=retention_days,
            description=description,
            created_by_id=created_by_id,
        )

        self.session.add(policy)
        await self.session.flush()

        logger.info(
            "Retention policy created: %s for org=%s, retention_days=%s",
            sanitize_for_log(resource_type),
            org_id,
            sanitize_for_log(retention_days),
        )
        return policy

    async def get_policy(
        self, org_id: uuid.UUID, policy_id: uuid.UUID
    ) -> Optional[OrgRetentionPolicy]:
        stmt = select(OrgRetentionPolicy).where(
            and_(
                OrgRetentionPolicy.id == policy_id, OrgRetentionPolicy.org_id == org_id
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_policy_by_resource_type(
        self, org_id: uuid.UUID, resource_type: str
    ) -> Optional[OrgRetentionPolicy]:
        stmt = select(OrgRetentionPolicy).where(
            and_(
                OrgRetentionPolicy.org_id == org_id,
                OrgRetentionPolicy.resource_type == resource_type,
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_policies(
        self,
        org_id: uuid.UUID,
        active_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[Sequence[OrgRetentionPolicy], int]:
        conditions = [OrgRetentionPolicy.org_id == org_id]

        if active_only:
            conditions.append(OrgRetentionPolicy.is_active == True)  # noqa: E712

        count_stmt = select(OrgRetentionPolicy).where(and_(*conditions))
        count_result = await self.session.execute(count_stmt)
        total = len(count_result.scalars().all())

        stmt = (
            select(OrgRetentionPolicy)
            .where(and_(*conditions))
            .order_by(OrgRetentionPolicy.created_at)
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        policies = result.scalars().all()

        return policies, total

    async def update_policy(
        self,
        org_id: uuid.UUID,
        policy_id: uuid.UUID,
        retention_days: Optional[int] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[OrgRetentionPolicy]:
        policy = await self.get_policy(org_id, policy_id)
        if not policy:
            return None

        if retention_days is not None:
            if retention_days < 1:
                raise ValueError("Retention days must be at least 1")
            policy.retention_days = retention_days
        if description is not None:
            policy.description = description
        if is_active is not None:
            policy.is_active = is_active

        policy.updated_at = datetime.now(timezone.utc)
        await self.session.flush()

        logger.info("Retention policy updated: %s for org=%s", policy_id, org_id)
        return policy

    async def delete_policy(self, org_id: uuid.UUID, policy_id: uuid.UUID) -> bool:
        policy = await self.get_policy(org_id, policy_id)
        if not policy:
            return False

        await self.session.delete(policy)
        await self.session.flush()

        logger.info("Retention policy deleted: %s for org=%s", policy_id, org_id)
        return True

    async def execute_policy(
        self, org_id: uuid.UUID, policy_id: uuid.UUID
    ) -> tuple[int, Optional[str]]:
        policy = await self.get_policy(org_id, policy_id)
        if not policy:
            return 0, "Policy not found"

        if not policy.is_active:
            return 0, "Policy is not active"

        resource_type = str(policy.resource_type)
        retention_days = int(policy.retention_days)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)

        deleted_count = 0
        error_message = None

        try:
            if resource_type == RetentionResourceType.AUDIT_LOGS.value:
                deleted_count = await self._delete_audit_logs(org_id, cutoff_date)
            else:
                error_message = (
                    f"Cleanup not implemented for resource type: {resource_type}"
                )
                logger.warning(error_message)
                return 0, error_message

            policy.last_run_at = datetime.now(timezone.utc)
            policy.last_run_deleted_count = deleted_count
            policy.next_run_at = datetime.now(timezone.utc) + timedelta(days=1)
            await self.session.flush()

            logger.info(
                "Retention policy executed: %s, deleted %d records older than %s",
                policy_id,
                deleted_count,
                cutoff_date.isoformat(),
            )

        except Exception as e:
            error_message = str(e)
            logger.exception("Error executing retention policy %s: %s", policy_id, e)

        return deleted_count, error_message

    async def _delete_audit_logs(self, org_id: uuid.UUID, cutoff_date: datetime) -> int:
        stmt = delete(AuditLog).where(
            and_(
                AuditLog.org_id == org_id,
                AuditLog.created_at < cutoff_date,
            )
        )
        result = await self.session.execute(stmt)
        return result.rowcount

    async def get_policies_due_for_execution(
        self, limit: int = 100
    ) -> Sequence[OrgRetentionPolicy]:
        now = datetime.now(timezone.utc)
        stmt = (
            select(OrgRetentionPolicy)
            .where(
                and_(
                    OrgRetentionPolicy.is_active == True,  # noqa: E712
                    OrgRetentionPolicy.next_run_at <= now,
                )
            )
            .order_by(OrgRetentionPolicy.next_run_at)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    def get_available_resource_types() -> list[str]:
        return [r.value for r in RetentionResourceType]
