from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.audit import (
    AuditAction,
    AuditLog,
    AuditResourceType,
)

logger = logging.getLogger(__name__)


@dataclass
class AuditLogEntry:
    id: str
    org_id: str
    user_id: str | None
    action: str
    resource_type: str
    resource_id: str
    description: str | None
    changes: dict[str, Any] | None
    request_metadata: dict[str, Any] | None
    status: str
    error_message: str | None
    created_at: datetime


@dataclass
class AuditLogFilter:
    user_id: str | None = None
    action: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    status: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class AuditService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def log(
        self,
        org_id: uuid.UUID,
        action: AuditAction | str,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: uuid.UUID | None = None,
        user: AuthenticatedUser | None = None,
        description: str | None = None,
        changes: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        request_id: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
        status: str = "success",
        error_message: str | None = None,
    ) -> AuditLog:
        action_str = action.value if isinstance(action, AuditAction) else action
        resource_type_str = (
            resource_type.value
            if isinstance(resource_type, AuditResourceType)
            else resource_type
        )

        req_metadata: dict[str, Any] = {}
        # Import impersonation context lazily to avoid circular imports at module level
        from dev_health_ops.api.services.auth import get_impersonation_context

        if ip_address:
            req_metadata["ip_address"] = ip_address
        if user_agent:
            req_metadata["user_agent"] = user_agent
        if request_id:
            req_metadata["request_id"] = request_id
        if extra_metadata:
            req_metadata.update(extra_metadata)
        if user and user.impersonated_by:
            req_metadata["impersonated_by"] = user.impersonated_by
        # Attach impersonation context if an impersonation session is active
        imp_ctx = get_impersonation_context()
        if imp_ctx is not None and imp_ctx.is_active:
            if "impersonated_by" not in req_metadata:
                req_metadata["impersonated_by"] = imp_ctx.real_user_id
            if "impersonation_target" not in req_metadata:
                req_metadata["impersonation_target"] = imp_ctx.target_user_id
            if "impersonation_org" not in req_metadata:
                req_metadata["impersonation_org"] = imp_ctx.target_org_id

        audit_log = AuditLog(
            org_id=org_id,
            user_id=user_id,
            action=action_str,
            resource_type=resource_type_str,
            resource_id=resource_id,
            description=description,
            changes=changes,
            request_metadata=req_metadata if req_metadata else None,
            status=status,
            error_message=error_message,
        )

        self.session.add(audit_log)
        await self.session.flush()

        logger.debug(
            "Audit log created: %s %s:%s by user=%s",
            action_str,
            resource_type_str,
            resource_id,
            user_id,
        )

        return audit_log

    async def log_create(
        self,
        org_id: uuid.UUID,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: uuid.UUID | None = None,
        description: str | None = None,
        created_values: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AuditLog:
        return await self.log(
            org_id=org_id,
            action=AuditAction.CREATE,
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            description=description,
            changes={"created": created_values} if created_values else None,
            **kwargs,
        )

    async def log_update(
        self,
        org_id: uuid.UUID,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: uuid.UUID | None = None,
        description: str | None = None,
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AuditLog:
        changes = None
        if before is not None or after is not None:
            changes = {}
            if before is not None:
                changes["before"] = before
            if after is not None:
                changes["after"] = after

        return await self.log(
            org_id=org_id,
            action=AuditAction.UPDATE,
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            description=description,
            changes=changes,
            **kwargs,
        )

    async def log_delete(
        self,
        org_id: uuid.UUID,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: uuid.UUID | None = None,
        description: str | None = None,
        deleted_values: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AuditLog:
        return await self.log(
            org_id=org_id,
            action=AuditAction.DELETE,
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            description=description,
            changes={"deleted": deleted_values} if deleted_values else None,
            **kwargs,
        )

    async def log_login(
        self,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        success: bool = True,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> AuditLog:
        return await self.log(
            org_id=org_id,
            action=AuditAction.LOGIN if success else AuditAction.LOGIN_FAILED,
            resource_type=AuditResourceType.SESSION,
            resource_id=str(user_id),
            user_id=user_id,
            description="User logged in" if success else "Login attempt failed",
            status="success" if success else "failure",
            error_message=error_message,
            **kwargs,
        )

    async def get_logs(
        self,
        org_id: uuid.UUID,
        filters: AuditLogFilter | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[Sequence[AuditLog], int]:
        conditions = [AuditLog.org_id == org_id]

        if filters:
            if filters.user_id:
                conditions.append(AuditLog.user_id == uuid.UUID(filters.user_id))
            if filters.action:
                conditions.append(AuditLog.action == filters.action)
            if filters.resource_type:
                conditions.append(AuditLog.resource_type == filters.resource_type)
            if filters.resource_id:
                conditions.append(AuditLog.resource_id == filters.resource_id)
            if filters.status:
                conditions.append(AuditLog.status == filters.status)
            if filters.start_date:
                conditions.append(AuditLog.created_at >= filters.start_date)
            if filters.end_date:
                conditions.append(AuditLog.created_at <= filters.end_date)

        count_stmt = select(AuditLog).where(and_(*conditions))
        count_result = await self.session.execute(count_stmt)
        total = len(count_result.scalars().all())

        stmt = (
            select(AuditLog)
            .where(and_(*conditions))
            .order_by(desc(AuditLog.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        logs = result.scalars().all()

        return logs, total

    async def get_log_by_id(
        self, org_id: uuid.UUID, log_id: uuid.UUID
    ) -> AuditLog | None:
        stmt = select(AuditLog).where(
            and_(AuditLog.id == log_id, AuditLog.org_id == org_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_resource_history(
        self,
        org_id: uuid.UUID,
        resource_type: str,
        resource_id: str,
        limit: int = 50,
    ) -> Sequence[AuditLog]:
        stmt = (
            select(AuditLog)
            .where(
                and_(
                    AuditLog.org_id == org_id,
                    AuditLog.resource_type == resource_type,
                    AuditLog.resource_id == resource_id,
                )
            )
            .order_by(desc(AuditLog.created_at))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_user_activity(
        self,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limit: int = 50,
    ) -> Sequence[AuditLog]:
        stmt = (
            select(AuditLog)
            .where(and_(AuditLog.org_id == org_id, AuditLog.user_id == user_id))
            .order_by(desc(AuditLog.created_at))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    def to_entry(log: AuditLog) -> AuditLogEntry:
        return AuditLogEntry(
            id=str(log.id),
            org_id=str(log.org_id),
            user_id=str(log.user_id) if log.user_id else None,
            action=str(log.action),
            resource_type=str(log.resource_type),
            resource_id=str(log.resource_id),
            description=log.description,
            changes=log.changes,
            request_metadata=log.request_metadata,
            status=str(log.status),
            error_message=log.error_message,
            created_at=log.created_at,
        )
