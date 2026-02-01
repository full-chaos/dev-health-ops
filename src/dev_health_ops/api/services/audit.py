from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

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
    user_id: Optional[str]
    action: str
    resource_type: str
    resource_id: str
    description: Optional[str]
    changes: Optional[dict[str, Any]]
    request_metadata: Optional[dict[str, Any]]
    status: str
    error_message: Optional[str]
    created_at: datetime


@dataclass
class AuditLogFilter:
    user_id: Optional[str] = None
    action: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class AuditService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def log(
        self,
        org_id: uuid.UUID,
        action: AuditAction | str,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        changes: Optional[dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        request_id: Optional[str] = None,
        extra_metadata: Optional[dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> AuditLog:
        action_str = action.value if isinstance(action, AuditAction) else action
        resource_type_str = (
            resource_type.value
            if isinstance(resource_type, AuditResourceType)
            else resource_type
        )

        req_metadata: dict[str, Any] = {}
        if ip_address:
            req_metadata["ip_address"] = ip_address
        if user_agent:
            req_metadata["user_agent"] = user_agent
        if request_id:
            req_metadata["request_id"] = request_id
        if extra_metadata:
            req_metadata.update(extra_metadata)

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
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        created_values: Optional[dict[str, Any]] = None,
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
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        before: Optional[dict[str, Any]] = None,
        after: Optional[dict[str, Any]] = None,
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
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        deleted_values: Optional[dict[str, Any]] = None,
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
        error_message: Optional[str] = None,
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
        filters: Optional[AuditLogFilter] = None,
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
    ) -> Optional[AuditLog]:
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
