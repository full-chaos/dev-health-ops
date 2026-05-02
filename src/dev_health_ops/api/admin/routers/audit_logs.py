from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id, require_superuser
from dev_health_ops.api.admin.schemas import AuditLogListResponse, AuditLogResponse
from dev_health_ops.api.services.audit import AuditLogFilter as ServiceAuditLogFilter
from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing import require_feature
from dev_health_ops.models.audit import AuditLog

from .common import get_session

router = APIRouter()


def _audit_log_response(log: object) -> AuditLogResponse:
    return AuditLogResponse.model_validate(
        {
            "id": str(getattr(log, "id")),
            "org_id": str(getattr(log, "org_id")),
            "user_id": (
                str(getattr(log, "user_id"))
                if getattr(log, "user_id") is not None
                else None
            ),
            "action": str(getattr(log, "action")),
            "resource_type": str(getattr(log, "resource_type")),
            "resource_id": str(getattr(log, "resource_id")),
            "description": getattr(log, "description"),
            "changes": getattr(log, "changes"),
            "request_metadata": getattr(log, "request_metadata"),
            "status": str(getattr(log, "status")),
            "error_message": getattr(log, "error_message"),
            "created_at": getattr(log, "created_at"),
        }
    )


@router.get("/audit-logs", response_model=AuditLogListResponse)
@require_feature("audit_log", required_tier="enterprise")
async def list_audit_logs(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user_id: str | None = Query(None, description="Filter by user ID"),
    action: str | None = Query(None, description="Filter by action type"),
    resource_type: str | None = Query(None, description="Filter by resource type"),
    resource_id: str | None = Query(None, description="Filter by resource ID"),
    status: str | None = Query(None, description="Filter by status (success/failure)"),
    start_date: datetime | None = Query(
        None, description="Filter logs after this date"
    ),
    end_date: datetime | None = Query(None, description="Filter logs before this date"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> AuditLogListResponse:
    """List audit logs for the organization with optional filters.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    filters = ServiceAuditLogFilter(
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    logs, total = await svc.get_logs(
        org_id=uuid.UUID(org_id),
        filters=filters,
        limit=limit,
        offset=offset,
    )

    return AuditLogListResponse(
        items=[_audit_log_response(log) for log in logs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/platform/audit-logs", response_model=AuditLogListResponse)
async def list_platform_audit_logs(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
    user_id: str | None = Query(None, description="Filter by user ID"),
    action: str | None = Query(None, description="Filter by action type"),
    resource_type: str | None = Query(None, description="Filter by resource type"),
    resource_id: str | None = Query(None, description="Filter by resource ID"),
    status: str | None = Query(None, description="Filter by status (success/failure)"),
    start_date: datetime | None = Query(
        None, description="Filter logs after this date"
    ),
    end_date: datetime | None = Query(None, description="Filter logs before this date"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> AuditLogListResponse:
    conditions = []
    if user_id:
        conditions.append(AuditLog.user_id == uuid.UUID(user_id))
    if action:
        conditions.append(AuditLog.action == action)
    if resource_type:
        conditions.append(AuditLog.resource_type == resource_type)
    if resource_id:
        conditions.append(AuditLog.resource_id == resource_id)
    if status:
        conditions.append(AuditLog.status == status)
    if start_date:
        conditions.append(AuditLog.created_at >= start_date)
    if end_date:
        conditions.append(AuditLog.created_at <= end_date)

    count_stmt = select(func.count()).select_from(AuditLog)
    if conditions:
        count_stmt = count_stmt.where(*conditions)
    total = int((await session.execute(count_stmt)).scalar_one())

    created_at_col = getattr(AuditLog, "created_at")
    logs_stmt = (
        select(AuditLog).order_by(created_at_col.desc()).limit(limit).offset(offset)
    )
    if conditions:
        logs_stmt = logs_stmt.where(*conditions)
    logs_result = await session.execute(logs_stmt)
    logs = logs_result.scalars().all()

    return AuditLogListResponse(
        items=[_audit_log_response(log) for log in logs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/audit-logs/{log_id}", response_model=AuditLogResponse)
@require_feature("audit_log", required_tier="enterprise")
async def get_audit_log(
    log_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> AuditLogResponse:
    """Get a specific audit log entry by ID.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    log = await svc.get_log_by_id(
        org_id=uuid.UUID(org_id),
        log_id=uuid.UUID(log_id),
    )

    if not log:
        raise HTTPException(status_code=404, detail="Audit log not found")

    return _audit_log_response(log)


@router.get(
    "/audit-logs/resource/{resource_type}/{resource_id}",
    response_model=list[AuditLogResponse],
)
@require_feature("audit_log", required_tier="enterprise")
async def get_resource_audit_history(
    resource_type: str,
    resource_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
) -> list[AuditLogResponse]:
    """Get audit history for a specific resource.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    logs = await svc.get_resource_history(
        org_id=uuid.UUID(org_id),
        resource_type=resource_type,
        resource_id=resource_id,
        limit=limit,
    )

    return [_audit_log_response(log) for log in logs]


@router.get("/audit-logs/user/{user_id}", response_model=list[AuditLogResponse])
@require_feature("audit_log", required_tier="enterprise")
async def get_user_audit_activity(
    user_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
) -> list[AuditLogResponse]:
    """Get audit log activity for a specific user.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    logs = await svc.get_user_activity(
        org_id=uuid.UUID(org_id),
        user_id=uuid.UUID(user_id),
        limit=limit,
    )

    return [_audit_log_response(log) for log in logs]
