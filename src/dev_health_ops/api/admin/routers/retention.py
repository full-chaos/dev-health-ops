from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    RetentionExecuteResponse,
    RetentionPolicyCreate,
    RetentionPolicyListResponse,
    RetentionPolicyResponse,
    RetentionPolicyUpdate,
)
from dev_health_ops.api.services.retention import RetentionService
from dev_health_ops.licensing import require_feature

from .common import get_session, get_user_id

router = APIRouter()


def _retention_policy_response(policy: object) -> RetentionPolicyResponse:
    return RetentionPolicyResponse.model_validate(
        {
            "id": str(getattr(policy, "id")),
            "org_id": str(getattr(policy, "org_id")),
            "resource_type": str(getattr(policy, "resource_type")),
            "retention_days": int(getattr(policy, "retention_days")),
            "description": getattr(policy, "description"),
            "is_active": getattr(policy, "is_active"),
            "last_run_at": getattr(policy, "last_run_at"),
            "last_run_deleted_count": getattr(policy, "last_run_deleted_count"),
            "next_run_at": getattr(policy, "next_run_at"),
            "created_by_id": (
                str(getattr(policy, "created_by_id"))
                if getattr(policy, "created_by_id") is not None
                else None
            ),
            "created_at": getattr(policy, "created_at"),
            "updated_at": getattr(policy, "updated_at"),
        }
    )


@router.get("/retention-policies", response_model=RetentionPolicyListResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def list_retention_policies(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    active_only: bool = Query(False, description="Filter to active policies only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> RetentionPolicyListResponse:
    svc = RetentionService(session)
    policies, total = await svc.list_policies(
        org_id=uuid.UUID(org_id),
        active_only=active_only,
        limit=limit,
        offset=offset,
    )
    return RetentionPolicyListResponse(
        items=[_retention_policy_response(policy) for policy in policies],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/retention-policies/resource-types")
@require_feature("retention_policies", required_tier="enterprise")
async def list_retention_resource_types() -> list[str]:
    return RetentionService.get_available_resource_types()


@router.post(
    "/retention-policies", response_model=RetentionPolicyResponse, status_code=201
)
@require_feature("retention_policies", required_tier="enterprise")
async def create_retention_policy(
    payload: RetentionPolicyCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user_id: str | None = Depends(get_user_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    try:
        policy = await svc.create_policy(
            org_id=uuid.UUID(org_id),
            resource_type=payload.resource_type,
            retention_days=payload.retention_days,
            description=payload.description,
            created_by_id=uuid.UUID(user_id) if user_id else None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _retention_policy_response(policy)


@router.get("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def get_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    policy = await svc.get_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return _retention_policy_response(policy)


@router.patch("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def update_retention_policy(
    policy_id: str,
    payload: RetentionPolicyUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    try:
        policy = await svc.update_policy(
            org_id=uuid.UUID(org_id),
            policy_id=uuid.UUID(policy_id),
            retention_days=payload.retention_days,
            description=payload.description,
            is_active=payload.is_active,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return _retention_policy_response(policy)


@router.delete("/retention-policies/{policy_id}")
@require_feature("retention_policies", required_tier="enterprise")
async def delete_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = RetentionService(session)
    deleted = await svc.delete_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return {"deleted": True}


@router.post(
    "/retention-policies/{policy_id}/execute", response_model=RetentionExecuteResponse
)
@require_feature("retention_policies", required_tier="enterprise")
async def execute_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> RetentionExecuteResponse:
    svc = RetentionService(session)
    deleted_count, error = await svc.execute_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    return RetentionExecuteResponse(
        deleted_count=deleted_count,
        error=error,
    )
