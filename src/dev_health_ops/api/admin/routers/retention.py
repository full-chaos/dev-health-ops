from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    RetentionExecuteRequest,
    RetentionExecuteResponse,
    RetentionPolicyCreate,
    RetentionPolicyListResponse,
    RetentionPolicyResponse,
    RetentionPolicyUpdate,
)
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.licensing import require_feature

from .common import get_user_id

router = APIRouter()


@router.get("/retention-policies", response_model=RetentionPolicyListResponse)
@require_feature("custom_retention", required_tier="enterprise")
async def list_retention_policies(
    org_id: str = Depends(get_admin_org_id),
    active_only: bool = Query(False, description="Filter to active policies only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> RetentionPolicyListResponse:
    raise_served_by_go_api("/api/v1/admin/retention-policies", GO_API)


@router.get("/retention-policies/resource-types")
@require_feature("custom_retention", required_tier="enterprise")
async def list_retention_resource_types() -> list[str]:
    raise_served_by_go_api("/api/v1/admin/retention-policies/resource-types", GO_API)


@router.post(
    "/retention-policies", response_model=RetentionPolicyResponse, status_code=201
)
@require_feature("custom_retention", required_tier="enterprise")
async def create_retention_policy(
    payload: RetentionPolicyCreate,
    org_id: str = Depends(get_admin_org_id),
    user_id: str | None = Depends(get_user_id),
) -> RetentionPolicyResponse:
    raise_served_by_go_api("/api/v1/admin/retention-policies", GO_API)


@router.get("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("custom_retention", required_tier="enterprise")
async def get_retention_policy(
    policy_id: str,
    org_id: str = Depends(get_admin_org_id),
) -> RetentionPolicyResponse:
    raise_served_by_go_api("/api/v1/admin/retention-policies/{policy_id}", GO_API)


@router.patch("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("custom_retention", required_tier="enterprise")
async def update_retention_policy(
    policy_id: str,
    payload: RetentionPolicyUpdate,
    org_id: str = Depends(get_admin_org_id),
) -> RetentionPolicyResponse:
    raise_served_by_go_api("/api/v1/admin/retention-policies/{policy_id}", GO_API)


@router.delete("/retention-policies/{policy_id}")
@require_feature("custom_retention", required_tier="enterprise")
async def delete_retention_policy(
    policy_id: str,
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    raise_served_by_go_api("/api/v1/admin/retention-policies/{policy_id}", GO_API)


@router.post(
    "/retention-policies/{policy_id}/execute", response_model=RetentionExecuteResponse
)
@require_feature("custom_retention", required_tier="enterprise")
async def execute_retention_policy(
    policy_id: str,
    payload: RetentionExecuteRequest | None = None,
    org_id: str = Depends(get_admin_org_id),
) -> RetentionExecuteResponse:
    raise_served_by_go_api(
        "/api/v1/admin/retention-policies/{policy_id}/execute", GO_API
    )
