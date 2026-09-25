from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    IPAllowlistCreate,
    IPAllowlistListResponse,
    IPAllowlistResponse,
    IPAllowlistUpdate,
    IPCheckRequest,
    IPCheckResponse,
)
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.licensing import require_feature

from .common import get_user_id

router = APIRouter()


@router.get("/ip-allowlist", response_model=IPAllowlistListResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def list_ip_allowlist_entries(
    org_id: str = Depends(get_admin_org_id),
    active_only: bool = Query(False, description="Filter to active entries only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> IPAllowlistListResponse:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist", GO_API)


@router.post("/ip-allowlist", response_model=IPAllowlistResponse, status_code=201)
@require_feature("ip_allowlist", required_tier="enterprise")
async def create_ip_allowlist_entry(
    payload: IPAllowlistCreate,
    org_id: str = Depends(get_admin_org_id),
    user_id: str | None = Depends(get_user_id),
) -> IPAllowlistResponse:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist", GO_API)


@router.get("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def get_ip_allowlist_entry(
    entry_id: str,
    org_id: str = Depends(get_admin_org_id),
) -> IPAllowlistResponse:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist/{entry_id}", GO_API)


@router.patch("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def update_ip_allowlist_entry(
    entry_id: str,
    payload: IPAllowlistUpdate,
    org_id: str = Depends(get_admin_org_id),
) -> IPAllowlistResponse:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist/{entry_id}", GO_API)


@router.delete("/ip-allowlist/{entry_id}")
@require_feature("ip_allowlist", required_tier="enterprise")
async def delete_ip_allowlist_entry(
    entry_id: str,
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist/{entry_id}", GO_API)


@router.post("/ip-allowlist/check", response_model=IPCheckResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def check_ip_allowed(
    payload: IPCheckRequest,
    org_id: str = Depends(get_admin_org_id),
) -> IPCheckResponse:
    raise_served_by_go_api("/api/v1/admin/ip-allowlist/check", GO_API)
