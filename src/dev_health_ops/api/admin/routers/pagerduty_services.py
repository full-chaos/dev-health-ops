from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.go_served import _raise_served_by_go_api

router = APIRouter()


class PagerDutyServiceResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    external_id: str
    display_name: str
    name_resolved: bool
    status: str | None


class PagerDutyServicesResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    credential_name: str
    services: list[PagerDutyServiceResponse]


@router.get(
    "/integrations/pagerduty/services",
    response_model=PagerDutyServicesResponse,
)
async def list_pagerduty_services(
    credential_name: str = Query(default="default", min_length=1),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyServicesResponse:
    _raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/services")
