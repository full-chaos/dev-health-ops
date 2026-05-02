from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
)
from dev_health_ops.api.services.settings import SettingsService
from dev_health_ops.models.settings import SettingCategory

from .common import get_session

router = APIRouter()


def _setting_response(setting: object) -> SettingResponse:
    response = SettingResponse.model_validate(setting)
    if response.is_encrypted:
        return response.model_copy(update={"value": "[ENCRYPTED]"})
    return response


@router.get("/settings/categories")
async def list_setting_categories() -> list[str]:
    return [c.value for c in SettingCategory]


@router.get("/settings/{category}", response_model=SettingsListResponse)
async def list_settings_by_category(
    category: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingsListResponse:
    svc = SettingsService(session, org_id)
    settings = await svc.list_by_category(category)
    return SettingsListResponse(
        category=category,
        settings=[SettingResponse(**s) for s in settings],
    )


@router.get("/settings/{category}/{key}", response_model=SettingResponse)
async def get_setting(
    category: str,
    key: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    value = await svc.get(key, category)
    if value is None:
        raise HTTPException(status_code=404, detail="Setting not found")
    return SettingResponse(
        key=key, value=value, category=category, is_encrypted=False, description=None
    )


@router.put("/settings/{category}/{key}", response_model=SettingResponse)
async def set_setting(
    category: str,
    key: str,
    payload: SettingUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    setting = await svc.set(
        key=key,
        value=payload.value,
        category=category,
        encrypt=payload.encrypt or False,
        description=payload.description,
    )
    return _setting_response(setting)


@router.post("/settings", response_model=SettingResponse)
async def create_setting(
    payload: SettingCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    setting = await svc.set(
        key=payload.key,
        value=payload.value,
        category=payload.category,
        encrypt=payload.encrypt,
        description=payload.description,
    )
    return _setting_response(setting)


@router.delete("/settings/{category}/{key}")
async def delete_setting(
    category: str,
    key: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SettingsService(session, org_id)
    deleted = await svc.delete(key, category)
    if not deleted:
        raise HTTPException(status_code=404, detail="Setting not found")
    return {"deleted": True}
