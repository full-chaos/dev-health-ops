from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.llm_settings import (
    LLMSettingsAccessError,
    get_llm_settings_response,
    require_byo_llm_access,
)
from dev_health_ops.api.admin.llm_settings import (
    delete_llm_settings as delete_llm_settings_values,
)
from dev_health_ops.api.admin.llm_settings import (
    upsert_llm_settings as upsert_llm_settings_values,
)
from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    LLMSettingsResponse,
    LLMSettingsUpsert,
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
)
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.models.settings import SettingCategory

from .common import get_session

router = APIRouter()


def _setting_response(setting: object) -> SettingResponse:
    response = SettingResponse.model_validate(setting)
    if response.is_encrypted:
        return response.model_copy(update={"value": "[ENCRYPTED]"})
    return response


def _reject_llm_category(category: str) -> None:
    # LLM settings are tier-gated, force-encrypted, and masked; they must only
    # be managed via the dedicated /llm-settings endpoints. The generic settings
    # routes would otherwise let any org admin write/read category='llm' rows
    # (bypassing the BYO-LLM tier gate and exposing the raw api_key).
    if category == SettingCategory.LLM.value:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "use_llm_settings_endpoint",
                "message": (
                    "LLM settings must be managed via /admin/llm-settings "
                    "(tier-gated, encrypted, masked)."
                ),
            },
        )


async def _require_byo_llm_tier(session: AsyncSession, org_id: str) -> None:
    try:
        await require_byo_llm_access(session, org_id)
    except LLMSettingsAccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/settings/categories")
async def list_setting_categories() -> list[str]:
    return [c.value for c in SettingCategory]


@router.get("/settings/{category}", response_model=SettingsListResponse)
async def list_settings_by_category(
    category: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingsListResponse:
    _reject_llm_category(category)
    svc = SettingsService(session, org_id)
    settings = await svc.list_by_category(category)
    return SettingsListResponse(
        category=category,
        settings=[_setting_response(SettingResponse(**s)) for s in settings],
    )


@router.get("/llm-settings", response_model=LLMSettingsResponse)
async def get_llm_settings(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsResponse:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    return await get_llm_settings_response(svc)


@router.put("/llm-settings", response_model=LLMSettingsResponse)
async def upsert_llm_settings(
    payload: LLMSettingsUpsert,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsResponse:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    return await upsert_llm_settings_values(svc, payload)


@router.delete("/llm-settings")
async def delete_llm_settings(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, bool]:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    deleted = await delete_llm_settings_values(svc)
    if not deleted:
        raise HTTPException(status_code=404, detail="LLM settings not found")
    return {"deleted": True}


@router.get("/settings/{category}/{key}", response_model=SettingResponse)
async def get_setting(
    category: str,
    key: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    _reject_llm_category(category)
    svc = SettingsService(session, org_id)
    rows = await svc.list_by_category(category)
    row = next((s for s in rows if s.get("key") == key), None)
    if row is None:
        raise HTTPException(status_code=404, detail="Setting not found")
    # Never return decrypted values from the generic endpoint; _setting_response
    # masks encrypted settings as [ENCRYPTED].
    return _setting_response(
        SettingResponse(
            key=key,
            value=row.get("value"),
            category=category,
            is_encrypted=bool(row.get("is_encrypted", False)),
            description=row.get("description"),
        )
    )


@router.put("/settings/{category}/{key}", response_model=SettingResponse)
async def set_setting(
    category: str,
    key: str,
    payload: SettingUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    _reject_llm_category(category)
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
    _reject_llm_category(payload.category)
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
    _reject_llm_category(category)
    svc = SettingsService(session, org_id)
    deleted = await svc.delete(key, category)
    if not deleted:
        raise HTTPException(status_code=404, detail="Setting not found")
    return {"deleted": True}
