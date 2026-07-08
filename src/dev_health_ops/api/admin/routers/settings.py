from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
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
    LLMSettingsStatusResponse,
    LLMSettingsUpsert,
    LLMSpendResponse,
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
)
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.db import require_clickhouse_uri
from dev_health_ops.llm.credentials import (
    evaluate_org_llm_status,
    latest_recent_org_byo_base_url_fallback_at,
)
from dev_health_ops.metrics.schemas import LLMTokenSpendSummaryRecord
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.models.settings import SettingCategory

from .common import get_session

router = APIRouter()

LLMSpendReader = Callable[..., LLMTokenSpendSummaryRecord | None]


def read_llm_token_spend_summary(
    *, org_id: str, limit: int, since: datetime | None
) -> LLMTokenSpendSummaryRecord | None:
    sink = create_sink(require_clickhouse_uri())
    try:
        return sink.read_llm_token_spend(org_id=org_id, limit=limit, since=since)
    finally:
        sink.close()


def get_llm_spend_reader() -> Iterator[LLMSpendReader]:
    yield read_llm_token_spend_summary


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


async def _require_byo_llm_tier(
    session: AsyncSession, org_id: str, *, for_cleanup: bool = False
) -> None:
    try:
        await require_byo_llm_access(session, org_id, for_cleanup=for_cleanup)
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


@router.get(
    "/llm-settings",
    response_model=LLMSettingsResponse,
    response_model_exclude_none=True,
)
async def get_llm_settings(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsResponse:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    return await get_llm_settings_response(svc)


@router.get(
    "/llm-settings/status",
    response_model=LLMSettingsStatusResponse,
)
async def get_llm_settings_status(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsStatusResponse:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    evaluation = await evaluate_org_llm_status(org_id, svc)
    last_fallback_at = await latest_recent_org_byo_base_url_fallback_at(
        session, org_id, evaluation
    )
    return LLMSettingsStatusResponse(
        configured=evaluation.configured,
        active=evaluation.active,
        degraded=evaluation.reason_code == "invalid_base_url",
        reason_code=evaluation.reason_code,
        last_fallback_at=last_fallback_at,
    )


@router.get(
    "/llm-settings/spend",
    response_model=LLMSpendResponse,
)
async def get_llm_settings_spend(
    limit: int = Query(20, ge=1),
    since: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    spend_reader: LLMSpendReader = Depends(get_llm_spend_reader),
) -> LLMSpendResponse:
    await _require_byo_llm_tier(session, org_id)
    response_since = since or datetime.now(timezone.utc) - timedelta(days=30)
    response_limit = min(max(1, limit), 100)
    svc = SettingsService(session, org_id)
    evaluation = await evaluate_org_llm_status(org_id, svc)
    if not evaluation.active:
        return LLMSpendResponse(since=response_since, limit=response_limit)
    summary = spend_reader(org_id=org_id, limit=limit, since=since)
    if summary is None:
        return LLMSpendResponse(since=response_since, limit=response_limit)
    return LLMSpendResponse.model_validate(asdict(summary))


@router.put(
    "/llm-settings",
    response_model=LLMSettingsResponse,
    response_model_exclude_none=True,
)
async def upsert_llm_settings(
    payload: LLMSettingsUpsert,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsResponse:
    await _require_byo_llm_tier(session, org_id)
    svc = SettingsService(session, org_id)
    try:
        return await upsert_llm_settings_values(svc, payload)
    except LLMSettingsAccessError as exc:
        # Persist-time base_url allowlist rejection (CHAOS-2552) -> 400.
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.delete("/llm-settings")
async def delete_llm_settings(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, bool]:
    # DELETE must remain available so an admin can clean up stored BYO secrets
    # even when the byo_llm flag is disabled or the org has been downgraded
    # below the BYO tier (CHAOS-2551 review).
    await _require_byo_llm_tier(session, org_id, for_cleanup=True)
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
