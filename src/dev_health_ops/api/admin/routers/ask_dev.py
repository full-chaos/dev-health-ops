"""Organization-administrator controls for the shared Ask Dev experience."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id, get_admin_user
from dev_health_ops.api.dev.org_policy import (
    ASK_DEV_EMERGENCY_DISABLED_KEY,
    ASK_DEV_FALLBACK_KEY,
    ASK_DEV_RETENTION_KEY,
    load_ask_dev_org_policy,
)
from dev_health_ops.api.dev.persistence.service import DevAdmissionLimits
from dev_health_ops.api.dev.production_runtime import resolve_certification_provider
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.licensing import FeatureDecisionReason, evaluate_org_feature_async
from dev_health_ops.licensing.registry import ASK_DEV_FEATURE
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessService,
    SettingsAgentReadinessStore,
)
from dev_health_ops.models.dev_persistence import DevRun
from dev_health_ops.models.settings import SettingCategory

from .common import get_session

router = APIRouter()

ReadinessState = Literal[
    "ready",
    "unsupported_model",
    "missing_credentials",
    "disabled",
    "degraded",
    "stale_readiness",
]
EntitlementState = Literal[
    "enabled", "not_entitled", "globally_disabled", "org_disabled", "unavailable"
]


class StrictAdminModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AskDevAdminSettings(StrictAdminModel):
    retention_days: Literal[0, 30]
    fallback_policy: Literal["fail_closed", "platform"]
    emergency_disabled: bool


class AskDevAdminSettingsUpdate(StrictAdminModel):
    retention_days: Literal[0, 30] | None = None
    fallback_policy: Literal["fail_closed", "platform"] | None = None
    emergency_disabled: bool | None = None

    @model_validator(mode="after")
    def require_change(self) -> AskDevAdminSettingsUpdate:
        if not self.model_fields_set:
            raise ValueError("at least one Ask Dev setting is required")
        return self


class AskDevAdminRequestLimits(StrictAdminModel):
    active_runs_per_user: int = Field(ge=1)
    active_runs_per_organization: int = Field(ge=1)
    requests_per_user_per_15_minutes: int = Field(ge=1)
    requests_per_organization_per_hour: int = Field(ge=1)


def _retention_options() -> list[Literal[0, 30]]:
    return [0, 30]


def _fallback_options() -> list[Literal["fail_closed", "platform"]]:
    return ["fail_closed", "platform"]


class AskDevAdminResponse(StrictAdminModel):
    schema_version: Literal["ask_dev_admin.v1"] = "ask_dev_admin.v1"
    entitlement_state: EntitlementState
    ask_dev_enabled: bool
    chat_window_available: bool
    full_page_available: bool
    effective_provider_label: str | None = Field(default=None, max_length=256)
    effective_model_label: str | None = Field(default=None, max_length=256)
    provider_source: Literal["platform", "byo"] | None = None
    readiness: ReadinessState
    readiness_checked_at: AwareDatetime | None = None
    readiness_version: str | None = Field(default=None, max_length=128)
    administrator_safe_failure_reason: str | None = Field(
        default=None, max_length=2_048
    )
    settings: AskDevAdminSettings
    retention_options: list[Literal[0, 30]] = Field(default_factory=_retention_options)
    fallback_options: list[Literal["fail_closed", "platform"]] = Field(
        default_factory=_fallback_options
    )
    request_limits: AskDevAdminRequestLimits
    no_training_by_default: Literal[True] = True


class AskDevAdminUsageResponse(StrictAdminModel):
    schema_version: Literal["ask_dev_admin_usage.v1"] = "ask_dev_admin_usage.v1"
    use_case: Literal["ask_dev"] = "ask_dev"
    since: AwareDatetime
    through: AwareDatetime
    request_count: int = Field(ge=0)
    run_count: int = Field(ge=0)
    completed_runs: int = Field(ge=0)
    failed_runs: int = Field(ge=0)
    degraded_runs: int = Field(ge=0)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    estimated_cost_microusd: int | None = Field(default=None, ge=0)
    failure_rate: float = Field(ge=0, le=1)
    degraded_rate: float = Field(ge=0, le=1)
    readiness: ReadinessState


def _block_impersonated_write(user: AuthenticatedUser) -> None:
    if user.impersonated_by:
        raise HTTPException(
            status_code=403,
            detail="Ask Dev administrative actions are unavailable while impersonating",
        )


async def _feature_state(
    session: AsyncSession, org_id: str
) -> tuple[EntitlementState, bool, str | None]:
    try:
        decision = await evaluate_org_feature_async(
            session, uuid.UUID(org_id), ASK_DEV_FEATURE
        )
    except Exception:
        return "unavailable", False, "Ask Dev entitlement is temporarily unavailable."
    if decision.allowed:
        return "enabled", True, None
    if decision.reason is FeatureDecisionReason.GLOBAL_DISABLED:
        return "globally_disabled", False, "Ask Dev is globally disabled."
    if decision.reason in {
        FeatureDecisionReason.INVALID_FEATURE_STATE,
        FeatureDecisionReason.STORAGE_ERROR,
        FeatureDecisionReason.FEATURE_NOT_REGISTERED,
    }:
        return "unavailable", False, "Ask Dev entitlement is temporarily unavailable."
    return "not_entitled", False, "Ask Dev is not entitled for this organization."


def _checked_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


async def _admin_response(
    session: AsyncSession,
    *,
    org_id: str,
) -> AskDevAdminResponse:
    settings_service = SettingsService(session, org_id)
    policy = await load_ask_dev_org_policy(settings_service)
    entitlement_state, entitled, entitlement_failure = await _feature_state(
        session, org_id
    )
    readiness_record = await SettingsAgentReadinessStore(settings_service).load()

    provider_label: str | None = None
    model_label: str | None = None
    provider_source: Literal["platform", "byo"] | None = None
    readiness: ReadinessState = "missing_credentials"
    failure_reason: str | None = None
    resolution = None

    if not entitled:
        readiness = "disabled"
        failure_reason = entitlement_failure
    elif policy.emergency_disabled:
        entitlement_state = "org_disabled"
        readiness = "disabled"
        failure_reason = "Ask Dev is disabled by an organization administrator."
    else:
        try:
            resolution = await resolve_certification_provider(session, org_id=org_id)
            provider_label = resolution.provider_label
            model_label = resolution.model_label
            provider_source = resolution.source.value
            if readiness_record is None:
                readiness = "stale_readiness"
                failure_reason = "The configured Ask Dev model has not been certified."
            elif readiness_record.is_current(
                fingerprint=resolution.readiness_fingerprint,
                readiness_version=READINESS_VERSION,
            ):
                if os.getenv("JWT_SECRET_KEY"):
                    readiness = "ready"
                else:
                    readiness = "degraded"
                    failure_reason = "Ask Dev evidence signing is unavailable."
            elif (
                readiness_record.fingerprint == resolution.readiness_fingerprint
                and readiness_record.readiness_version == READINESS_VERSION
                and readiness_record.outcome is AgentReadinessOutcome.FAILED
            ):
                readiness = "degraded"
                failure_reason = "The configured Ask Dev model failed certification."
            else:
                readiness = "stale_readiness"
                failure_reason = (
                    "Ask Dev readiness is stale for the current configuration."
                )
        except DevRuntimeUnavailable as exc:
            readiness = (
                "unsupported_model"
                if exc.code == "model_not_supported"
                else "missing_credentials"
            )
            failure_reason = exc.safe_message
        except Exception:
            readiness = "degraded"
            failure_reason = "Ask Dev readiness is temporarily unavailable."
        finally:
            if resolution is not None:
                try:
                    await resolution.provider.aclose()
                except Exception:
                    pass

    available = entitled and not policy.emergency_disabled and readiness == "ready"
    limits = DevAdmissionLimits()
    return AskDevAdminResponse(
        entitlement_state=entitlement_state,
        ask_dev_enabled=entitled and not policy.emergency_disabled,
        chat_window_available=available,
        full_page_available=available,
        effective_provider_label=provider_label,
        effective_model_label=model_label,
        provider_source=provider_source,
        readiness=readiness,
        readiness_checked_at=(
            _checked_at(readiness_record.checked_at) if readiness_record else None
        ),
        readiness_version=(
            readiness_record.readiness_version
            if readiness_record
            else READINESS_VERSION
        ),
        administrator_safe_failure_reason=failure_reason,
        settings=AskDevAdminSettings(
            retention_days=policy.retention_days,
            fallback_policy=policy.fallback_policy,
            emergency_disabled=policy.emergency_disabled,
        ),
        request_limits=AskDevAdminRequestLimits(
            active_runs_per_user=limits.active_runs_per_user,
            active_runs_per_organization=limits.active_runs_per_org,
            requests_per_user_per_15_minutes=limits.requests_per_user_per_15_minutes,
            requests_per_organization_per_hour=limits.requests_per_org_per_hour,
        ),
    )


@router.get("/ask-dev", response_model=AskDevAdminResponse)
async def get_ask_dev_admin(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> AskDevAdminResponse:
    return await _admin_response(session, org_id=org_id)


@router.patch("/ask-dev/settings", response_model=AskDevAdminResponse)
async def update_ask_dev_admin_settings(
    payload: AskDevAdminSettingsUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user: AuthenticatedUser = Depends(get_admin_user),
) -> AskDevAdminResponse:
    _block_impersonated_write(user)
    settings = SettingsService(session, org_id)
    category = SettingCategory.ASK_DEV.value
    if payload.retention_days is not None:
        await settings.set(
            ASK_DEV_RETENTION_KEY,
            str(payload.retention_days),
            category,
            description="Ask Dev conversation content retention in days",
        )
    if payload.fallback_policy is not None:
        await settings.set(
            ASK_DEV_FALLBACK_KEY,
            payload.fallback_policy,
            category,
            description="Explicit Ask Dev platform fallback policy",
        )
    if payload.emergency_disabled is not None:
        await settings.set(
            ASK_DEV_EMERGENCY_DISABLED_KEY,
            "true" if payload.emergency_disabled else "false",
            category,
            description="Organization emergency disable for both Ask Dev surfaces",
        )
    await session.flush()
    return await _admin_response(session, org_id=org_id)


@router.post("/ask-dev/readiness", response_model=AskDevAdminResponse)
async def run_ask_dev_readiness(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user: AuthenticatedUser = Depends(get_admin_user),
) -> AskDevAdminResponse:
    _block_impersonated_write(user)
    policy = await load_ask_dev_org_policy(SettingsService(session, org_id))
    entitlement_state, entitled, _ = await _feature_state(session, org_id)
    if not entitled or policy.emergency_disabled:
        reason = (
            "the organization emergency disable is active"
            if policy.emergency_disabled
            else entitlement_state
        )
        raise HTTPException(
            status_code=403,
            detail=f"Ask Dev readiness cannot run while {reason}",
        )
    try:
        resolution = await resolve_certification_provider(session, org_id=org_id)
    except DevRuntimeUnavailable:
        return await _admin_response(session, org_id=org_id)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Ask Dev readiness is temporarily unavailable",
        ) from exc
    try:
        await AgentReadinessService(
            SettingsAgentReadinessStore(SettingsService(session, org_id)),
            org_id=org_id,
        ).certify(
            resolution.provider,
            provider_name=resolution.family,
            model=resolution.model,
            fingerprint=resolution.readiness_fingerprint,
        )
    finally:
        try:
            await resolution.provider.aclose()
        except Exception:
            pass
    return await _admin_response(session, org_id=org_id)


@router.get("/ask-dev/usage", response_model=AskDevAdminUsageResponse)
async def get_ask_dev_usage(
    since: Annotated[datetime | None, Query()] = None,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> AskDevAdminUsageResponse:
    through = datetime.now(UTC)
    response_since = since or through - timedelta(days=30)
    if response_since.tzinfo is None:
        raise HTTPException(status_code=422, detail="since must include a timezone")
    if response_since >= through:
        raise HTTPException(status_code=422, detail="since must be before now")

    failed_case = case((DevRun.state == "failed", 1), else_=0)
    degraded_case = case((DevRun.state == "insufficient_evidence", 1), else_=0)
    completed_case = case((DevRun.state == "completed", 1), else_=0)
    statement = select(
        func.count(DevRun.id),
        func.coalesce(func.sum(completed_case), 0),
        func.coalesce(func.sum(failed_case), 0),
        func.coalesce(func.sum(degraded_case), 0),
        func.coalesce(func.sum(DevRun.input_tokens), 0),
        func.coalesce(func.sum(DevRun.output_tokens), 0),
        func.sum(DevRun.estimated_cost_microusd),
    ).where(
        DevRun.org_id == uuid.UUID(org_id),
        DevRun.started_at >= response_since,
        DevRun.started_at <= through,
    )
    row = (await session.execute(statement)).one()
    run_count = int(row[0] or 0)
    failed_runs = int(row[2] or 0)
    degraded_runs = int(row[3] or 0)
    admin = await _admin_response(session, org_id=org_id)
    return AskDevAdminUsageResponse(
        since=response_since,
        through=through,
        request_count=run_count,
        run_count=run_count,
        completed_runs=int(row[1] or 0),
        failed_runs=failed_runs,
        degraded_runs=degraded_runs,
        input_tokens=int(row[4] or 0),
        output_tokens=int(row[5] or 0),
        estimated_cost_microusd=(int(row[6]) if row[6] is not None else None),
        failure_rate=(failed_runs / run_count if run_count else 0.0),
        degraded_rate=(degraded_runs / run_count if run_count else 0.0),
        readiness=admin.readiness,
    )
