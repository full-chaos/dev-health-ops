"""Organization-administrator controls for the shared Ask Dev experience."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import (
    block_impersonated_write,
    get_admin_org_id,
    get_admin_user,
)
from dev_health_ops.api.dev.org_policy import (
    ASK_DEV_EMERGENCY_DISABLED_KEY,
    ASK_DEV_FALLBACK_KEY,
    ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY,
    ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY,
    ASK_DEV_RETENTION_KEY,
    ASK_DEV_RUN_COST_HARD_MAX_MICROUSD,
    PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
    PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX,
    PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
    load_ask_dev_org_policy,
    platform_operator_cost_limit_microusd,
    platform_operator_request_limit,
)
from dev_health_ops.api.dev.persistence.service import DevAdmissionLimits
from dev_health_ops.api.dev.production_runtime import resolve_certification_provider
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.services import askdev_allowance_counters
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.licensing import FeatureDecisionReason, evaluate_org_feature_async
from dev_health_ops.licensing.registry import ASK_DEV_FEATURE
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY as _PLATFORM_READINESS_SETTING_KEY,
)
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_SETTINGS_ORG_ID as _PLATFORM_SETTINGS_ORG_ID,
)
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    ReadinessState,
    SettingsAgentReadinessStore,
    readiness_failure_state,
)
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    RoleCertificationProfile,
    RoleCertificationRecord,
    RoleCertificationState,
    SettingsRoleCertificationStore,
)
from dev_health_ops.models.dev_persistence import DevRun
from dev_health_ops.models.settings import SettingCategory

from .common import get_session

router = APIRouter()
logger = logging.getLogger(__name__)

# A generic, organization-safe unavailability message. When Ask Dev's
# effective provider resolves to the PLATFORM candidate, the org-admin
# response must never surface the platform's own safe failure reason (that
# is Platform Admin's diagnostic, not this organization's) -- CHAOS-3265.
_PLATFORM_GENERIC_UNAVAILABLE_MESSAGE = (
    "Ask Dev is temporarily unavailable. Contact your platform operator."
)

EntitlementState = Literal[
    "enabled", "not_entitled", "globally_disabled", "org_disabled", "unavailable"
]

# CHAOS-3285: the safe, wire-stable vocabulary for a per-role certification
# projection. This is deliberately NOT RoleCertificationState (that enum is
# an internal capability-model detail -- COMPATIBLE/INCOMPATIBLE/FAILED/
# STALE/UNCHECKED -- and must never leak onto the admin wire). It extends the
# existing ReadinessState vocabulary with exactly one new value,
# "not_yet_certified", so a role nothing has ever certified reads as visibly
# distinct from "stale_readiness" (was certified, now invalidated) and from
# every FAILED-derived state (a certification attempt that did not pass).
RoleReadinessState = Literal[
    "ready",
    "unsupported_model",
    "missing_credentials",
    "disabled",
    "degraded",
    "stale_readiness",
    "not_yet_certified",
]

_ROLE_ORDER: tuple[AgentRole, ...] = (
    AgentRole.LEGACY_AGENT,
    AgentRole.INTENT_CLASSIFICATION,
    AgentRole.ANSWER_FRAME_NARRATIVE,
)


class StrictAdminModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AskDevRoleReadiness(StrictAdminModel):
    schema_version: Literal["ask_dev_role_readiness.v1"] = "ask_dev_role_readiness.v1"
    role: Literal["legacy_agent", "intent_classification", "answer_frame_narrative"]
    state: RoleReadinessState
    checked_at: AwareDatetime | None = None
    safe_remediation: str | None = Field(default=None, max_length=2_048)


def _role_readiness_entry(
    role: AgentRole,
    record: RoleCertificationRecord | None,
    *,
    certification_key: str | None,
) -> AskDevRoleReadiness:
    """Project one role's stored certification record into the safe wire
    vocabulary. ``certification_key`` is the CURRENT capability-input
    fingerprint for that role (e.g. from ``_readiness_fingerprint``); when
    unavailable (a role with no probe yet) a stored record's state is
    reported as-is, without a staleness check against a key we cannot
    compute here.
    """

    if record is None:
        return AskDevRoleReadiness(
            role=role.value,
            state="not_yet_certified",
            checked_at=None,
            safe_remediation="This Ask Dev role has not been certified yet.",
        )
    # Deliberately NOT record.is_current() -- that method also requires
    # state is COMPATIBLE, which is right for runtime *selection* but wrong
    # for admin *display*: a FAILED/INCOMPATIBLE record whose capability
    # inputs still match the current configuration must show its actual
    # failure reason (e.g. unsupported_model), not be recolored as merely
    # "stale" just because it never became COMPATIBLE. Staleness here means
    # only one thing: the record was computed against a DIFFERENT
    # certification_key than the one in effect now.
    if certification_key is not None and record.certification_key != certification_key:
        return AskDevRoleReadiness(
            role=role.value,
            state="stale_readiness",
            checked_at=_checked_at(record.checked_at),
            safe_remediation=(
                "This role's certification is stale for the current "
                "configuration. Run preflight again."
            ),
        )
    if record.state is RoleCertificationState.COMPATIBLE:
        return AskDevRoleReadiness(
            role=role.value,
            state="ready",
            checked_at=_checked_at(record.checked_at),
            safe_remediation=None,
        )
    state, remediation = readiness_failure_state(record.safe_error_code)
    return AskDevRoleReadiness(
        role=role.value,
        state=state,
        checked_at=_checked_at(record.checked_at),
        safe_remediation=remediation,
    )


def _role_readiness_list(
    profile: RoleCertificationProfile,
    *,
    legacy_agent_certification_key: str | None,
) -> list[AskDevRoleReadiness]:
    """Build the full three-role projection. Only ``legacy_agent`` has a
    live certification_key today (CHAOS-3285 PR3's probe); intent/narrative
    have no probe yet (PR4) so their entries fall back to reporting whatever
    is stored (ordinarily nothing -> not_yet_certified)."""

    certification_keys: dict[AgentRole, str | None] = {
        AgentRole.LEGACY_AGENT: legacy_agent_certification_key,
        AgentRole.INTENT_CLASSIFICATION: None,
        AgentRole.ANSWER_FRAME_NARRATIVE: None,
    }
    return [
        _role_readiness_entry(
            role, profile.for_role(role), certification_key=certification_keys[role]
        )
        for role in _ROLE_ORDER
    ]


class AskDevAdminSettings(StrictAdminModel):
    retention_days: Literal[0, 30]
    fallback_policy: Literal["fail_closed", "platform"]
    emergency_disabled: bool
    platform_monthly_request_limit: int = Field(
        ge=PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
        le=PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX,
    )
    platform_monthly_cost_limit_microusd: int = Field(
        ge=PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
        le=PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    )


class AskDevAdminSettingsUpdate(StrictAdminModel):
    retention_days: Literal[0, 30] | None = None
    fallback_policy: Literal["fail_closed", "platform"] | None = None
    emergency_disabled: bool | None = None
    platform_monthly_request_limit: int | None = Field(
        default=None,
        ge=PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
        le=PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX,
    )
    platform_monthly_cost_limit_microusd: int | None = Field(
        default=None,
        ge=PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
        le=PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    )

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


class AskDevPlatformAllowanceBounds(StrictAdminModel):
    request_minimum: int = Field(ge=1)
    request_maximum: int = Field(ge=1)
    cost_minimum_microusd: int = Field(ge=1)
    cost_maximum_microusd: int = Field(ge=1)


class AskDevPlatformAllowanceUsage(StrictAdminModel):
    window_start: AwareDatetime
    reset_at: AwareDatetime
    request_limit: int = Field(ge=1)
    request_used: int = Field(ge=0)
    request_remaining: int = Field(ge=0)
    cost_limit_microusd: int = Field(ge=1)
    cost_used_microusd: int = Field(ge=0)
    cost_remaining_microusd: int = Field(ge=0)
    warning: Literal["none", "eighty_percent", "ninety_percent", "exhausted"]


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
    # CHAOS-3285 round 2 (Codex HIGH): this is now the EFFECTIVE readiness --
    # combining the transport-echo binary check AND the legacy_agent role
    # certification live selection actually requires. A binary-ready-but-
    # role-incompatible provider must never report "ready" here (it would
    # contradict live selection, which already rejects it). The raw,
    # binary-only result is kept separately below as a named diagnostic.
    readiness: ReadinessState
    binary_transport_readiness: ReadinessState
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
    platform_allowance_bounds: AskDevPlatformAllowanceBounds
    no_training_by_default: Literal[True] = True
    # CHAOS-3285: additive field. A single generic readiness badge cannot
    # represent "certified for narrative but not for the legacy full-agent
    # role" -- see AskDevRoleReadiness. Deliberately NOT a schema_version
    # bump: dev-health-web has not yet been updated to read this field, and
    # bumping schema_version on an already-deployed contract is a
    # coordinated, separate change (plan risk R6).
    role_readiness: list[AskDevRoleReadiness] = Field(default_factory=list)


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
    platform_allowance: AskDevPlatformAllowanceUsage


def _allowance_warning(*, used: int, limit: int) -> int:
    if used >= limit:
        return 3
    if used * 10 >= limit * 9:
        return 2
    if used * 10 >= limit * 8:
        return 1
    return 0


async def _platform_allowance_usage(
    session: AsyncSession,
    *,
    org_id: str,
    request_limit: int,
    cost_limit_microusd: int,
) -> AskDevPlatformAllowanceUsage:
    # CHAOS-3522: reads the shared Valkey counters (self-healing from
    # dev_runs on a cold key or Valkey outage) instead of re-scanning
    # dev_runs on every admin usage read.
    counts, window_start, reset_at = await askdev_allowance_counters.read_counts(
        session,
        org_id=org_id,
        per_run_reservation_microusd=ASK_DEV_RUN_COST_HARD_MAX_MICROUSD,
    )
    requests = counts.requests
    cost = counts.cost_microusd
    warning_rank = max(
        _allowance_warning(used=requests, limit=request_limit),
        _allowance_warning(used=cost, limit=cost_limit_microusd),
    )
    warning = ("none", "eighty_percent", "ninety_percent", "exhausted")[warning_rank]
    return AskDevPlatformAllowanceUsage(
        window_start=window_start,
        reset_at=reset_at,
        request_limit=request_limit,
        request_used=requests,
        request_remaining=max(0, request_limit - requests),
        cost_limit_microusd=cost_limit_microusd,
        cost_used_microusd=cost,
        cost_remaining_microusd=max(0, cost_limit_microusd - cost),
        warning=warning,
    )


def _platform_readiness_store(session: AsyncSession) -> SettingsAgentReadinessStore:
    """The platform-owned provider's readiness store (see production_runtime)."""

    return SettingsAgentReadinessStore(
        SettingsService(session, _PLATFORM_SETTINGS_ORG_ID),
        key=_PLATFORM_READINESS_SETTING_KEY,
    )


def _platform_role_store(session: AsyncSession) -> SettingsRoleCertificationStore:
    """The platform-owned provider's per-role certification store."""

    return SettingsRoleCertificationStore(
        SettingsService(session, _PLATFORM_SETTINGS_ORG_ID),
        key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
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
    # CHAOS-3285: only populated when a candidate actually resolves below --
    # entitlement-disabled/org-disabled runs never attempted a certification
    # check at all, so an empty list (no data) is more honest here than a
    # fabricated "not_yet_certified" landscape for a feature that is off.
    role_readiness: list[AskDevRoleReadiness] = []

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
            if provider_source == "platform":
                # Platform's own certification lives in the platform-global
                # scope -- only Platform Admin can trigger it (CHAOS-3265).
                # Re-read from there so an org relying on platform fallback
                # reflects the ACTUAL platform certification state, not a
                # stale/absent org-scoped record this org can no longer
                # produce.
                readiness_record = await _platform_readiness_store(session).load()
                role_store = _platform_role_store(session)
            else:
                role_store = SettingsRoleCertificationStore(settings_service)
            role_profile = await role_store.load()
            role_readiness = _role_readiness_list(
                role_profile,
                legacy_agent_certification_key=resolution.readiness_fingerprint,
            )
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
                readiness, failure_reason = readiness_failure_state(
                    readiness_record.safe_error_code
                )
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
                    # Best-effort cleanup only: the readiness state above is
                    # already finalized, so a transport-close error here must
                    # never mask that outcome or fail this request. Still
                    # worth knowing about (a leaked connection is an
                    # operational signal), so log it rather than swallow it.
                    logger.warning(
                        "Failed to close Ask Dev provider connection after readiness check",
                        exc_info=True,
                    )

    # CHAOS-3285 round 2 (Codex HIGH): the binary transport check alone is
    # never sufficient for "ready" -- live selection (production_runtime.py
    # _candidate()) also requires a current, COMPATIBLE legacy_agent role
    # certification. Combine them here so this response can never claim
    # availability that selection would reject. Only override when the
    # binary check itself passed: a binary failure (missing credentials,
    # unsupported model, ...) already explains why nothing is available and
    # must keep its own specific reason rather than being masked by a role
    # state that was never meaningfully evaluated against a failing binary
    # candidate in the first place.
    binary_transport_readiness: ReadinessState = readiness
    legacy_role_entry = next(
        (entry for entry in role_readiness if entry.role == "legacy_agent"), None
    )
    if (
        readiness == "ready"
        and legacy_role_entry is not None
        and legacy_role_entry.state != "ready"
    ):
        # "not_yet_certified" has no ReadinessState counterpart (it exists
        # only to distinguish "never certified" from "stale" in the per-role
        # array); both mean the same thing for this combined top-level
        # field -- run preflight -- so it collapses to stale_readiness here.
        readiness = (
            "stale_readiness"
            if legacy_role_entry.state == "not_yet_certified"
            else legacy_role_entry.state
        )
        failure_reason = legacy_role_entry.safe_remediation

    if provider_source == "platform":
        # This organization's admin surface must never expose the
        # platform-owned provider's identity or its platform-specific
        # failure reason (that belongs to Platform Admin) -- CHAOS-3265.
        provider_label = None
        model_label = None
        provider_source = None
        if readiness != "ready":
            failure_reason = _PLATFORM_GENERIC_UNAVAILABLE_MESSAGE
        # Same boundary (CHAOS-3265): a per-role remediation string must not
        # carry platform-specific diagnostic text into an org's response
        # either. The role/state landscape itself is safe to show (it
        # carries no identity), only the free-text remediation is redacted.
        role_readiness = [
            entry.model_copy(
                update={
                    "safe_remediation": (
                        None
                        if entry.state == "ready"
                        else _PLATFORM_GENERIC_UNAVAILABLE_MESSAGE
                    )
                }
            )
            for entry in role_readiness
        ]

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
        binary_transport_readiness=binary_transport_readiness,
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
            platform_monthly_request_limit=policy.platform_monthly_request_limit,
            platform_monthly_cost_limit_microusd=(
                policy.platform_monthly_cost_limit_microusd
            ),
        ),
        request_limits=AskDevAdminRequestLimits(
            active_runs_per_user=limits.active_runs_per_user,
            active_runs_per_organization=limits.active_runs_per_org,
            requests_per_user_per_15_minutes=limits.requests_per_user_per_15_minutes,
            requests_per_organization_per_hour=limits.requests_per_org_per_hour,
        ),
        platform_allowance_bounds=AskDevPlatformAllowanceBounds(
            request_minimum=PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
            request_maximum=platform_operator_request_limit(),
            cost_minimum_microusd=PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
            cost_maximum_microusd=platform_operator_cost_limit_microusd(),
        ),
        role_readiness=role_readiness,
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
    block_impersonated_write(
        user,
        detail="Ask Dev administrative actions are unavailable while impersonating",
    )
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
    operator_request_limit = platform_operator_request_limit()
    operator_cost_limit = platform_operator_cost_limit_microusd()
    if (
        payload.platform_monthly_request_limit is not None
        and payload.platform_monthly_request_limit > operator_request_limit
    ):
        raise HTTPException(
            status_code=422,
            detail="platform request limit exceeds the provisioned maximum",
        )
    if (
        payload.platform_monthly_cost_limit_microusd is not None
        and payload.platform_monthly_cost_limit_microusd > operator_cost_limit
    ):
        raise HTTPException(
            status_code=422,
            detail="platform cost limit exceeds the provisioned maximum",
        )
    if payload.platform_monthly_request_limit is not None:
        await settings.set(
            ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY,
            str(payload.platform_monthly_request_limit),
            category,
            description="Ask Dev platform monthly accepted-run allowance",
        )
    if payload.platform_monthly_cost_limit_microusd is not None:
        await settings.set(
            ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY,
            str(payload.platform_monthly_cost_limit_microusd),
            category,
            description="Ask Dev platform monthly provider cost allowance in micro-USD",
        )
    await session.flush()
    return await _admin_response(session, org_id=org_id)


@router.post("/ask-dev/readiness", status_code=410)
async def run_ask_dev_readiness(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user: AuthenticatedUser = Depends(get_admin_user),
) -> dict[str, str]:
    """Deprecated: platform certification no longer runs through this route.

    CHAOS-3265 closed the bug where an org admin could trigger and observe
    certification of the PLATFORM-owned provider through this org-scoped
    endpoint. Platform certification now lives exclusively behind
    ``POST /platform/ask-dev/readiness`` (superuser-only, Platform Admin),
    and BYO's own preflight now lives at ``POST /llm-settings/readiness``.

    The route is kept registered -- rather than deleted outright -- purely
    for rolling-deploy safety: an already-deployed older web frontend may
    still call this route before its paired frontend change deploys. It
    intentionally executes NO certification logic and touches no readiness
    store at all; it only returns a static, generic 410 response. This is
    what actually closes the privilege-escalation bug -- there is no code
    path left under this route that resolves or certifies any provider.
    Full route deletion can happen in a follow-up ticket once rollout of
    both the ops and web changes is confirmed complete.
    """

    del session, org_id, user  # deliberately unused: no logic may run here
    return {
        "detail": (
            "Platform preflight has moved to Platform Admin. "
            "Use BYO LLM settings for BYO preflight."
        )
    }


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
    allowance = await _platform_allowance_usage(
        session,
        org_id=org_id,
        request_limit=admin.settings.platform_monthly_request_limit,
        cost_limit_microusd=admin.settings.platform_monthly_cost_limit_microusd,
    )
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
        platform_allowance=allowance,
    )
