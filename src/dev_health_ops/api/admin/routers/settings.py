from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.llm_settings import (
    LLMSettingsAccessError,
    require_byo_llm_access,
)
from dev_health_ops.api.admin.middleware import (
    block_impersonated_write,
    get_admin_org_id,
    get_admin_user,
)
from dev_health_ops.api.admin.schemas import (
    LLMBudgetResponse,
    LLMSettingsResponse,
    LLMSettingsStatusResponse,
    LLMSettingsUpsert,
    LLMSpendResponse,
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
)
from dev_health_ops.api.dev.production_runtime import (
    _byo_candidate,
    _readiness_fingerprint,
    resolve_byo_certification_provider,
)
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.db import require_clickhouse_uri
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessService,
    SettingsAgentReadinessStore,
    readiness_failure_state,
)
from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
from dev_health_ops.llm.agent.roles import AgentRole, SettingsRoleCertificationStore
from dev_health_ops.llm.credentials import (
    evaluate_org_llm_status,
    latest_recent_org_byo_base_url_fallback_at,
)
from dev_health_ops.metrics.schemas import LLMTokenSpendSummaryRecord
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.models.settings import SettingCategory

from .ask_dev import _checked_at, _role_readiness_list
from .common import get_session

router = APIRouter()
logger = logging.getLogger(__name__)

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
    org_id: str = Depends(get_admin_org_id),
) -> SettingsListResponse:
    raise_served_by_go_api("/api/v1/admin/settings/{category}", GO_API)


@router.get(
    "/llm-settings",
    response_model=LLMSettingsResponse,
    response_model_exclude_none=True,
)
async def get_llm_settings(
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsResponse:
    raise_served_by_go_api("/api/v1/admin/llm-settings", GO_API)


async def _llm_settings_status_response(
    session: AsyncSession, org_id: str
) -> LLMSettingsStatusResponse:
    svc = SettingsService(session, org_id)
    evaluation = await evaluate_org_llm_status(org_id, svc)
    last_fallback_at = await latest_recent_org_byo_base_url_fallback_at(
        session, org_id, evaluation
    )
    # This org-scoped "ask_dev_agent_readiness" slot is BYO's alone in
    # practice going forward: the platform-owned provider's own readiness now
    # lives in the org_id="" sentinel scope (see
    # dev_health_ops.llm.agent.readiness), reachable only from the Platform
    # Admin router. Any record found here can only have been written by this
    # org's own BYO preflight (POST /llm-settings/readiness) -- fingerprints
    # differ by candidate/source so a stray platform certification could
    # never collide with it even before this migration.
    #
    # A record's outcome is only trusted as "ready"/"failed" when its
    # fingerprint AND readiness_version still match the org's CURRENT BYO
    # candidate -- exactly the same currency check ask_dev.py's
    # _admin_response uses (record.is_current(...)). A stale record (the org
    # edited its BYO config since the last check, OR a READINESS_VERSION bump
    # invalidated every stored certification platform-wide, e.g. CHAOS-3254)
    # reports "stale" with a safe, accurate remediation -- never silently
    # reused as "ready", and never reported as if something is broken.
    readiness_record = await SettingsAgentReadinessStore(svc).load()
    binary_transport_readiness: Literal["ready", "failed", "stale", "never_checked"] = (
        "never_checked"
    )
    readiness_checked_at: datetime | None = None
    readiness_safe_failure_reason: str | None = None
    # Computed unconditionally (not just when a binary record exists): it is
    # also the legacy_agent role's certification_key, needed below
    # regardless of whether the binary probe has ever run.
    current_byo = await _byo_candidate(svc, readiness=None, certification=True)
    current_fingerprint = (
        _readiness_fingerprint(current_byo) if current_byo is not None else None
    )
    if readiness_record is not None:
        readiness_checked_at = _checked_at(readiness_record.checked_at)
        is_current = (
            current_fingerprint is not None
            and readiness_record.fingerprint == current_fingerprint
            and readiness_record.readiness_version == READINESS_VERSION
        )
        if not is_current:
            binary_transport_readiness = "stale"
            readiness_safe_failure_reason = (
                "This configuration has not been certified under the current "
                "readiness requirements. Run preflight again."
            )
        elif readiness_record.outcome is AgentReadinessOutcome.READY:
            binary_transport_readiness = "ready"
        else:
            binary_transport_readiness = "failed"
            _, readiness_safe_failure_reason = readiness_failure_state(
                readiness_record.safe_error_code
            )

    # CHAOS-3285 round 2 (Codex HIGH): the binary transport check alone is
    # never sufficient for "ready" -- live selection also requires a
    # current, COMPATIBLE legacy_agent role certification (see
    # production_runtime.py _candidate()). Combine them the same way
    # ask_dev.py's admin surface does, only overriding when the binary
    # check itself passed.
    role_profile = await SettingsRoleCertificationStore(svc).load()
    role_readiness = _role_readiness_list(
        role_profile, legacy_agent_certification_key=current_fingerprint
    )
    legacy_role_entry = next(
        (entry for entry in role_readiness if entry.role == "legacy_agent"), None
    )
    readiness = binary_transport_readiness
    if (
        binary_transport_readiness == "ready"
        and legacy_role_entry is not None
        and legacy_role_entry.state != "ready"
    ):
        readiness_safe_failure_reason = legacy_role_entry.safe_remediation
        readiness = (
            "never_checked"
            if legacy_role_entry.state == "not_yet_certified"
            else "stale"
            if legacy_role_entry.state == "stale_readiness"
            else "failed"
        )
    return LLMSettingsStatusResponse(
        configured=evaluation.configured,
        active=evaluation.active,
        degraded=evaluation.reason_code == "invalid_base_url",
        reason_code=evaluation.reason_code,
        last_fallback_at=last_fallback_at,
        readiness=readiness,
        binary_transport_readiness=binary_transport_readiness,
        readiness_checked_at=readiness_checked_at,
        readiness_safe_failure_reason=readiness_safe_failure_reason,
    )


@router.get(
    "/llm-settings/status",
    response_model=LLMSettingsStatusResponse,
)
async def get_llm_settings_status(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> LLMSettingsStatusResponse:
    await _require_byo_llm_tier(session, org_id)
    return await _llm_settings_status_response(session, org_id)


@router.post(
    "/llm-settings/readiness",
    response_model=LLMSettingsStatusResponse,
)
async def run_llm_settings_readiness(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> LLMSettingsStatusResponse:
    """Certify the org's OWN saved BYO LLM configuration.

    TRIGGERING this check is independent of Ask Dev's provider-selection
    arbitration: it runs based on BYO configuration being saved, regardless
    of whether BYO currently wins Ask Dev's fallback arbitration, whether it
    has ever been certified before, or whether Ask Dev itself is
    enabled/entitled. It never touches DevRun, Ask Dev's platform-allowance
    tables, or Ask Dev's entitlement/emergency-disabled checks, and it never
    reads or writes Ask Dev's fallback POLICY (fail_closed vs platform).

    It DOES write to the same per-org readiness slot
    (``SettingsAgentReadinessStore``, category=llm, key=ask_dev_agent_readiness)
    that ``resolve_production_provider`` reads to decide whether the BYO
    candidate is "current" (see production_runtime._provider_candidates). That
    is intentional, not a side channel: marking BYO's own credentials as
    certified is precisely what makes BYO usable/selectable for subsequent
    LIVE Ask Dev runs (real chat execution, and the ordinary end-user
    `/dev/capabilities` projection) -- that's the entire point of a preflight
    check. A SUCCESSFUL run here can therefore change which provider a later
    live run resolves to (e.g. flip it from platform-fallback to BYO), exactly
    as running the org's old, now-removed certify flow always did.

    NOTE this does NOT affect the org-admin `GET /ask-dev` projection's
    `provider_source`/`readiness` fields: that route resolves via
    ``resolve_certification_provider`` (certification bypass, see
    production_runtime.py), which already prefers a validly-shaped BYO
    candidate over platform as soon as BYO settings are saved and complete --
    independent of whether it has ever been certified. Only the LIVE
    selection path (`resolve_production_provider`) is gated on certification
    currency, and that is the path this endpoint's write affects.

    What changed under CHAOS-3265 is only WHO can reach platform's own
    certification (Platform Admin only) and WHERE that lives -- not this
    BYO-certifies-BYO mechanic, which is unchanged and working as designed
    (CHAOS-3265; see test_llm_settings_readiness_can_flip_ask_dev_selection_to_byo
    for a test that proves this honestly against the real, unmocked live
    resolver instead of masking it).
    """

    await _require_byo_llm_tier(session, org_id)
    block_impersonated_write(
        current_user,
        detail={
            "error": "impersonated_write_forbidden",
            "message": "BYO LLM readiness checks are unavailable while impersonating",
        },
    )
    try:
        resolution = await resolve_byo_certification_provider(session, org_id=org_id)
    except DevRuntimeUnavailable as exc:
        raise HTTPException(status_code=404, detail=exc.safe_message) from exc
    try:
        await AgentReadinessService(
            SettingsAgentReadinessStore(SettingsService(session, org_id))
        ).certify(
            resolution.provider,
            provider_name=resolution.family,
            model=resolution.model,
            fingerprint=resolution.readiness_fingerprint,
        )
        try:
            # CHAOS-3285: certify the legacy_agent role in the new per-role
            # store too, on the same already-resolved provider. Live
            # selection (resolve_production_provider) now REQUIRES a
            # current, COMPATIBLE legacy_agent role record in addition to
            # the binary record above -- without this call, this org's BYO
            # candidate would become permanently unselectable for live
            # traffic despite the binary preflight above reporting "ready"
            # (Codex CHAOS-3285 review). Best-effort: a bug here must never
            # regress the existing, already-relied-upon binary preflight
            # result written above.
            #
            # CHAOS-3285 round 2 (Codex MEDIUM): run inside a SAVEPOINT
            # (begin_nested), never bare -- see platform_ask_dev.py's
            # identical comment for the full rationale. A DB error caught
            # without a rollback/savepoint poisons the session's
            # transaction; the request's own session dependency then rolls
            # back the WHOLE transaction on its final commit attempt,
            # silently discarding the binary certify() write just above.
            async with session.begin_nested():
                await RoleReadinessService(
                    SettingsRoleCertificationStore(SettingsService(session, org_id))
                ).certify_role(
                    AgentRole.LEGACY_AGENT,
                    resolution.provider,
                    certification_key=resolution.readiness_fingerprint,
                )
        except Exception:
            logger.warning(
                "Failed to certify the legacy_agent role during BYO Ask Dev preflight",
                exc_info=True,
            )
    finally:
        try:
            await resolution.provider.aclose()
        except Exception:
            # Best-effort cleanup only: the certify() call above has already
            # persisted the readiness result (or its own failure state), so a
            # transport-close error here must never mask that outcome or fail
            # this request. Still worth knowing about (a leaked connection is
            # an operational signal), so log it rather than swallow it.
            logger.warning(
                "Failed to close BYO Ask Dev provider connection after preflight",
                exc_info=True,
            )
    return await _llm_settings_status_response(session, org_id)


@router.get(
    "/llm-settings/budget",
    response_model=LLMBudgetResponse,
)
async def get_llm_settings_budget(
    org_id: str = Depends(get_admin_org_id),
) -> LLMBudgetResponse:
    raise_served_by_go_api("/api/v1/admin/llm-settings/budget", GO_API)


@router.get(
    "/llm-settings/spend",
    response_model=LLMSpendResponse,
)
async def get_llm_settings_spend(
    limit: int = Query(20, ge=1),
    since: datetime | None = None,
    org_id: str = Depends(get_admin_org_id),
    spend_reader: LLMSpendReader = Depends(get_llm_spend_reader),
) -> LLMSpendResponse:
    raise_served_by_go_api("/api/v1/admin/llm-settings/spend", GO_API)


@router.put(
    "/llm-settings",
    response_model=LLMSettingsResponse,
    response_model_exclude_none=True,
)
async def upsert_llm_settings(
    payload: LLMSettingsUpsert,
    org_id: str = Depends(get_admin_org_id),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> LLMSettingsResponse:
    raise_served_by_go_api("/api/v1/admin/llm-settings", GO_API)


@router.delete("/llm-settings")
async def delete_llm_settings(
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, bool]:
    # DELETE must remain available so an admin can clean up stored BYO secrets
    # even when the byo_llm flag is disabled or the org has been downgraded
    # below the BYO tier (CHAOS-2551 review).
    raise_served_by_go_api("/api/v1/admin/llm-settings", GO_API)


@router.get("/settings/{category}/{key}", response_model=SettingResponse)
async def get_setting(
    category: str,
    key: str,
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    raise_served_by_go_api("/api/v1/admin/settings/{category}/{key}", GO_API)


@router.put("/settings/{category}/{key}", response_model=SettingResponse)
async def set_setting(
    category: str,
    key: str,
    payload: SettingUpdate,
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    raise_served_by_go_api("/api/v1/admin/settings/{category}/{key}", GO_API)


@router.post("/settings", response_model=SettingResponse)
async def create_setting(
    payload: SettingCreate,
    org_id: str = Depends(get_admin_org_id),
) -> SettingResponse:
    raise_served_by_go_api("/api/v1/admin/settings", GO_API)


@router.delete("/settings/{category}/{key}")
async def delete_setting(
    category: str,
    key: str,
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    raise_served_by_go_api("/api/v1/admin/settings/{category}/{key}", GO_API)
