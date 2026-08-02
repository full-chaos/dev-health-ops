"""Platform-administrator controls for the platform-owned Ask Dev provider.

CHAOS-3265: previously, an org admin could trigger and observe certification
of the PLATFORM-owned (operator env-configured) LLM provider through the
org-scoped ``/admin/ask-dev`` surface. This router gives Platform Admin its
own, superuser-only surface for that -- it never reads or writes any
organization's settings, DevRun rows, or Ask Dev policy.
"""

from __future__ import annotations

import logging
import os
from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import AwareDatetime, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import (
    block_impersonated_write,
    require_superuser,
)
from dev_health_ops.api.dev.production_runtime import (
    resolve_platform_certification_provider,
)
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY,
    PLATFORM_SETTINGS_ORG_ID,
    AgentReadinessOutcome,
    AgentReadinessService,
    ReadinessState,
    SettingsAgentReadinessStore,
    readiness_failure_state,
)
from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    SettingsRoleCertificationStore,
)

from .ask_dev import (
    AskDevRoleReadiness,
    StrictAdminModel,
    _checked_at,
    _role_readiness_list,
)
from .common import get_session

router = APIRouter()
logger = logging.getLogger(__name__)


class PlatformAskDevReadinessResponse(StrictAdminModel):
    schema_version: Literal["platform_ask_dev_readiness.v1"] = (
        "platform_ask_dev_readiness.v1"
    )
    configured: bool
    provider_label: str | None = Field(default=None, max_length=256)
    model_label: str | None = Field(default=None, max_length=256)
    # CHAOS-3285 round 2 (Codex HIGH): the EFFECTIVE readiness -- binary
    # transport check AND legacy_agent role certification combined. See
    # AskDevAdminResponse's identical field for the full rationale.
    readiness: ReadinessState
    binary_transport_readiness: ReadinessState
    readiness_checked_at: AwareDatetime | None = None
    readiness_version: str | None = Field(default=None, max_length=128)
    safe_remediation: str | None = Field(default=None, max_length=2_048)
    # CHAOS-3285: additive field, same rationale as AskDevAdminResponse's
    # role_readiness -- a single generic badge cannot represent per-role
    # certification. No schema_version bump here either (plan risk R6: web
    # is a separate, coordinated change).
    role_readiness: list[AskDevRoleReadiness] = Field(default_factory=list)


def _platform_store(session: AsyncSession) -> SettingsAgentReadinessStore:
    # The platform-owned provider's readiness record is scoped by the
    # ``org_id=""`` sentinel: every real org's org_id is a non-empty UUID
    # string enforced at the admin-auth boundary (get_admin_org_id 403s on a
    # falsy org_id), so "" can never collide with a real org's rows. This
    # store always explicitly writes/reads org_id="" -- see
    # dev_health_ops.llm.agent.readiness for the full rationale, including
    # why the `settings` table's column default is irrelevant here.
    #
    # Belt-and-suspenders: this ALSO uses a setting key
    # (PLATFORM_READINESS_SETTING_KEY) distinct from the ordinary per-org
    # "ask_dev_agent_readiness" key, so even a stray/buggy write that somehow
    # landed with an empty org_id under the ordinary key would still be
    # invisible to this store (CHAOS-3265).
    return SettingsAgentReadinessStore(
        SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
        key=PLATFORM_READINESS_SETTING_KEY,
    )


def _platform_role_store(session: AsyncSession) -> SettingsRoleCertificationStore:
    """The platform-owned provider's per-role certification store -- same
    ``org_id=""`` sentinel scoping and dedicated-key rationale as
    ``_platform_store`` above (CHAOS-3265), under a key distinct from both
    the ordinary per-org role store AND the legacy binary platform key."""

    return SettingsRoleCertificationStore(
        SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
        key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    )


async def _platform_readiness_response(
    session: AsyncSession,
) -> PlatformAskDevReadinessResponse:
    store = _platform_store(session)
    readiness_record = await store.load()
    try:
        resolution = await resolve_platform_certification_provider()
    except DevRuntimeUnavailable as exc:
        readiness: ReadinessState = (
            "unsupported_model"
            if exc.code == "model_not_supported"
            else "missing_credentials"
        )
        return PlatformAskDevReadinessResponse(
            configured=False,
            provider_label=None,
            model_label=None,
            readiness=readiness,
            binary_transport_readiness=readiness,
            readiness_checked_at=(
                _checked_at(readiness_record.checked_at) if readiness_record else None
            ),
            readiness_version=(
                readiness_record.readiness_version
                if readiness_record
                else READINESS_VERSION
            ),
            safe_remediation=exc.safe_message,
            # No candidate resolved -- there is nothing to project a
            # per-role certification_key against (same reasoning as
            # ask_dev.py's empty-list-when-unresolvable choice).
            role_readiness=[],
        )
    try:
        readiness_state: ReadinessState
        safe_remediation: str | None
        if readiness_record is None:
            readiness_state = "stale_readiness"
            safe_remediation = "The platform Ask Dev model has not been certified."
        elif readiness_record.is_current(
            fingerprint=resolution.readiness_fingerprint,
            readiness_version=READINESS_VERSION,
        ):
            if os.getenv("JWT_SECRET_KEY"):
                readiness_state = "ready"
                safe_remediation = None
            else:
                readiness_state = "degraded"
                safe_remediation = "Ask Dev evidence signing is unavailable."
        elif (
            readiness_record.fingerprint == resolution.readiness_fingerprint
            and readiness_record.readiness_version == READINESS_VERSION
            and readiness_record.outcome is AgentReadinessOutcome.FAILED
        ):
            readiness_state, safe_remediation = readiness_failure_state(
                readiness_record.safe_error_code
            )
        else:
            # Covers both a config/fingerprint change since the last check
            # AND a READINESS_VERSION bump (e.g. CHAOS-3254) invalidating
            # every stored certification platform-wide. Never reused as
            # "ready", and never phrased as if something is broken -- nothing
            # is; it just hasn't been certified under the current
            # requirements yet.
            readiness_state = "stale_readiness"
            safe_remediation = (
                "This configuration has not been certified under the current "
                "readiness requirements. Run preflight again."
            )
        role_profile = await _platform_role_store(session).load()
        role_readiness = _role_readiness_list(
            role_profile,
            legacy_agent_certification_key=resolution.readiness_fingerprint,
        )
        # CHAOS-3285 round 2 (Codex HIGH): combine binary + role, same as
        # ask_dev.py -- only override when the binary check itself passed.
        binary_transport_readiness = readiness_state
        legacy_role_entry = next(
            (entry for entry in role_readiness if entry.role == "legacy_agent"), None
        )
        if (
            readiness_state == "ready"
            and legacy_role_entry is not None
            and legacy_role_entry.state != "ready"
        ):
            readiness_state = (
                "stale_readiness"
                if legacy_role_entry.state == "not_yet_certified"
                else legacy_role_entry.state
            )
            safe_remediation = legacy_role_entry.safe_remediation
        return PlatformAskDevReadinessResponse(
            configured=True,
            provider_label=resolution.provider_label,
            model_label=resolution.model_label,
            readiness=readiness_state,
            binary_transport_readiness=binary_transport_readiness,
            readiness_checked_at=(
                _checked_at(readiness_record.checked_at) if readiness_record else None
            ),
            readiness_version=(
                readiness_record.readiness_version
                if readiness_record
                else READINESS_VERSION
            ),
            safe_remediation=safe_remediation,
            role_readiness=role_readiness,
        )
    finally:
        try:
            await resolution.provider.aclose()
        except Exception:
            pass


@router.get(
    "/platform/ask-dev/readiness",
    response_model=PlatformAskDevReadinessResponse,
)
async def get_platform_ask_dev_readiness(
    session: AsyncSession = Depends(get_session),
    _current_user: AuthenticatedUser = Depends(require_superuser),
) -> PlatformAskDevReadinessResponse:
    return await _platform_readiness_response(session)


@router.post(
    "/platform/ask-dev/readiness",
    response_model=PlatformAskDevReadinessResponse,
)
async def run_platform_ask_dev_readiness(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> PlatformAskDevReadinessResponse:
    # Defense in depth: this route takes no org_id at all and impersonation
    # is otherwise scoped to org-admin actions, so a superuser session
    # created via impersonation should not realistically be able to reach
    # here impersonating anyone. We still block it: platform certification
    # is a platform-operator action and must always run under the
    # superuser's own, non-impersonated identity (CHAOS-3265).
    block_impersonated_write(
        current_user,
        detail="Platform Ask Dev administrative actions are unavailable while impersonating",
    )
    try:
        resolution = await resolve_platform_certification_provider()
    except DevRuntimeUnavailable:
        return await _platform_readiness_response(session)
    try:
        await AgentReadinessService(_platform_store(session)).certify(
            resolution.provider,
            provider_name=resolution.family,
            model=resolution.model,
            fingerprint=resolution.readiness_fingerprint,
        )
        try:
            # CHAOS-3285: certify the legacy_agent role in the new per-role
            # store too, on the same already-resolved provider, so the
            # per-role projection reflects real preflight results rather
            # than staying "not_yet_certified" forever. Best-effort: a bug
            # here must never regress the existing (tested, relied-upon)
            # binary certify() call above -- certify_legacy_agent already
            # turns every AgentProviderError into a FAILED/INCOMPATIBLE
            # record internally, so only a genuinely unexpected failure
            # (e.g. store I/O) reaches this except.
            #
            # CHAOS-3285 round 2 (Codex MEDIUM): the whole attempt runs
            # inside a SAVEPOINT (begin_nested), never bare. A DB error
            # caught by a plain except -- without a rollback or savepoint --
            # leaves the session's transaction unable to accept further
            # operations; the NEXT query on it raises PendingRollbackError,
            # and when the request's own session dependency later tries to
            # commit, THAT failure rolls back the whole transaction --
            # silently discarding the binary certify() write just above,
            # despite this being "caught". begin_nested()'s __aexit__ rolls
            # back only to the savepoint on an exception, leaving the outer
            # transaction (and the binary write already flushed into it)
            # intact and the session usable for the rest of this request.
            async with session.begin_nested():
                await RoleReadinessService(_platform_role_store(session)).certify_role(
                    AgentRole.LEGACY_AGENT,
                    resolution.provider,
                    certification_key=resolution.readiness_fingerprint,
                )
        except Exception:
            logger.warning(
                "Failed to certify the legacy_agent role during platform "
                "Ask Dev preflight",
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
                "Failed to close platform Ask Dev provider connection after preflight",
                exc_info=True,
            )
    return await _platform_readiness_response(session)
