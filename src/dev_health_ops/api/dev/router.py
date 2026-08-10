"""Authenticated, tenant-scoped REST surface for Ask Dev."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api._health import _analytics_db_url
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.queries.client import get_global_client
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.api.services.permissions import get_user_permissions
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing import evaluate_org_feature_async
from dev_health_ops.licensing.registry import ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE
from dev_health_ops.llm.agent.contracts import AgentUsage
from dev_health_ops.models.dev_persistence import DevMessage as PersistedDevMessage

from .contracts import (
    DevAnswer,
    DevCapabilities,
    DevConversation,
    DevConversationSummary,
    DevConversationTranscript,
    DevError,
    DevEvidenceExpansion,
    DevFeedback,
    DevMessageRequest,
    DevRunResumeRequest,
    DevScope,
    DevTimeRange,
    DevTranscriptEntry,
    dev_error_remediation,
)
from .entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from .graph_routing_policy import (
    CanonicalGraphRoutingEntitlementAuthorizer,
    GraphRoutingPolicyDeniedError,
)
from .no_match_terminal import redact_persisted_answer, redact_persisted_error
from .orchestrator import OrchestratorEvent, OrchestratorResult, RunState
from .orchestrator_persistence import PersistenceRunRecorder
from .org_policy import load_ask_dev_org_policy
from .persistence.service import (
    ConversationRecord,
    DevAdmissionLimits,
    DevConcurrencyLimitExceeded,
    DevMonthlyCostLimitExceeded,
    DevMonthlyRequestLimitExceeded,
    DevPersistenceConflict,
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
    DevPlatformAllowance,
    DevRateLimitExceeded,
    TranscriptRecord,
)
from .platform_auto_certification import schedule_platform_recertification
from .production_runtime import (
    build_production_runtime,
    expand_production_evidence,
    resolve_production_provider,
)
from .prompts import (
    MAX_PRIOR_CONTENT_BYTES,
    MAX_PRIOR_TURNS,
    PromptConversationTurn,
)
from .runtime import BoundedDevRuntime, DevRuntimeUnavailable
from .streaming import (
    encoded_persisted_sse_stream,
    encoded_sse_stream,
    validate_persisted_resume_events,
)


def _disable_shared_cache(response: Response) -> None:
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Pragma"] = "no-cache"


logger = logging.getLogger(__name__)

#: How long a non-terminal run must sit unchanged before a duplicate
#: ``client_message_id`` POST is allowed to force it terminal instead of
#: 409ing indefinitely (CHAOS-3297 Codex review round 5 HIGH closure).
#: Ask Dev turns are expected to complete in low tens of seconds end to
#: end; five minutes is comfortably longer than any run that is still
#: genuinely in flight could take without something else already having
#: failed it, while still being short enough that the double-failure
#: scenario this closes (the request's own fallback write ALSO failing)
#: recovers within a user's normal retry patience rather than needing a
#: background sweep to notice it.
_STALE_NON_TERMINAL_RUN_THRESHOLD = timedelta(minutes=5)
_TERMINAL_RUN_STATES = frozenset(
    {"completed", "insufficient_evidence", "refused", "failed", "cancelled"}
)


router = APIRouter(
    prefix="/api/v1/dev",
    tags=["dev"],
    dependencies=[Depends(_disable_shared_cache)],
)


class StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DevConversationCreateRequest(StrictRequestModel):
    current_scope: DevScope
    retention_days: Literal[0, 30] = 30
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)] | None = None


class DevConversationRenameRequest(StrictRequestModel):
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)] | None = None


class DevFeedbackCreateRequest(StrictRequestModel):
    """CHAOS-3660 §8(f)/(j). ``reasons`` mirrors ``contracts.DevFeedback.
    reasons`` -- request-body validation gate, not the wire response model
    itself -- so this list and ``persistence.service._FEEDBACK_REASONS``
    (the third, persistence-layer gate) must be widened in lockstep with
    the contract, or a client sending a newly-additive reason gets
    rejected here despite the contract declaring it valid.
    """

    rating: Literal["helpful", "not_helpful"]
    reasons: list[
        Literal[
            "incorrect",
            "missing_evidence",
            "wrong_scope",
            "stale_data",
            "unclear",
            "useful",
            "wrong_subject",
            "wrong_cohort",
            "wrong_driver",
            "unsafe_certainty",
            "other",
            "unspecified",
        ]
    ] = Field(min_length=1, max_length=12)
    comment: (
        Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None
    ) = None

    @field_validator("reasons")
    @classmethod
    def enforce_unspecified_exclusivity(cls, value: list[str]) -> list[str]:
        if "unspecified" in value and value != ["unspecified"]:
            raise ValueError(
                "'unspecified' must be the only reason when present, "
                "never combined with a specific reason"
            )
        return value


class DevConversationListResponse(StrictRequestModel):
    items: list[DevConversationSummary]
    next_cursor: str | None = None


@dataclass(frozen=True, slots=True)
class DevCapabilityRuntime:
    """Safe provider/readiness projection; construction remains server-owned."""

    effective_provider_label: str | None = None
    effective_model_label: str | None = None
    provider_source: Literal["platform", "byo"] | None = None
    readiness: Literal[
        "ready",
        "unsupported_model",
        "missing_credentials",
        "disabled",
        "degraded",
    ] = "missing_credentials"
    contextual_entrypoints: bool = False
    evidence_resolver: bool = True
    graph_routing_enabled: bool = False
    safe_failure_reason: str | None = "No certified Ask Dev model is ready."


@dataclass(frozen=True, slots=True)
class DevExecutionRuntimeResolution:
    runtime: BoundedDevRuntime | None
    error_code: str | None = None
    safe_message: str | None = None


async def _authenticated_user(
    authorization: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser:
    try:
        return await get_current_user(authorization)
    except HTTPException as exc:
        _raise(
            exc.status_code,
            "unauthenticated",
            "Authentication is required for Ask Dev.",
            request_id=x_request_id,
        )
        raise AssertionError("unreachable")


async def get_dev_capability_runtime(
    user: Annotated[AuthenticatedUser, Depends(_authenticated_user)],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
) -> DevCapabilityRuntime:
    """Project the certified provider policy without exposing configuration."""

    try:
        provider = await resolve_production_provider(session, org_id=user.org_id)
    except DevRuntimeUnavailable as exc:
        readiness: Literal["unsupported_model", "missing_credentials"] = (
            "unsupported_model"
            if exc.code == "model_not_supported"
            else "missing_credentials"
        )
        return DevCapabilityRuntime(
            readiness=readiness,
            safe_failure_reason=exc.safe_message,
        )
    except Exception:
        return DevCapabilityRuntime(
            readiness="degraded",
            safe_failure_reason="Ask Dev model readiness is temporarily unavailable.",
        )
    try:
        evidence_ready = bool(os.getenv("JWT_SECRET_KEY"))
        graph_routing_enabled = True
        try:
            await CanonicalGraphRoutingEntitlementAuthorizer(session).require(user.org_id)
        except GraphRoutingPolicyDeniedError:
            graph_routing_enabled = False
        return DevCapabilityRuntime(
            effective_provider_label=provider.provider_label,
            effective_model_label=provider.model_label,
            provider_source=provider.source.value,
            readiness="ready" if evidence_ready else "degraded",
            contextual_entrypoints=True,
            evidence_resolver=evidence_ready,
            graph_routing_enabled=graph_routing_enabled,
            safe_failure_reason=(
                None if evidence_ready else "Ask Dev evidence signing is unavailable."
            ),
        )
    finally:
        try:
            await provider.provider.aclose()
        except Exception:
            pass
        if provider.qua_shadow_provider is not None:
            try:
                await provider.qua_shadow_provider.aclose()
            except Exception:
                pass


async def get_dev_execution_runtime(
    user: Annotated[AuthenticatedUser, Depends(_authenticated_user)],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
) -> AsyncGenerator[DevExecutionRuntimeResolution, None]:
    """Build one request-local certified runtime and exact nine-tool registry."""

    runtime: BoundedDevRuntime | None = None
    try:
        runtime = await build_production_runtime(
            session,
            org_id=user.org_id,
            permission_fingerprint=_permission_fingerprint(user),
            clickhouse=await get_global_client(_analytics_db_url()),
        )
        resolution = DevExecutionRuntimeResolution(runtime=runtime)
    except DevRuntimeUnavailable as exc:
        resolution = DevExecutionRuntimeResolution(
            runtime=None,
            error_code=exc.code,
            safe_message=exc.safe_message,
        )
    except Exception:
        resolution = DevExecutionRuntimeResolution(
            runtime=None,
            error_code="provider_not_configured",
            safe_message="No certified Ask Dev model is ready.",
        )
    try:
        yield resolution
    finally:
        if runtime is not None:
            try:
                await runtime.aclose()
            except Exception:
                pass


class AskDevApiError(Exception):
    def __init__(self, status_code: int, error: DevError):
        self.status_code = status_code
        self.error = error
        super().__init__(error.code)


async def ask_dev_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AskDevApiError)
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.error.model_dump(mode="json", exclude_none=True),
        headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
    )


async def ask_dev_validation_error_handler(
    request: Request, exc: Exception
) -> Response:
    assert isinstance(exc, RequestValidationError)
    if not request.url.path.startswith("/api/v1/dev"):
        return await request_validation_exception_handler(request, exc)
    error = _error(
        request.headers.get("x-request-id"),
        "invalid_request",
        "The Ask Dev request is invalid.",
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=error.model_dump(mode="json", exclude_none=True),
        headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
    )


def _error(
    request_id: str | None,
    code: str,
    message: str,
    *,
    retryable: bool = False,
    limit_reset_at: datetime | None = None,
) -> DevError:
    safe_request_id = request_id or str(uuid.uuid4())
    try:
        return DevError(
            schema_version="dev_error.v1",
            request_id=safe_request_id,
            code=code,
            safe_message=message,
            retryable=retryable,
            limit_reset_at=limit_reset_at,
        )
    except ValueError:
        return DevError(
            schema_version="dev_error.v1",
            request_id=str(uuid.uuid4()),
            code=code,
            safe_message=message,
            retryable=retryable,
            limit_reset_at=limit_reset_at,
        )


def _raise(
    status_code: int,
    code: str,
    message: str,
    *,
    request_id: str | None = None,
    retryable: bool = False,
    limit_reset_at: datetime | None = None,
) -> None:
    raise AskDevApiError(
        status_code,
        _error(
            request_id,
            code,
            message,
            retryable=retryable,
            limit_reset_at=limit_reset_at,
        ),
    )


def _service(session: AsyncSession) -> DevPersistenceService:
    return DevPersistenceService(session)


async def _require_ask_dev(
    user: Annotated[AuthenticatedUser, Depends(_authenticated_user)],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    x_request_id: Annotated[str | None, Header()] = None,
) -> tuple[AuthenticatedUser, DevPersistenceService, str | None]:
    try:
        await CanonicalAskDevEntitlementAuthorizer(session).require(user.org_id)
    except AskDevEntitlementDeniedError:
        _raise(
            status.HTTP_403_FORBIDDEN,
            "feature_not_enabled",
            "Ask Dev is not enabled for this organization.",
            request_id=x_request_id,
        )
    policy = await load_ask_dev_org_policy(SettingsService(session, user.org_id))
    if policy.emergency_disabled:
        _raise(
            status.HTTP_403_FORBIDDEN,
            "feature_not_enabled",
            "Ask Dev is disabled by an organization administrator.",
            request_id=x_request_id,
        )
    return user, _service(session), x_request_id


async def _allow_ask_dev_cleanup(
    user: Annotated[AuthenticatedUser, Depends(_authenticated_user)],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    x_request_id: Annotated[str | None, Header()] = None,
) -> tuple[AuthenticatedUser, DevPersistenceService, str | None]:
    """Keep user-owned deletion available after an entitlement or disable change."""

    return user, _service(session), x_request_id


def _owned_ids(
    user: AuthenticatedUser, request_id: str | None
) -> tuple[uuid.UUID, uuid.UUID]:
    try:
        return uuid.UUID(user.org_id), uuid.UUID(user.user_id)
    except ValueError:
        _raise(
            status.HTTP_401_UNAUTHORIZED,
            "unauthenticated",
            "The authenticated Ask Dev identity is invalid.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")


def _parse_uuid(value: str | uuid.UUID, request_id: str | None) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(value)
    except ValueError:
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev identifier is invalid.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")


def _raise_persistence(exc: Exception, request_id: str | None) -> None:
    if isinstance(exc, DevMonthlyCostLimitExceeded):
        _raise(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "cost_limit_reached",
            "The Ask Dev platform cost allowance was reached.",
            request_id=request_id,
            limit_reset_at=exc.reset_at,
        )
    if isinstance(exc, DevMonthlyRequestLimitExceeded):
        _raise(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "rate_limited",
            "The Ask Dev platform request allowance was reached.",
            request_id=request_id,
            limit_reset_at=exc.reset_at,
        )
    if isinstance(exc, DevRateLimitExceeded):
        _raise(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "rate_limited",
            "The Ask Dev request rate limit was reached.",
            request_id=request_id,
            retryable=True,
        )
    if isinstance(exc, DevConcurrencyLimitExceeded):
        _raise(
            status.HTTP_409_CONFLICT,
            "concurrency_limited",
            "Another Ask Dev request is already running.",
            request_id=request_id,
            retryable=True,
        )
    if isinstance(exc, DevPersistenceNotFound):
        _raise(
            status.HTTP_404_NOT_FOUND,
            "conversation_not_found",
            "The Ask Dev resource was not found.",
            request_id=request_id,
        )
    if isinstance(exc, DevPersistenceValidationError):
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev request is invalid.",
            request_id=request_id,
        )
    if isinstance(exc, DevPersistenceConflict):
        _raise(
            status.HTTP_409_CONFLICT,
            "invalid_request",
            "The Ask Dev request conflicts with existing state.",
            request_id=request_id,
        )
    _raise(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        "internal_error",
        "Ask Dev data is temporarily unavailable.",
        request_id=request_id,
        retryable=True,
    )


def _conversation_model(record: ConversationRecord) -> DevConversation:
    scope = DevScope.model_validate(record.conversation.current_scope)
    return DevConversation(
        schema_version="dev_conversation.v1",
        conversation_id=str(record.conversation.id),
        title=record.conversation.title,
        current_scope=scope,
        retention_days=record.conversation.retention_days,
        state="active",
        message_count=record.message_count,
        latest_answer_id=(
            str(record.latest_answer_id)
            if record.latest_answer_id is not None
            else None
        ),
        created_at=_aware(record.conversation.created_at),
        updated_at=_aware(record.conversation.updated_at),
        expires_at=_aware(record.conversation.expires_at),
    )


def _summary_model(record: ConversationRecord) -> DevConversationSummary:
    scope = DevScope.model_validate(record.conversation.current_scope)
    return DevConversationSummary(
        schema_version="dev_conversation_summary.v1",
        conversation_id=str(record.conversation.id),
        title=record.conversation.title,
        direct_scope=scope.direct_scope,
        state="active",
        message_count=record.message_count,
        updated_at=_aware(record.conversation.updated_at),
        expires_at=_aware(record.conversation.expires_at),
    )


def _encode_cursor(record: ConversationRecord) -> str:
    payload = json.dumps(
        {
            "updated_at": _aware_required(record.conversation.updated_at).isoformat(),
            "id": str(record.conversation.id),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _encode_transcript_cursor(record: TranscriptRecord) -> str:
    payload = json.dumps(
        {
            "created_at": _aware_required(record.message.created_at).isoformat(),
            "id": str(record.message.id),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _aware_required(value: datetime) -> datetime:
    return _aware(value) or value.replace(tzinfo=UTC)


def _storage_uuid(value: str, *scope: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        return uuid.uuid5(uuid.NAMESPACE_URL, "\0".join((*scope, value)))


def _permission_fingerprint(user: AuthenticatedUser) -> str:
    payload = json.dumps(
        {
            "org_id": user.org_id,
            "user_id": user.user_id,
            "role": user.role,
            "token_version": user.token_version,
            "impersonated_by": user.impersonated_by,
            "permissions": sorted(get_user_permissions(user)),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _replay_fallback_error(run: Any) -> DevError:
    """The generic "did not complete" shape for a run with nothing to replay.

    Used both when no answer/frame was ever persisted for a terminal run,
    and (CHAOS-3297 Codex review MEDIUM) when a persisted v2 frame payload
    fails to validate -- a corrupted or legacy-shaped row must degrade to
    this safe public shape rather than 500 every future replay of the same
    idempotency key.
    """

    code = run.safe_error_code or "internal_error"
    try:
        return DevError(
            schema_version="dev_error.v1",
            request_id=str(run.request_id),
            code=code,
            safe_message=("The prior Ask Dev request did not complete with an answer."),
            retryable=code in {"provider_unavailable", "source_unavailable"},
            # A replayed run must carry the same corrective guidance a
            # live failure with this code would (CHAOS-3254) -- never
            # silently drop remediation just because the client is
            # reading back an idempotent replay instead of a fresh run.
            remediation=dev_error_remediation(code),
        )
    except ValueError:
        return DevError(
            schema_version="dev_error.v1",
            request_id=str(run.request_id),
            code="internal_error",
            safe_message=("The prior Ask Dev request did not complete with an answer."),
            retryable=True,
        )


def _replayed_result(
    *,
    run: Any,
    answer_payload: dict[str, Any] | None,
    frame_payload: dict[str, Any] | None = None,
    organization_id: str | None = None,
    time_range: DevTimeRange | None = None,
) -> OrchestratorResult:
    state = RunState(run.state)
    answer = None
    error = None
    terminal_error_payload = getattr(run, "terminal_error_payload", None)
    if answer_payload is not None:
        # CHAOS-3367: orchestrator.finish() is a WRITE-time boundary and
        # cannot reach rows written before it existed -- including the run
        # from the reported live screenshot, which is already stored and
        # still schema-valid. Replaying it verbatim would keep rendering the
        # leaked token on every reload, so the same rule is applied on the
        # way out (codex adversarial review, round 1 HIGH).
        answer = redact_persisted_answer(DevAnswer.model_validate(answer_payload))
    elif terminal_error_payload is not None:
        # CHAOS-3297 Codex review HIGH #1: the exact validated v1 DevError
        # `PersistenceRunRecorder.terminal` persisted at terminal time, for
        # *every* origin (the orchestrator's own error() closure,
        # _provider_error, or a preflight termination's
        # project_preflight_error) -- replayed verbatim. This supersedes the
        # frame-reconstruction branch below: reconstructing from
        # `compat._ERROR_OUTCOME_CODES`'s fixed, outcome-keyed table is only
        # ever an *approximation* of the live copy (correct code, but a
        # generic canonical safe_message/remediation that can differ from
        # the producer-authored text a live run actually streamed -- e.g.
        # "scope_not_found" reconstructs the same *code* on both sides but a
        # different *message*, which the old code-only coherence guard could
        # not catch). A row that has this column is authoritative on its
        # own; it never needs the frame at all.
        try:
            # Redacted on the same read boundary and for the same reason as
            # the answer above (CHAOS-3367). `code` is untouched: it is a
            # machine field clients switch on, not copy.
            error = redact_persisted_error(
                DevError.model_validate(terminal_error_payload)
            )
        except ValidationError:
            error = _replay_fallback_error(run)
    elif (
        frame_payload is not None
        and organization_id is not None
        and time_range is not None
    ):
        # Compatibility fallback for a run persisted before 0079 added
        # `terminal_error_payload` -- no such row has ever run through the
        # branch above, so this reconstruction (with its documented
        # code-only-fidelity caveat) is the best available replay for it.
        # CHAOS-3299 / TRD v2 Section 12: a v2 run with no answer message
        # (needs_clarification/not_found/temporarily_unavailable/unsupported/
        # denied/failed -- no assistant DevMessage is ever recorded for these)
        # renders from the stored frame rather than degrading to a generic
        # "did not complete" error. Every outcome reachable in this branch is
        # a no-answer outcome by construction (an answer_id would have taken
        # the branch above instead), so the projection is always a DevError
        # -- and it must be the *same* DevError a live run would have
        # streamed. ``preflight_outcomes.project_preflight_error`` is that
        # terminal projection (CHAOS-3292's ratified design): it special-
        # cases ``needs_clarification`` to the ``scope_ambiguous`` shape the
        # router and web client already handle, rather than the generic
        # compat projector's fabricated-candidate ``DevAnswer`` (which is a
        # different response shape from what the live run actually sent).
        # For the other four outcomes it delegates to the same one backend
        # v2-to-v1 projector (CHAOS-3294 guardrail) live preflight uses, so
        # this is never a second, divergent mapping.
        from .contracts_v2.answer import _OUTCOME_DISPLAY_LABELS, DevAnswerV2
        from .contracts_v2.base import PublicOutcome
        from .contracts_v2.frame import DevAnswerFrame as _DevAnswerFrameV2
        from .contracts_v2.no_answer_policy import NO_ANSWER_OUTCOMES
        from .preflight_outcomes import project_preflight_error
        from .terminal_frames import (
            is_orchestrator_error_frame,
            tolerant_parse_legacy_frame_payload,
        )

        # project_preflight_error is only total over the no-answer outcomes
        # plus needs_clarification (CHAOS-3292's ratified vocabulary for a
        # preflight termination) -- ANSWERED/ANSWERED_WITH_GAPS raise
        # RuntimeError there by design, since a preflight never terminates
        # with an answer. A row reachable through this branch (no
        # answer_payload) is only trustworthy if the frame agrees: it must
        # both carry one of those outcomes *and* match the run's own
        # `public_outcome` column, which is written from the same frame at
        # persist time (CHAOS-3297 Codex review MEDIUM #2). A corrupted or
        # schema-skewed row that still happens to validate must not project
        # false public semantics or crash the replay -- degrade to the same
        # safe fallback used for a missing frame.
        _REPLAYABLE_PREFLIGHT_OUTCOMES = NO_ANSWER_OUTCOMES | {
            PublicOutcome.NEEDS_CLARIFICATION.value
        }
        try:
            frame_obj = _DevAnswerFrameV2.model_validate(
                # CHAOS-3297 s3 version-skew read posture: a row persisted
                # before this branch's DevMetricRefV2.evidence_classification
                # field existed has no way to satisfy F10's XOR check without
                # this shim -- see terminal_frames.tolerant_parse_legacy_
                # frame_payload's own docstring. This branch never carries a
                # non-empty metrics tuple today (NO_ANSWER_FRAME_FIELD_POLICY
                # forces it empty for every outcome reachable here), so this
                # is defensive hardening ahead of a future read path, not a
                # fix for an observed crash on this one.
                tolerant_parse_legacy_frame_payload(frame_payload)
            )
            if (
                frame_obj.public_outcome.value not in _REPLAYABLE_PREFLIGHT_OUTCOMES
                or run.public_outcome != frame_obj.public_outcome.value
            ):
                error = _replay_fallback_error(run)
            else:
                answer_v2 = DevAnswerV2(
                    schema_version="dev_answer.v2",
                    answer_id=str(run.id),
                    conversation_id=str(run.conversation_id),
                    run_id=str(run.id),
                    # SQLite (test fixtures only) does not round-trip
                    # tz-aware datetimes through DateTime(timezone=True);
                    # PostgreSQL does, so this was unobserved until
                    # CHAOS-3299 Codex finding 1's fix made this branch
                    # reachable for the first time.
                    generated_at=_aware_required(run.ended_at or run.started_at),
                    public_outcome=frame_obj.public_outcome,
                    outcome_display_label=_OUTCOME_DISPLAY_LABELS[
                        frame_obj.public_outcome
                    ],
                    frame=frame_obj,
                    narrative=None,
                )
                error = project_preflight_error(
                    answer_v2, request_id=str(run.request_id)
                )
                # CHAOS-3297: `project_preflight_error` reconstructs a v1
                # code from a *fixed*, outcome-keyed table
                # (`compat._ERROR_OUTCOME_CODES` / the `scope_ambiguous`
                # special case) -- correct for a genuinely preflight-sourced
                # frame, but orchestrator.run()'s own ~30 non-preflight
                # terminal() calls (CHAOS-3297 stack #1) now also persist a
                # frame for replay-gate reachability, carrying today's
                # richer v1 code taxonomy verbatim in `run.safe_error_code`
                # (e.g. "cancelled", "tool_limit_reached", "scope_forbidden").
                # Trusting the frame reconstruction there would silently
                # rewrite the replayed code away from what live actually
                # streamed (a v1 wire-vocabulary change on retry). Whichever
                # origin produced this frame, `run.safe_error_code` is always
                # what `terminal()` persisted from the exact live error
                # object -- so if the frame's fixed reconstruction disagrees
                # with it, the frame is not authoritative for this run and
                # the exact-fidelity fallback below wins instead.
                #
                # CHAOS-3297 stack #5 -- reconciling that rule with
                # frames-authoritative semantics. Two clauses, deliberately
                # separate so a mutation can defeat either alone:
                #
                # * the ORIGIN clause. "Disagreeing codes" was only ever a
                #   proxy for "this frame did not author the live copy", and
                #   it is a coincidence-prone one: `scope_not_found`,
                #   `internal_error` and `feature_not_enabled` each project
                #   back to their own code, so the code check passed while
                #   the replayed *message* was canonical preflight copy the
                #   orchestrator's own call site never sent (live: "The
                #   requested scope was not found."; replayed: "No matching
                #   subject was found for this question."). An
                #   orchestrator-origin frame is now identified structurally
                #   -- its frame_id is a pure uuid5 over (run_id, code), see
                #   terminal_frames.is_orchestrator_error_frame -- and never
                #   speaks for the v1 error, regardless of whether the
                #   projected code happens to match.
                # * the CODE clause, kept as-is: a frame whose origin cannot
                #   be established but whose projection disagrees is still
                #   not authoritative.
                #
                # What this does NOT change: the frame remains the source of
                # truth for content wherever content is what is being read.
                # This branch is specifically the v1 *error* wire shape,
                # which stack #1 ruled the frame never owns.
                if run.safe_error_code and (
                    error.code != run.safe_error_code
                    or is_orchestrator_error_frame(
                        frame_id=frame_obj.frame_id,
                        run_id=str(run.id),
                        code=run.safe_error_code,
                    )
                ):
                    error = _replay_fallback_error(run)
        except ValidationError:
            error = _replay_fallback_error(run)
    else:
        error = _replay_fallback_error(run)
    return OrchestratorResult(
        run_id=str(run.id),
        state=state,
        answer=answer,
        error=error,
        events=(OrchestratorEvent(state, error.code if error else None),),
        usage=AgentUsage(
            input_tokens=run.input_tokens or 0,
            output_tokens=run.output_tokens or 0,
            estimated_cost_microusd=run.estimated_cost_microusd,
        ),
        tool_call_count=run.tool_call_count,
        provider_fingerprint=run.provider_fingerprint,
        model_fingerprint=run.model_fingerprint,
        # CHAOS-3497: a replay must stream the same frames the live run did.
        # ``streaming`` reads ``scope.resolved`` off this field now (so a
        # no-answer terminal can publish one at all), and leaving it None
        # here would silently DROP the frame from every replayed answer --
        # a regression introduced by the very change that closes the gap.
        #
        # Only the answer branch can supply it. A replayed no-answer run has
        # no persisted ``dev_scope_resolution.v1`` to read (the run row keeps
        # ``terminal_error_payload`` and the frame, neither of which carries
        # one), so it replays without the frame rather than with a fabricated
        # one -- an honest known gap, filed rather than papered over: a live
        # no-answer run publishes its resolution, its replay does not.
        scope_resolution=answer.resolved_scope if answer is not None else None,
    )


def _bounded_prompt_history(
    messages: Sequence[PersistedDevMessage],
) -> tuple[PromptConversationTurn, ...]:
    """Project only user questions and validated answer summaries into the prompt."""

    turns: list[PromptConversationTurn] = []
    for message in messages:
        if message.role == "user":
            content = message.content
        elif (
            isinstance(message.answer_payload, Mapping)
            and message.answer_payload.get("schema_version") == "dev_error.v1"
        ):
            # CHAOS-3423: a no-answer terminal's assistant row carries a
            # dev_error.v1 payload, not a DevAnswer -- validate and project
            # its safe_message the same defense-in-depth way the DevAnswer
            # branch below does, rather than trusting message.content alone.
            content = DevError.model_validate(message.answer_payload).safe_message
        else:
            content = DevAnswer.model_validate(message.answer_payload).direct_summary
        if content is None:
            raise DevPersistenceConflict("prompt history message has no safe content")
        turns.append(PromptConversationTurn(role=message.role, content=content))

    bounded = turns[-MAX_PRIOR_TURNS:]
    while bounded:
        payload = [{"role": turn.role, "content": turn.content} for turn in bounded]
        if (
            len(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            <= MAX_PRIOR_CONTENT_BYTES
        ):
            break
        bounded = bounded[1:]
    return tuple(bounded)


def _decode_cursor(
    cursor: str | None, request_id: str | None
) -> tuple[datetime | None, uuid.UUID | None]:
    if cursor is None:
        return None, None
    try:
        if len(cursor) > 512:
            raise ValueError
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
        if set(payload) != {"updated_at", "id"}:
            raise ValueError
        return datetime.fromisoformat(payload["updated_at"]), uuid.UUID(payload["id"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev cursor is invalid.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")


def _decode_transcript_cursor(
    cursor: str | None, request_id: str | None
) -> tuple[datetime | None, uuid.UUID | None]:
    if cursor is None:
        return None, None
    try:
        if len(cursor) > 512:
            raise ValueError
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
        if set(payload) != {"created_at", "id"}:
            raise ValueError
        created_at = datetime.fromisoformat(payload["created_at"])
        if created_at.tzinfo is None:
            raise ValueError
        return created_at, uuid.UUID(payload["id"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev cursor is invalid.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")


async def _feature_allowed(session: AsyncSession, org_id: uuid.UUID, key: str) -> bool:
    try:
        return (await evaluate_org_feature_async(session, org_id, key)).allowed
    except Exception:
        return False


@router.get("/capabilities", response_model=DevCapabilities)
async def capabilities(
    user: Annotated[AuthenticatedUser, Depends(_authenticated_user)],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    runtime: Annotated[DevCapabilityRuntime, Depends(get_dev_capability_runtime)],
    response: Response,
) -> DevCapabilities:
    build_sha = os.getenv("DEV_HEALTH_BUILD_SHA", "").strip()
    runtime_sha = build_sha if re.fullmatch(r"[0-9a-f]{40}", build_sha) else None
    if runtime_sha:
        response.headers["x-backend-sha"] = build_sha
    org_id, _ = _owned_ids(user, None)
    ask_dev = await _feature_allowed(session, org_id, "ask_dev")
    contextual_entrypoints = await _feature_allowed(
        session, org_id, ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE
    )
    byo_llm = await _feature_allowed(session, org_id, "byo_llm")
    agent_context_runtime = await _feature_allowed(
        session, org_id, "agent_context_runtime"
    )
    policy = await load_ask_dev_org_policy(SettingsService(session, user.org_id))
    effective_ask_dev = ask_dev and not policy.emergency_disabled
    readiness = runtime.readiness if effective_ask_dev else "disabled"
    safe_failure = (
        runtime.safe_failure_reason
        if effective_ask_dev
        else (
            "Ask Dev is disabled by an organization administrator."
            if ask_dev and policy.emergency_disabled
            else "Ask Dev is not enabled."
        )
    )
    return DevCapabilities(
        schema_version="dev_capabilities.v1",
        backend_sha=runtime_sha,
        ask_dev=effective_ask_dev,
        ask_dev_graph_routing=effective_ask_dev and runtime.graph_routing_enabled,
        byo_llm=byo_llm,
        agent_context_runtime=agent_context_runtime,
        can_read=effective_ask_dev and readiness == "ready",
        can_manage=effective_ask_dev and user.is_admin,
        effective_provider_label=runtime.effective_provider_label,
        effective_model_label=runtime.effective_model_label,
        provider_source=runtime.provider_source,
        readiness=readiness,
        supported_contract_versions=[
            "dev_capabilities.v1",
            "dev_conversation.v1",
            "dev_conversation_transcript.v1",
            "dev_message_request.v1",
            "dev_answer.v1",
            "dev_evidence_expansion.v1",
            "dev_stream_event.v1",
            "dev_error.v1",
        ],
        contextual_entrypoints=(
            effective_ask_dev
            and contextual_entrypoints
            and runtime.contextual_entrypoints
        ),
        evidence_resolver=effective_ask_dev and runtime.evidence_resolver,
        administrator_safe_failure_reason=safe_failure,
    )


@router.get("/conversations", response_model=DevConversationListResponse)
async def list_conversations(
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> DevConversationListResponse:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    before, before_id = _decode_cursor(cursor, request_id)
    try:
        records = await service.list_conversation_records(
            org_id=org_id,
            user_id=user_id,
            limit=limit,
            before=before,
            before_id=before_id,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    next_cursor = _encode_cursor(records[-1]) if len(records) == limit else None
    return DevConversationListResponse(
        items=[_summary_model(record) for record in records],
        next_cursor=next_cursor,
    )


@router.post(
    "/conversations",
    response_model=DevConversation,
    status_code=status.HTTP_201_CREATED,
)
async def create_conversation(
    body: DevConversationCreateRequest,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> DevConversation:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    policy = await load_ask_dev_org_policy(
        SettingsService(service.session, user.org_id)
    )
    if body.current_scope.organization_id != user.org_id:
        _raise(
            status.HTTP_403_FORBIDDEN,
            "scope_forbidden",
            "The Ask Dev scope is not authorized.",
            request_id=request_id,
        )
    try:
        conversation = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope=body.current_scope.model_dump(mode="json"),
            # Organization policy owns retention for the app-shell window and
            # full-page workspace. The legacy request field remains accepted
            # for wire compatibility but cannot override that policy.
            retention_days=policy.retention_days,
            title=body.title,
        )
        record = await service.get_conversation_record(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    return _conversation_model(record)


@router.get("/conversations/{conversation_id}", response_model=DevConversation)
async def get_conversation(
    conversation_id: uuid.UUID,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> DevConversation:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    try:
        record = await service.get_conversation_record(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    return _conversation_model(record)


@router.get(
    "/conversations/{conversation_id}/transcript",
    response_model=DevConversationTranscript,
)
async def get_conversation_transcript(
    conversation_id: uuid.UUID,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> DevConversationTranscript:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    after, after_id = _decode_transcript_cursor(cursor, request_id)
    try:
        page = await service.list_transcript_records(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            limit=limit,
            after=after,
            after_id=after_id,
        )
        entries: list[DevTranscriptEntry] = []
        for record in page.records:
            message = record.message
            run = record.run
            common = {
                "schema_version": "dev_transcript_entry.v1",
                "message_id": str(message.id),
                "created_at": _aware_required(message.created_at),
                "run_id": str(run.id),
                "retry_of_run_id": (
                    str(run.retry_of_run_id)
                    if run.retry_of_run_id is not None
                    else None
                ),
                "run_state": run.state,
            }
            if message.role == "user":
                entries.append(
                    DevTranscriptEntry(
                        **common,
                        role="user",
                        question=message.content,
                        scope=DevScope.model_validate(message.scope_snapshot),
                    )
                )
            else:
                # CHAOS-3423/CHAOS-3440: a no-answer terminal's assistant row
                # (record_error_message) is never returned here --
                # list_transcript_records defaults include_errors=False for
                # exactly this wire-facing read, because the checked-in
                # dev-health-web client runtime-validates every response
                # against the pinned v1 schema (closed-world: unknown keys
                # rejected) and its own hand-written invariant requires
                # every assistant entry to carry a real `answer`
                # (jsonSchemaValidation.ts, contractValidation.ts). Every
                # `message` reaching this branch is therefore still a real
                # DevAnswer, exactly as before CHAOS-3423 -- surfacing a
                # no-answer turn on this wire is CHAOS-3440, gated on a
                # coordinated client update.
                entries.append(
                    DevTranscriptEntry(
                        **common,
                        role="assistant",
                        # Same read boundary as `_replayed_result` above: a
                        # transcript read hands stored model prose straight to
                        # the client (CHAOS-3367).
                        answer=redact_persisted_answer(
                            DevAnswer.model_validate(message.answer_payload)
                        ),
                    )
                )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    return DevConversationTranscript(
        schema_version="dev_conversation_transcript.v1",
        conversation_id=str(conversation_id),
        items=entries,
        next_cursor=(
            _encode_transcript_cursor(page.records[-1])
            if page.has_more and page.records
            else None
        ),
    )


@router.patch("/conversations/{conversation_id}", response_model=DevConversation)
async def rename_conversation(
    conversation_id: uuid.UUID,
    body: DevConversationRenameRequest,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> DevConversation:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    try:
        conversation = await service.rename_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            title=body.title,
        )
        record = await service.get_conversation_record(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    return _conversation_model(record)


@router.delete(
    "/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_conversation(
    conversation_id: uuid.UUID,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_allow_ask_dev_cleanup),
    ],
) -> Response:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    try:
        deleted = await service.delete_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    if not deleted:
        _raise(
            status.HTTP_404_NOT_FOUND,
            "conversation_not_found",
            "The Ask Dev resource was not found.",
            request_id=request_id,
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/runs/{run_id}/resume")
async def resume_run(
    run_id: uuid.UUID,
    body: DevRunResumeRequest,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> StreamingResponse:
    """Rejoin one owned run by replaying its durable SSE event ledger."""

    user, service, header_request_id = auth
    request_id = body.request_id or header_request_id
    org_id, user_id = _owned_ids(user, request_id)
    try:
        run, persisted_scope = await service.get_run_resume_metadata(
            org_id=org_id, user_id=user_id, run_id=run_id
        )
        if run.conversation_id != _parse_uuid(body.conversation_id, request_id):
            _raise(
                status.HTTP_409_CONFLICT,
                "resume_scope_mismatch",
                "The resume cursor does not belong to this conversation.",
                request_id=request_id,
            )
        if persisted_scope != body.scope.model_dump(mode="json"):
            _raise(
                status.HTTP_409_CONFLICT,
                "resume_scope_mismatch",
                "The resume scope does not match the accepted run.",
                request_id=request_id,
            )
        events = await service.list_stream_events(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            after_sequence=body.last_sequence,
        )
        if run.state not in _TERMINAL_RUN_STATES and not events:
            _raise(
                status.HTTP_409_CONFLICT,
                "resume_unavailable",
                "The live run has no durable event after this cursor.",
                request_id=request_id,
                retryable=True,
            )
        payloads = [row.event_data for row in events]
        validate_persisted_resume_events(
            run_id=str(run_id),
            after_sequence=body.last_sequence,
            persisted_events=payloads,
        )
    except (AskDevApiError, HTTPException):
        raise
    except ValueError as exc:
        await service.session.rollback()
        _raise(
            status.HTTP_409_CONFLICT,
            "resume_stream_invalid",
            "The persisted Ask Dev stream cannot be resumed safely.",
            request_id=request_id,
        )
        raise AssertionError("unreachable") from exc
    except Exception as exc:
        await service.session.rollback()
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")

    return StreamingResponse(
        encoded_persisted_sse_stream(
            run_id=str(run_id),
            after_sequence=body.last_sequence,
            persisted_events=payloads,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
    )


@router.post("/conversations/{conversation_id}/messages")
async def create_message(
    conversation_id: uuid.UUID,
    body: DevMessageRequest,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
    runtime_resolution: Annotated[
        DevExecutionRuntimeResolution, Depends(get_dev_execution_runtime)
    ],
) -> StreamingResponse:
    user, service, header_request_id = auth
    request_id = body.request_id or header_request_id
    org_id, user_id = _owned_ids(user, request_id)
    if body.conversation_id is not None and body.conversation_id != str(
        conversation_id
    ):
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev conversation identifier does not match the request path.",
            request_id=request_id,
        )
    if body.scope.organization_id != user.org_id:
        _raise(
            status.HTTP_403_FORBIDDEN,
            "scope_forbidden",
            "The Ask Dev scope is not authorized.",
            request_id=request_id,
        )

    storage_client_message_id = _storage_uuid(
        body.client_message_id,
        user.org_id,
        user.user_id,
        str(conversation_id),
        "client_message",
    )
    storage_request_id = _storage_uuid(
        body.request_id,
        user.org_id,
        user.user_id,
        str(conversation_id),
        "request",
    )
    prior_turns: tuple[PromptConversationTurn, ...] = ()
    try:
        runtime = runtime_resolution.runtime
        policy = await load_ask_dev_org_policy(
            SettingsService(service.session, user.org_id)
        )
        provider_source = runtime.provider_source if runtime is not None else None
        # CHAOS-3358: heal a stale platform certification automatically, so an
        # operator never has to press the preflight button to recover from a
        # READINESS_VERSION bump or a fingerprint-format change.
        #
        # This is the ONLY place that triggers it, and deliberately so. It sits
        # after _require_ask_dev (entitlement AND emergency-disable both
        # checked) and behind provider_source == "platform", so a run that
        # selected BYO, a disabled or non-entitled organization, and the
        # capabilities projection -- which resolves a provider before any Ask
        # Dev authorization runs -- can never spend operator provider calls.
        # Codex CHAOS-3358 review CONFIRMED that reachable path when this was
        # scheduled from provider resolution instead.
        if (
            runtime is not None
            and provider_source == "platform"
            and runtime.platform_certification_stale
        ):
            schedule_platform_recertification()
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=storage_client_message_id,
            question=body.question,
            scope_snapshot=body.scope.model_dump(mode="json"),
            request_id=storage_request_id,
            retry_of_run_id=(
                _parse_uuid(body.retry_of_run_id, request_id)
                if body.retry_of_run_id is not None
                else None
            ),
            admission_limits=DevAdmissionLimits(),
            provider_source=provider_source,
            platform_allowance=(
                DevPlatformAllowance(
                    monthly_request_limit=policy.platform_monthly_request_limit,
                    monthly_cost_limit_microusd=(
                        policy.platform_monthly_cost_limit_microusd
                    ),
                )
                if provider_source == "platform"
                else None
            ),
        )
        if accepted.created:
            history = await service.list_prompt_history_messages(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                exclude_message_id=accepted.message.id,
                limit=MAX_PRIOR_TURNS,
            )
            prior_turns = _bounded_prompt_history(history)
        accepted_run_id = accepted.run.id
        await service.session.commit()
    except Exception as exc:
        await service.session.rollback()
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")

    cancellation = asyncio.Event()
    run_id = str(accepted_run_id)
    pending_events: list[Mapping[str, Any]] = []
    use_independent_event_session = (
        service.session.bind is not None
        and service.session.bind.dialect.name != "sqlite"
    )

    async def persist_event(event: Mapping[str, Any]) -> None:
        # Use an independent session: the orchestrator owns the request
        # session and may flush concurrently while this public event is
        # emitted. Per-emission commit makes reconnect durable even when the
        # client closes the SSE generator before terminal completion.
        if not use_independent_event_session:
            pending_events.append(event)
            return
        try:
            async with get_postgres_session() as event_session:
                event_service = DevPersistenceService(event_session)
                await event_service.record_stream_event(
                    org_id=org_id,
                    user_id=user_id,
                    run_id=accepted_run_id,
                    event=event,
                )
                await event_session.commit()
        except DevPersistenceNotFound:
            # A cleanup sweep can cascade-delete a zombie run while its
            # response is still draining. The run is already gone, so there
            # is no durable ledger row to append and persistence must not
            # replace the orchestrator's safe terminal response.
            return

    if not accepted.created:
        replay_run = accepted.run
        if replay_run.state not in {
            RunState.COMPLETED.value,
            RunState.INSUFFICIENT_EVIDENCE.value,
            RunState.REFUSED.value,
            RunState.FAILED.value,
            RunState.CANCELLED.value,
        }:
            # CHAOS-3297 Codex review round 5 HIGH: recovery at the point
            # of manifestation, not a background job. Ordinarily a
            # non-terminal run here just means the original request is
            # genuinely still running -- 409 is correct, and
            # recover_stale_non_terminal_run leaves any run younger than
            # the threshold untouched, returning None so this still 409s.
            # But if run_with_events's own except-block fallback
            # (force_terminal_fallback) ALSO failed on the same DB
            # incident that broke the original attempt, this run would
            # otherwise stay non-terminal forever and every future replay
            # of this client_message_id would 409 indefinitely. Recover
            # it here instead, once it is old enough that it cannot
            # possibly still be a genuinely in-flight request.
            recovered = await service.recover_stale_non_terminal_run(
                org_id=org_id,
                user_id=user_id,
                run_id=replay_run.id,
                stale_after=_STALE_NON_TERMINAL_RUN_THRESHOLD,
            )
            if recovered is None:
                _raise(
                    status.HTTP_409_CONFLICT,
                    "concurrency_limited",
                    "The matching Ask Dev request is still running.",
                    request_id=request_id,
                    retryable=True,
                )
                raise AssertionError("unreachable")
            replay_run = recovered
        answer_payload = None
        frame_payload = None
        if replay_run.answer_id is not None:
            try:
                answer_message = await service.get_answer_message(
                    org_id=org_id,
                    user_id=user_id,
                    answer_id=replay_run.answer_id,
                )
                answer_payload = answer_message.answer_payload
            except Exception as exc:
                _raise_persistence(exc, request_id)
                raise AssertionError("unreachable")
        elif replay_run.contract_generation == "v2":
            frame = await service.get_answer_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=replay_run.id,
            )
            if frame is not None:
                frame_payload = frame.payload
        replayed = _replayed_result(
            run=replay_run,
            answer_payload=answer_payload,
            frame_payload=frame_payload,
            organization_id=str(org_id),
            time_range=body.scope.time_range,
        )

        async def replay(_sink) -> OrchestratorResult:
            return replayed

        return StreamingResponse(
            encoded_sse_stream(
                run_id=run_id,
                run_with_events=replay,
                cancellation=cancellation,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
        )

    runtime = runtime_resolution.runtime
    if runtime is None:
        # CHAOS-3423 Codex adversarial review round 3 (MEDIUM, confirmed):
        # this is a real, reachable no-answer terminal (a misconfigured or
        # currently-unavailable provider) that never reaches
        # orchestrator.finish() at all -- it short-circuits here, before an
        # orchestrator run is even constructed. Persisted the same way
        # every other no-answer terminal now is, through the same
        # record_error_message helper, so the transcript-completeness
        # invariant CHAOS-3423 exists to guarantee actually holds here too
        # (and terminal_error_payload is set so a duplicate request replays
        # this exact copy, not the generic fallback).
        error_code = runtime_resolution.error_code or "provider_not_configured"
        safe_message = (
            runtime_resolution.safe_message or "No certified Ask Dev model is ready."
        )
        # CHAOS-3423 Codex adversarial review round 4 (MEDIUM, confirmed): a
        # SINGLE DevError, reused for the persisted transcript row AND the
        # immediate HTTP response below -- the two used to disagree on
        # `retryable` (this call site's own pre-CHAOS-3423 `_raise(...)`
        # never passed one, i.e. `False`; the newly-added persisted row
        # hard-coded `True`), which would make a duplicate-request replay
        # tell the client something different from what the live response
        # already told it.
        error = DevError(
            schema_version="dev_error.v1",
            request_id=request_id or str(accepted.run.request_id),
            code=error_code,
            safe_message=safe_message,
            retryable=False,
        )
        recorder = PersistenceRunRecorder(
            service,
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            run_id=accepted_run_id,
            # Never read by record_error_message (it only persists the
            # transcript row) -- this run has no resolved provider at all,
            # so there is no real "platform"/"byo" value to report.
            provider_source="platform",
        )
        try:
            await recorder.record_error_message(
                error, scope_snapshot=body.scope.model_dump(mode="json")
            )
        except Exception:
            # Best-effort, exactly like orchestrator.finish()'s own
            # error_message_write_fault handling: never let a transcript-row
            # write failure block marking the run terminal below.
            logger.exception(
                "ask_dev.router.provider_unavailable_error_message_write_fault",
                extra={"run_id": run_id},
            )
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted_run_id,
            state=RunState.FAILED.value,
            safe_error_code=error_code,
            terminal_error_payload=error.model_dump(mode="json"),
        )
        await service.session.commit()
        _raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code,
            safe_message,
            request_id=request_id,
        )
        raise AssertionError("unreachable")

    answer_id = str(uuid.uuid4())
    recorder = PersistenceRunRecorder(
        service,
        org_id=org_id,
        user_id=user_id,
        conversation_id=conversation_id,
        run_id=accepted_run_id,
        provider_source=runtime.provider_source,
    )

    async def run_with_events(event_sink) -> OrchestratorResult:
        try:
            return await runtime.run(
                request=body,
                org_id=user.org_id,
                user_id=user.user_id,
                permission_fingerprint=_permission_fingerprint(user),
                run_id=run_id,
                conversation_id=str(conversation_id),
                answer_id=answer_id,
                cancellation=cancellation,
                recorder=recorder,
                event_sink=event_sink,
                prior_turns=prior_turns,
            )
        except Exception:
            # CHAOS-3297 Codex review round 3 Finding 2: finish()
            # (orchestrator.py) can flush an answer and/or a frame on this
            # request's session, then have its own terminal write fail for
            # a reason unrelated to input validity. streaming.
            # stream_orchestrator catches this exception and turns it into
            # a generic internal_error SSE event without re-raising to
            # this request's session-dependency teardown, which then
            # commits whatever was already flushed: artifacts and a v2 tag
            # on a run stuck non-terminal forever.
            #
            # Roll back this (possibly poisoned) session's own pending,
            # uncommitted writes FIRST -- a fresh session's fallback write
            # to the *same* dev_runs row would otherwise deadlock waiting
            # for a lock this session's still-open transaction holds
            # (nothing will ever commit it now that we are handling its
            # failure). Any artifact that was only flushed, never
            # committed, is discarded by this rollback -- acceptable,
            # since a flush alone was never a durability guarantee.
            # Anything committed by an *earlier*, already-completed
            # transaction on this session is unaffected.
            try:
                await service.session.rollback()
            except Exception:
                # Best-effort only: this session is about to be abandoned
                # regardless (the fallback below always uses a fresh
                # session/connection, never this one), so a rollback
                # failure here -- the session already unusable, the
                # connection already dropped -- changes nothing about
                # what happens next. Swallowing it, not re-raising, is
                # what lets the fallback attempt still run.
                pass
            # Force the run terminal from a FRESH session/connection --
            # never the session just rolled back above -- so no run can
            # end up stuck non-terminal after this failure.
            #
            # Bounded retry (CHAOS-3297 Codex review round 5 HIGH): one
            # extra attempt, since a single transient failure here (the
            # same connection blip that broke the original write, a
            # momentary pool exhaustion) should not be allowed to strand
            # the run non-terminal when a second attempt on a distinct
            # connection would likely succeed. Not a queue-and-backoff
            # mechanism -- if both attempts fail (the same DB incident
            # taking out the fallback too), that failure is logged and
            # swallowed here rather than replacing the original
            # exception; recover_stale_non_terminal_run on the replay
            # path is the durable backstop for that case, not this loop.
            fallback_exc: Exception | None = None
            for attempt in range(2):
                try:
                    async with get_postgres_session() as fallback_session:
                        await DevPersistenceService(
                            fallback_session
                        ).force_terminal_fallback(
                            org_id=org_id, user_id=user_id, run_id=accepted_run_id
                        )
                    fallback_exc = None
                    break
                except Exception as exc:
                    fallback_exc = exc
            if fallback_exc is not None:
                logger.error(
                    "ask_dev.force_terminal_fallback_failed",
                    extra={
                        "run_id": run_id,
                        "org_id": str(org_id),
                        "attempts": 2,
                    },
                    exc_info=fallback_exc,
                )
            raise

    async def chunks() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in encoded_sse_stream(
                run_id=run_id,
                run_with_events=run_with_events,
                cancellation=cancellation,
                persist_event=persist_event,
            ):
                yield chunk
        finally:
            if pending_events:
                for event in pending_events:
                    try:
                        await service.record_stream_event(
                            org_id=org_id,
                            user_id=user_id,
                            run_id=accepted_run_id,
                            event=event,
                        )
                    except DevPersistenceNotFound:
                        break
                await service.session.commit()

    return StreamingResponse(
        chunks(),
        media_type="text/event-stream",
        headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
    )


@router.get("/evidence/{evidence_ref_id}", response_model=DevEvidenceExpansion)
async def expand_evidence(
    evidence_ref_id: str,
    answer_id: Annotated[uuid.UUID, Query()],
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> DevEvidenceExpansion:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    if not evidence_ref_id or len(evidence_ref_id.encode()) > 128:
        _raise(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "invalid_request",
            "The Ask Dev evidence identifier is invalid.",
            request_id=request_id,
        )
    try:
        answer_message = await service.get_answer_message(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
        )
        answer = DevAnswer.model_validate(answer_message.answer_payload)
    except DevPersistenceNotFound:
        _raise(
            status.HTTP_404_NOT_FOUND,
            "scope_not_found",
            "The Ask Dev evidence was not found.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    evidence = next(
        (item for item in answer.evidence if item.evidence_ref_id == evidence_ref_id),
        None,
    )
    if evidence is None:
        _raise(
            status.HTTP_404_NOT_FOUND,
            "scope_not_found",
            "The Ask Dev evidence was not found.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")
    scope = (
        answer.resolved_scope.resolved_scope or answer.resolved_scope.requested_scope
    )
    try:
        result = await expand_production_evidence(
            service.session,
            org_id=user.org_id,
            permission_fingerprint=_permission_fingerprint(user),
            clickhouse=await get_global_client(_analytics_db_url()),
            scope=scope,
            evidence=[evidence],
        )
    except DevRuntimeUnavailable as exc:
        _raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "source_unavailable",
            exc.safe_message,
            request_id=request_id,
            retryable=True,
        )
        raise AssertionError("unreachable")
    except Exception:
        _raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "source_unavailable",
            "Ask Dev evidence expansion is temporarily unavailable.",
            request_id=request_id,
            retryable=True,
        )
        raise AssertionError("unreachable")
    expansion = result.expansions[0]
    if expansion.state.value == "unauthorized":
        _raise(
            status.HTTP_404_NOT_FOUND,
            "scope_not_found",
            "The Ask Dev evidence was not found.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")
    return DevEvidenceExpansion(
        schema_version="dev_evidence_expansion.v1",
        evidence=expansion.evidence,
        state=expansion.state.value,
        safe_excerpt=expansion.safe_excerpt,
        serialized_bytes=expansion.serialized_bytes,
        warning=expansion.warning,
        query_version=result.query_version,
    )


@router.post("/answers/{answer_id}/feedback", response_model=DevFeedback)
async def create_feedback(
    answer_id: uuid.UUID,
    body: DevFeedbackCreateRequest,
    auth: Annotated[
        tuple[AuthenticatedUser, DevPersistenceService, str | None],
        Depends(_require_ask_dev),
    ],
) -> DevFeedback:
    user, service, request_id = auth
    org_id, user_id = _owned_ids(user, request_id)
    try:
        feedback = await service.record_feedback(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
            rating=body.rating,
            reasons=body.reasons,
            comment=body.comment,
        )
    except Exception as exc:
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")
    return DevFeedback(
        schema_version="dev_feedback.v1",
        feedback_id=str(feedback.id),
        answer_id=str(feedback.answer_id),
        rating=feedback.rating,
        reasons=feedback.reasons,
        comment=feedback.comment,
        created_at=feedback.created_at,
    )


__all__ = [
    "AskDevApiError",
    "DevCapabilityRuntime",
    "ask_dev_error_handler",
    "ask_dev_validation_error_handler",
    "get_dev_capability_runtime",
    "router",
]
