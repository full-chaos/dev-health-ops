"""Authenticated, tenant-scoped REST surface for Ask Dev."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
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
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api._health import _analytics_db_url
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.queries.client import get_global_client
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.api.services.permissions import get_user_permissions
from dev_health_ops.licensing import evaluate_org_feature_async
from dev_health_ops.licensing.registry import ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE
from dev_health_ops.llm.agent.contracts import AgentUsage

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
    DevScope,
    DevTranscriptEntry,
)
from .entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from .orchestrator import OrchestratorEvent, OrchestratorResult, RunState
from .orchestrator_persistence import PersistenceRunRecorder
from .org_policy import load_ask_dev_org_policy
from .persistence.service import (
    ConversationRecord,
    DevAdmissionLimits,
    DevConcurrencyLimitExceeded,
    DevPersistenceConflict,
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
    DevRateLimitExceeded,
    TranscriptRecord,
)
from .production_runtime import (
    build_production_runtime,
    expand_production_evidence,
    resolve_production_provider,
)
from .runtime import BoundedDevRuntime, DevRuntimeUnavailable
from .streaming import encoded_sse_stream


def _disable_shared_cache(response: Response) -> None:
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Pragma"] = "no-cache"


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
    rating: Literal["helpful", "not_helpful"]
    reasons: list[
        Literal[
            "incorrect",
            "missing_evidence",
            "wrong_scope",
            "stale_data",
            "unclear",
            "useful",
        ]
    ] = Field(min_length=1, max_length=6)
    comment: (
        Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None
    ) = None


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
        return DevCapabilityRuntime(
            effective_provider_label=provider.provider_label,
            effective_model_label=provider.model_label,
            provider_source=provider.source.value,
            readiness="ready" if evidence_ready else "degraded",
            contextual_entrypoints=True,
            evidence_resolver=evidence_ready,
            safe_failure_reason=(
                None if evidence_ready else "Ask Dev evidence signing is unavailable."
            ),
        )
    finally:
        try:
            await provider.provider.aclose()
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
        content=exc.error.model_dump(mode="json"),
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
        content=error.model_dump(mode="json"),
        headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"},
    )


def _error(
    request_id: str | None,
    code: str,
    message: str,
    *,
    retryable: bool = False,
) -> DevError:
    safe_request_id = request_id or str(uuid.uuid4())
    try:
        return DevError(
            schema_version="dev_error.v1",
            request_id=safe_request_id,
            code=code,
            safe_message=message,
            retryable=retryable,
        )
    except ValueError:
        return DevError(
            schema_version="dev_error.v1",
            request_id=str(uuid.uuid4()),
            code=code,
            safe_message=message,
            retryable=retryable,
        )


def _raise(
    status_code: int,
    code: str,
    message: str,
    *,
    request_id: str | None = None,
    retryable: bool = False,
) -> None:
    raise AskDevApiError(
        status_code,
        _error(request_id, code, message, retryable=retryable),
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


def _replayed_result(
    *, run: Any, answer_payload: dict[str, Any] | None
) -> OrchestratorResult:
    state = RunState(run.state)
    answer = None
    error = None
    if answer_payload is not None:
        answer = DevAnswer.model_validate(answer_payload)
    else:
        code = run.safe_error_code or "internal_error"
        try:
            error = DevError(
                schema_version="dev_error.v1",
                request_id=str(run.request_id),
                code=code,
                safe_message=(
                    "The prior Ask Dev request did not complete with an answer."
                ),
                retryable=code in {"provider_unavailable", "source_unavailable"},
            )
        except ValueError:
            error = DevError(
                schema_version="dev_error.v1",
                request_id=str(run.request_id),
                code="internal_error",
                safe_message=(
                    "The prior Ask Dev request did not complete with an answer."
                ),
                retryable=True,
            )
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
    )


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
) -> DevCapabilities:
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
        ask_dev=effective_ask_dev,
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
                entries.append(
                    DevTranscriptEntry(
                        **common,
                        role="assistant",
                        answer=DevAnswer.model_validate(message.answer_payload),
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
    try:
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
        )
        await service.session.commit()
    except Exception as exc:
        await service.session.rollback()
        _raise_persistence(exc, request_id)
        raise AssertionError("unreachable")

    cancellation = asyncio.Event()
    run_id = str(accepted.run.id)

    if not accepted.created:
        if accepted.run.state not in {
            RunState.COMPLETED.value,
            RunState.INSUFFICIENT_EVIDENCE.value,
            RunState.REFUSED.value,
            RunState.FAILED.value,
            RunState.CANCELLED.value,
        }:
            _raise(
                status.HTTP_409_CONFLICT,
                "concurrency_limited",
                "The matching Ask Dev request is still running.",
                request_id=request_id,
                retryable=True,
            )
        answer_payload = None
        if accepted.run.answer_id is not None:
            try:
                answer_message = await service.get_answer_message(
                    org_id=org_id,
                    user_id=user_id,
                    answer_id=accepted.run.answer_id,
                )
                answer_payload = answer_message.answer_payload
            except Exception as exc:
                _raise_persistence(exc, request_id)
                raise AssertionError("unreachable")
        replayed = _replayed_result(
            run=accepted.run,
            answer_payload=answer_payload,
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
        error_code = runtime_resolution.error_code or "provider_not_configured"
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            state=RunState.FAILED.value,
            safe_error_code=error_code,
        )
        await service.session.commit()
        _raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code,
            runtime_resolution.safe_message or "No certified Ask Dev model is ready.",
            request_id=request_id,
        )
        raise AssertionError("unreachable")

    answer_id = str(uuid.uuid4())
    recorder = PersistenceRunRecorder(
        service,
        org_id=org_id,
        user_id=user_id,
        conversation_id=conversation_id,
        run_id=accepted.run.id,
        provider_source=runtime.provider_source,
    )

    async def run_with_events(event_sink) -> OrchestratorResult:
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
        )

    async def chunks() -> AsyncGenerator[bytes, None]:
        async for chunk in encoded_sse_stream(
            run_id=run_id,
            run_with_events=run_with_events,
            cancellation=cancellation,
        ):
            yield chunk

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
