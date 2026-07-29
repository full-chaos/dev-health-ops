"""Authenticated, tenant-scoped REST surface for Ask Dev."""

from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Literal

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
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing import evaluate_org_feature_async

from .contracts import (
    DevCapabilities,
    DevConversation,
    DevConversationSummary,
    DevError,
    DevFeedback,
    DevScope,
)
from .entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from .persistence.service import (
    ConversationRecord,
    DevPersistenceConflict,
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
)


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


async def get_dev_capability_runtime() -> DevCapabilityRuntime:
    """Fail-closed default until a certified provider selection is injected."""

    return DevCapabilityRuntime()


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


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _aware_required(value: datetime) -> datetime:
    return _aware(value) or value.replace(tzinfo=UTC)


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
    byo_llm = await _feature_allowed(session, org_id, "byo_llm")
    agent_context_runtime = await _feature_allowed(
        session, org_id, "agent_context_runtime"
    )
    readiness = runtime.readiness if ask_dev else "disabled"
    safe_failure = runtime.safe_failure_reason if ask_dev else "Ask Dev is not enabled."
    return DevCapabilities(
        schema_version="dev_capabilities.v1",
        ask_dev=ask_dev,
        byo_llm=byo_llm,
        agent_context_runtime=agent_context_runtime,
        can_read=ask_dev and readiness == "ready",
        can_manage=ask_dev and user.is_admin,
        effective_provider_label=runtime.effective_provider_label,
        effective_model_label=runtime.effective_model_label,
        provider_source=runtime.provider_source,
        readiness=readiness,
        supported_contract_versions=[
            "dev_capabilities.v1",
            "dev_conversation.v1",
            "dev_message_request.v1",
            "dev_answer.v1",
            "dev_stream_event.v1",
            "dev_error.v1",
        ],
        contextual_entrypoints=ask_dev and runtime.contextual_entrypoints,
        evidence_resolver=ask_dev and runtime.evidence_resolver,
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
            retention_days=body.retention_days,
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
        Depends(_require_ask_dev),
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
