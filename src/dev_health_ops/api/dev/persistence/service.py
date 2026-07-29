"""Tenant-scoped persistence and lifecycle operations for Ask Dev.

The service accepts authenticated organization/user identifiers from its caller;
HTTP payloads and model output must never supply those values. All reads and
writes include both identifiers, including administrator operations where the
target user is explicit.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeAlias

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.dev_persistence import (
    DEV_RETENTION_DAYS,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevToolCall,
)

logger = logging.getLogger(__name__)

AnswerPayloadValidator: TypeAlias = Callable[[Mapping[str, Any]], Mapping[str, Any]]

_TERMINAL_RUN_STATES = frozenset(
    {
        "completed",
        "insufficient_evidence",
        "refused",
        "failed",
        "cancelled",
    }
)
_RUN_STATES = _TERMINAL_RUN_STATES | frozenset(
    {
        "accepted",
        "resolving_scope",
        "model_decision",
        "tool_validation",
        "tool_execution",
        "answer_validation",
    }
)
_FEEDBACK_REASONS = frozenset(
    {
        "incorrect",
        "missing_evidence",
        "wrong_scope",
        "stale_data",
        "unclear",
        "useful",
    }
)
_FORBIDDEN_METADATA_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "chain_of_thought",
        "credential",
        "credentials",
        "graphql_result",
        "password",
        "private_reasoning",
        "provider_request",
        "provider_response",
        "raw_prompt",
        "raw_provider_payload",
        "secret",
        "source_payload",
        "sql_result",
        "system_prompt",
    }
)
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")


class DevPersistenceNotFound(LookupError):
    """The requested tenant-owned resource is absent or unauthorized."""


class DevPersistenceConflict(RuntimeError):
    """An idempotency key exists without its required durable companion row."""


class DevPersistenceValidationError(ValueError):
    """Input is outside the approved bounded persistence contract."""


@dataclass(frozen=True, slots=True)
class MessageRunResult:
    message: DevMessage
    run: DevRun
    created: bool


@dataclass(frozen=True, slots=True)
class CleanupResult:
    reason: str
    selected: int
    purged: int


@dataclass(frozen=True, slots=True)
class ConversationRecord:
    conversation: DevConversation
    message_count: int
    latest_answer_id: uuid.UUID | None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _json_copy(value: Any, *, field: str, forbid_sensitive_keys: bool = True) -> Any:
    if forbid_sensitive_keys:
        _reject_sensitive_keys(value, field=field)
    try:
        encoded = json.dumps(value, separators=(",", ":"), sort_keys=True)
        return json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise DevPersistenceValidationError(
            f"{field} must be JSON serializable"
        ) from exc


def _reject_sensitive_keys(value: Any, *, field: str) -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).strip().lower()
            if key in _FORBIDDEN_METADATA_KEYS:
                raise DevPersistenceValidationError(
                    f"{field} contains prohibited metadata key"
                )
            _reject_sensitive_keys(child, field=field)
    elif isinstance(value, list | tuple):
        for child in value:
            _reject_sensitive_keys(child, field=field)


def _bounded_text(value: str | None, *, field: str, max_bytes: int) -> str | None:
    if value is None:
        return None
    if len(value.encode("utf-8")) > max_bytes:
        raise DevPersistenceValidationError(f"{field} exceeds {max_bytes} UTF-8 bytes")
    return value


def _digest(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    if len(value) != 71 or not value.startswith("sha256:"):
        raise DevPersistenceValidationError(f"{field} must be a sha256 digest")
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise DevPersistenceValidationError(f"{field} must be a sha256 digest") from exc
    return value


def _safe_token(value: str | None, *, field: str, max_bytes: int) -> str | None:
    value = _bounded_text(value, field=field, max_bytes=max_bytes)
    if value is not None and _SAFE_TOKEN.fullmatch(value) is None:
        raise DevPersistenceValidationError(f"{field} must be a safe identifier")
    return value


def _safe_count_summary(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    copied = _json_copy(dict(value), field=field)
    for key, item in copied.items():
        if _SAFE_TOKEN.fullmatch(key) is None:
            raise DevPersistenceValidationError(f"{field} contains an unsafe key")
        if item is not None and not isinstance(item, bool | int | float):
            raise DevPersistenceValidationError(
                f"{field} may contain only counts, booleans, or null"
            )
        if isinstance(item, int | float) and not isinstance(item, bool) and item < 0:
            raise DevPersistenceValidationError(f"{field} counts must be nonnegative")
    return copied


class DevPersistenceService:
    """Persistence seam used by the future Ask Dev API and orchestrator."""

    def __init__(
        self, session: AsyncSession, *, now: Callable[[], datetime] = _utc_now
    ):
        self.session = session
        self._now = now

    async def create_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        current_scope: Mapping[str, Any],
        retention_days: int = 30,
        title: str | None = None,
    ) -> DevConversation:
        if retention_days not in DEV_RETENTION_DAYS:
            raise DevPersistenceValidationError(
                "Ask Dev retention must be exactly 0 or 30 days"
            )
        title = _bounded_text(title, field="title", max_bytes=200)
        scope = _json_copy(dict(current_scope), field="current_scope")
        now = self._now()
        conversation = DevConversation(
            org_id=org_id,
            user_id=user_id,
            title=title,
            current_scope=scope,
            retention_days=retention_days,
            created_at=now,
            updated_at=now,
            expires_at=(now + timedelta(days=30)) if retention_days == 30 else None,
        )
        self.session.add(conversation)
        await self.session.flush()
        return conversation

    async def list_conversation_records(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limit: int = 50,
        before: datetime | None = None,
        before_id: uuid.UUID | None = None,
    ) -> Sequence[ConversationRecord]:
        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError("limit must be between 1 and 100")
        if (before is None) != (before_id is None):
            raise DevPersistenceValidationError(
                "before and before_id must be provided together"
            )
        now = self._now()
        conditions = [
            DevConversation.org_id == org_id,
            DevConversation.user_id == user_id,
            DevConversation.deleted_at.is_(None),
            (DevConversation.expires_at.is_(None) | (DevConversation.expires_at > now)),
        ]
        if before is not None and before_id is not None:
            conditions.append(
                or_(
                    DevConversation.updated_at < before,
                    and_(
                        DevConversation.updated_at == before,
                        DevConversation.id < before_id,
                    ),
                )
            )
        latest_answer_id = (
            select(DevMessage.answer_id)
            .where(
                DevMessage.conversation_id == DevConversation.id,
                DevMessage.org_id == DevConversation.org_id,
                DevMessage.user_id == DevConversation.user_id,
                DevMessage.role == "assistant",
                DevMessage.answer_id.is_not(None),
            )
            .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        message_count = (
            select(func.count(DevMessage.id))
            .where(
                DevMessage.conversation_id == DevConversation.id,
                DevMessage.org_id == DevConversation.org_id,
                DevMessage.user_id == DevConversation.user_id,
            )
            .scalar_subquery()
        )
        rows = await self.session.execute(
            select(
                DevConversation,
                message_count.label("message_count"),
                latest_answer_id.label("latest_answer_id"),
            )
            .where(and_(*conditions))
            .order_by(DevConversation.updated_at.desc(), DevConversation.id.desc())
            .limit(limit)
        )
        return [
            ConversationRecord(
                conversation=row[0],
                message_count=int(row[1] or 0),
                latest_answer_id=row[2],
            )
            for row in rows.all()
        ]

    async def list_conversations(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limit: int = 50,
        before: datetime | None = None,
        before_id: uuid.UUID | None = None,
    ) -> Sequence[DevConversation]:
        records = await self.list_conversation_records(
            org_id=org_id,
            user_id=user_id,
            limit=limit,
            before=before,
            before_id=before_id,
        )
        return [record.conversation for record in records]

    async def get_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> DevConversation:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            include_expired=False,
        )
        if conversation is None:
            raise DevPersistenceNotFound("conversation not found")
        return conversation

    async def get_conversation_record(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> ConversationRecord:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        message_count = await self.session.scalar(
            select(func.count(DevMessage.id)).where(
                DevMessage.conversation_id == conversation.id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
            )
        )
        latest_answer_id = await self.session.scalar(
            select(DevMessage.answer_id)
            .where(
                DevMessage.conversation_id == conversation.id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.role == "assistant",
                DevMessage.answer_id.is_not(None),
            )
            .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
            .limit(1)
        )
        return ConversationRecord(
            conversation=conversation,
            message_count=int(message_count or 0),
            latest_answer_id=latest_answer_id,
        )

    async def rename_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        title: str | None,
    ) -> DevConversation:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        conversation.title = _bounded_text(title, field="title", max_bytes=200)
        self._touch(conversation)
        await self.session.flush()
        return conversation

    async def append_user_message_and_run(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
        question: str,
        scope_snapshot: Mapping[str, Any],
        request_id: uuid.UUID | None = None,
        retry_of_run_id: uuid.UUID | None = None,
    ) -> MessageRunResult:
        question = _bounded_text(question, field="question", max_bytes=8192) or ""
        if not question.strip():
            raise DevPersistenceValidationError("question must not be empty")
        scope = _json_copy(dict(scope_snapshot), field="scope_snapshot")
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )

        existing = await self._message_run_by_client_id(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=client_message_id,
        )
        if existing is not None:
            return existing

        message = DevMessage(
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            client_message_id=client_message_id,
            role="user",
            content=question,
            scope_snapshot=scope,
            created_at=self._now(),
        )
        run = DevRun(
            request_id=request_id or uuid.uuid4(),
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            state="accepted",
            retry_of_run_id=retry_of_run_id,
            started_at=self._now(),
            created_at=self._now(),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(message)
                await self.session.flush()
                run.user_message_id = message.id
                self.session.add(run)
                await self.session.flush()
        except IntegrityError:
            existing = await self._message_run_by_client_id(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=client_message_id,
            )
            if existing is None:
                raise
            return existing

        self._touch(conversation)
        return MessageRunResult(message=message, run=run, created=True)

    async def append_assistant_answer(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        answer_payload: Mapping[str, Any],
        validator: AnswerPayloadValidator,
        scope_snapshot: Mapping[str, Any],
        rendered_content: str | None = None,
    ) -> DevMessage:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        try:
            validated = validator(answer_payload)
        except Exception as exc:
            raise DevPersistenceValidationError(
                "answer payload validation failed"
            ) from exc
        payload = _json_copy(
            dict(validated),
            field="answer_payload",
        )
        if payload.get("schema_version") != "dev_answer.v1":
            raise DevPersistenceValidationError(
                "validated answer payload must use dev_answer.v1"
            )
        try:
            answer_id = uuid.UUID(str(payload["answer_id"]))
            payload_conversation_id = uuid.UUID(str(payload["conversation_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise DevPersistenceValidationError(
                "validated answer payload is missing canonical identifiers"
            ) from exc
        if payload_conversation_id != conversation_id:
            raise DevPersistenceValidationError(
                "validated answer conversation does not match storage target"
            )
        existing = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.answer_id == answer_id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.conversation_id == conversation_id,
                DevMessage.role == "assistant",
            )
        )
        if existing is not None:
            return existing
        content = _bounded_text(
            rendered_content, field="rendered_content", max_bytes=32 * 1024
        )
        message = DevMessage(
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            role="assistant",
            content=content,
            answer_id=answer_id,
            answer_payload=payload,
            scope_snapshot=_json_copy(dict(scope_snapshot), field="scope_snapshot"),
            created_at=self._now(),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(message)
                await self.session.flush()
        except IntegrityError:
            existing = await self.session.scalar(
                select(DevMessage).where(
                    DevMessage.answer_id == answer_id,
                    DevMessage.org_id == org_id,
                    DevMessage.user_id == user_id,
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
            if existing is None:
                raise
            return existing
        self._touch(conversation)
        return message

    async def update_run(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        state: str,
        answer_id: uuid.UUID | None = None,
        terminal_reason: str | None = None,
        provider_source: str | None = None,
        provider_fingerprint: str | None = None,
        model_fingerprint: str | None = None,
        prompt_version: str | None = None,
        tool_contract_version: str | None = None,
        metric_version: str | None = None,
        query_version: str | None = None,
        latency_ms: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        estimated_cost_microusd: int | None = None,
        tool_call_count: int | None = None,
        citation_count: int | None = None,
        metric_count: int | None = None,
        grounding_validation_status: str | None = None,
        safe_error_code: str | None = None,
    ) -> DevRun | None:
        if state not in _RUN_STATES:
            raise DevPersistenceValidationError("invalid run state")
        if provider_source not in {None, "platform", "byo"}:
            raise DevPersistenceValidationError("invalid provider source")
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if run.state in _TERMINAL_RUN_STATES:
            if run.state == state:
                return run
            raise DevPersistenceConflict("terminal run state cannot be changed")
        for value, field in (
            (latency_ms, "latency_ms"),
            (input_tokens, "input_tokens"),
            (output_tokens, "output_tokens"),
            (estimated_cost_microusd, "estimated_cost_microusd"),
            (tool_call_count, "tool_call_count"),
            (citation_count, "citation_count"),
            (metric_count, "metric_count"),
        ):
            if value is not None and value < 0:
                raise DevPersistenceValidationError(f"{field} must be nonnegative")

        run.state = state
        run.answer_id = answer_id
        run.terminal_reason = _safe_token(
            terminal_reason, field="terminal_reason", max_bytes=64
        )
        run.provider_source = provider_source
        run.provider_fingerprint = _digest(
            provider_fingerprint, field="provider_fingerprint"
        )
        run.model_fingerprint = _digest(model_fingerprint, field="model_fingerprint")
        run.prompt_version = _safe_token(
            prompt_version, field="prompt_version", max_bytes=128
        )
        run.tool_contract_version = _safe_token(
            tool_contract_version, field="tool_contract_version", max_bytes=128
        )
        run.metric_version = _safe_token(
            metric_version, field="metric_version", max_bytes=128
        )
        run.query_version = _safe_token(
            query_version, field="query_version", max_bytes=128
        )
        run.latency_ms = latency_ms
        run.input_tokens = input_tokens
        run.output_tokens = output_tokens
        run.estimated_cost_microusd = estimated_cost_microusd
        if tool_call_count is not None:
            run.tool_call_count = tool_call_count
        if citation_count is not None:
            run.citation_count = citation_count
        if metric_count is not None:
            run.metric_count = metric_count
        run.grounding_validation_status = _safe_token(
            grounding_validation_status,
            field="grounding_validation_status",
            max_bytes=32,
        )
        run.safe_error_code = _safe_token(
            safe_error_code, field="safe_error_code", max_bytes=64
        )
        if state in _TERMINAL_RUN_STATES:
            run.ended_at = self._now()
        await self.session.flush()

        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=run.conversation_id,
            include_expired=True,
        )
        if (
            state in _TERMINAL_RUN_STATES
            and conversation is not None
            and conversation.retention_days == 0
        ):
            await self._purge_conversation(
                conversation=conversation,
                reason="ephemeral_completed",
                actor_user_id=user_id,
            )
            return None
        return run

    async def append_tool_call(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        tool_id: str,
        tool_version: str,
        canonical_input_hash: str,
        safe_scope_summary: Mapping[str, Any],
        status: str,
        result_digest: str | None = None,
        evidence_ref_ids: Sequence[str] = (),
        latency_ms: int | None = None,
        row_count: int | None = None,
        item_count: int | None = None,
        byte_count: int | None = None,
        safe_error_code: str | None = None,
    ) -> DevToolCall:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 32767:
            raise DevPersistenceValidationError("tool ordinal is out of range")
        if status not in {"pending", "running", "completed", "failed", "cancelled"}:
            raise DevPersistenceValidationError("invalid tool-call status")
        for value, field in (
            (latency_ms, "latency_ms"),
            (row_count, "row_count"),
            (item_count, "item_count"),
            (byte_count, "byte_count"),
        ):
            if value is not None and value < 0:
                raise DevPersistenceValidationError(f"{field} must be nonnegative")
        references = [
            _safe_token(str(value), field="evidence_ref_id", max_bytes=128) or ""
            for value in evidence_ref_ids
        ]
        if len(references) > 25:
            raise DevPersistenceValidationError("too many evidence references")
        tool_call = DevToolCall(
            run_id=run.id,
            conversation_id=run.conversation_id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            tool_id=_safe_token(tool_id, field="tool_id", max_bytes=96) or "",
            tool_version=_safe_token(tool_version, field="tool_version", max_bytes=64)
            or "",
            canonical_input_hash=_digest(
                canonical_input_hash, field="canonical_input_hash"
            )
            or "",
            safe_scope_summary=_safe_count_summary(
                safe_scope_summary, field="safe_scope_summary"
            ),
            status=status,
            result_digest=_digest(result_digest, field="result_digest"),
            evidence_ref_ids=references,
            latency_ms=latency_ms,
            row_count=row_count,
            item_count=item_count,
            byte_count=byte_count,
            safe_error_code=_safe_token(
                safe_error_code, field="safe_error_code", max_bytes=64
            ),
            created_at=self._now(),
            completed_at=self._now()
            if status in {"completed", "failed", "cancelled"}
            else None,
        )
        self.session.add(tool_call)
        await self.session.flush()
        return tool_call

    async def record_feedback(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        answer_id: uuid.UUID,
        rating: str,
        reasons: Sequence[str],
        comment: str | None = None,
    ) -> DevFeedback:
        if rating not in {"helpful", "not_helpful"}:
            raise DevPersistenceValidationError("invalid feedback rating")
        normalized_reasons = sorted(set(reasons))
        if not set(normalized_reasons).issubset(_FEEDBACK_REASONS):
            raise DevPersistenceValidationError("invalid feedback reason")
        comment = _bounded_text(comment, field="feedback comment", max_bytes=2048)
        answer = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.answer_id == answer_id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.role == "assistant",
            )
        )
        if answer is None:
            raise DevPersistenceNotFound("answer not found")
        feedback = await self.session.scalar(
            select(DevFeedback).where(
                DevFeedback.answer_id == answer_id,
                DevFeedback.org_id == org_id,
                DevFeedback.user_id == user_id,
            )
        )
        now = self._now()
        if feedback is None:
            feedback = DevFeedback(
                answer_id=answer_id,
                org_id=org_id,
                user_id=user_id,
                rating=rating,
                reasons=normalized_reasons,
                comment=comment,
                created_at=now,
                updated_at=now,
            )
            self.session.add(feedback)
        else:
            feedback.rating = rating
            feedback.reasons = normalized_reasons
            feedback.comment = comment
            feedback.updated_at = now
        await self.session.flush()
        return feedback

    async def delete_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> bool:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            include_expired=True,
        )
        if conversation is None:
            return False
        await self._purge_conversation(
            conversation=conversation,
            reason="user_deleted",
            actor_user_id=user_id,
        )
        return True

    async def admin_purge_conversation(
        self,
        *,
        org_id: uuid.UUID,
        target_user_id: uuid.UUID,
        actor_user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> bool:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=target_user_id,
            conversation_id=conversation_id,
            include_expired=True,
        )
        if conversation is None:
            return False
        await self._purge_conversation(
            conversation=conversation,
            reason="admin_purged",
            actor_user_id=actor_user_id,
        )
        return True

    async def purge_user_content(
        self,
        *,
        org_id: uuid.UUID,
        target_user_id: uuid.UUID,
        actor_user_id: uuid.UUID | None,
        admin: bool,
        limit: int = 500,
    ) -> CleanupResult:
        if limit < 1 or limit > 500:
            raise DevPersistenceValidationError("purge limit must be between 1 and 500")
        conversations = (
            (
                await self.session.execute(
                    select(DevConversation)
                    .where(
                        DevConversation.org_id == org_id,
                        DevConversation.user_id == target_user_id,
                    )
                    .order_by(DevConversation.created_at, DevConversation.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        reason = "admin_purged" if admin else "user_deleted"
        for conversation in conversations:
            await self._purge_conversation(
                conversation=conversation,
                reason=reason,
                actor_user_id=actor_user_id,
            )
        return CleanupResult(
            reason=reason,
            selected=len(conversations),
            purged=len(conversations),
        )

    async def cleanup_expired(self, *, limit: int = 500) -> CleanupResult:
        """Purge one retry-safe bounded batch selected by persisted expiry."""

        if limit < 1 or limit > 500:
            raise DevPersistenceValidationError(
                "cleanup limit must be between 1 and 500"
            )
        now = self._now()
        conversations = (
            (
                await self.session.execute(
                    select(DevConversation)
                    .where(
                        DevConversation.expires_at.is_not(None),
                        DevConversation.expires_at <= now,
                    )
                    .order_by(DevConversation.expires_at, DevConversation.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        for conversation in conversations:
            reason = (
                "ephemeral_completed"
                if conversation.retention_days == 0
                else "retention_expired"
            )
            await self._purge_conversation(
                conversation=conversation,
                reason=reason,
                actor_user_id=None,
            )
        logger.info(
            "ask_dev_retention_cleanup_completed",
            extra={"selected": len(conversations), "purged": len(conversations)},
        )
        return CleanupResult(
            reason="retention_expired",
            selected=len(conversations),
            purged=len(conversations),
        )

    async def _owned_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        include_expired: bool,
    ) -> DevConversation | None:
        conditions = [
            DevConversation.id == conversation_id,
            DevConversation.org_id == org_id,
            DevConversation.user_id == user_id,
            DevConversation.deleted_at.is_(None),
        ]
        if not include_expired:
            conditions.append(
                DevConversation.expires_at.is_(None)
                | (DevConversation.expires_at > self._now())
            )
        return await self.session.scalar(select(DevConversation).where(*conditions))

    async def _owned_run(
        self, *, org_id: uuid.UUID, user_id: uuid.UUID, run_id: uuid.UUID
    ) -> DevRun | None:
        return await self.session.scalar(
            select(DevRun).where(
                DevRun.id == run_id,
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
            )
        )

    async def _message_run_by_client_id(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
    ) -> MessageRunResult | None:
        message = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.conversation_id == conversation_id,
                DevMessage.client_message_id == client_message_id,
            )
        )
        if message is None:
            return None
        run = await self.session.scalar(
            select(DevRun).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                DevRun.user_message_id == message.id,
            )
        )
        if run is None:
            raise DevPersistenceConflict(
                "client message exists without its idempotent run"
            )
        return MessageRunResult(message=message, run=run, created=False)

    def _touch(self, conversation: DevConversation) -> None:
        now = self._now()
        conversation.updated_at = now
        if conversation.retention_days == 30:
            conversation.expires_at = now + timedelta(days=30)

    async def _purge_conversation(
        self,
        *,
        conversation: DevConversation,
        reason: str,
        actor_user_id: uuid.UUID | None,
    ) -> None:
        existing = await self.session.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == conversation.id
            )
        )
        if existing is None:
            self.session.add(
                DevConversationTombstone(
                    conversation_id=conversation.id,
                    org_id=conversation.org_id,
                    user_id=conversation.user_id,
                    actor_user_id=actor_user_id,
                    reason=reason,
                    retention_days=conversation.retention_days,
                    conversation_created_at=conversation.created_at,
                    deleted_at=self._now(),
                )
            )
            await self.session.flush()
        await self.session.delete(conversation)
        await self.session.flush()
