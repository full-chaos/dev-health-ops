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

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dev.org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD
from dev_health_ops.models.dev_persistence import (
    DEV_RETENTION_DAYS,
    DevAnswerFrame,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevRunIntent,
    DevRunInvestigationResult,
    DevRunNarrative,
    DevRunResolution,
    DevRunSourceObservation,
    DevRunStageDiagnostic,
    DevRunSubjectSet,
    DevToolCall,
)
from dev_health_ops.models.users import Organization, User

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
        # CHAOS-3292 preflight phases. Non-terminal by construction, so a run
        # sitting in one correctly yields 409 concurrency_limited on replay
        # rather than being treated as a finished run.
        "interpreting",
        "resolving_subjects",
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

# -- Wave 3.1 (CHAOS-3299) closed vocabularies -------------------------------
# Mirrors the CHECK constraints on the corresponding tables (defense in
# depth, matching the existing double-check pattern in append_tool_call /
# update_run: the DB CHECK is the hard backstop, this is the friendly error).
_QUESTION_INTENT_IDS = frozenset(
    {
        "entity_status",
        "portfolio_status",
        "remaining_work",
        "observed_change",
        "registered_statistics",
        "metric_comparison",
        "data_trust",
        "project_health",
        "team_health",
        "team_workload_balance",
        "operational_deficiency_inventory",
        "bounded_investigation",
    }
)
_CARDINALITIES = frozenset({"singular", "plural_cohort", "organization_wide"})
_RESOLUTION_OUTCOMES = frozenset(
    {
        "exact_match",
        "ambiguous_candidates",
        "no_authorized_match",
        "catalog_unavailable",
        "unsupported_kind",
    }
)
_ENTITY_KINDS = frozenset(
    {"repository", "project", "work_unit", "issue", "pull_request", "team"}
)
_SOURCE_CLASSES = frozenset(
    {
        "status_change",
        "work_item",
        "work_graph",
        "pull_request",
        "code_change",
        "review",
        "ci_run",
        "test_report",
        "deployment",
        "incident",
        "operational_control",
        "source_health",
    }
)
_REQUIREMENT_LEVELS = frozenset(
    {"mandatory", "conditional", "optional", "not_applicable"}
)
_SOURCE_OBSERVED_STATES = frozenset(
    {
        "available_current",
        "available_stale",
        "available_unknown",
        "unconfigured",
        "unavailable",
        "unauthorized_or_not_visible",
        "not_applicable",
        "truncated",
    }
)
_DATA_SEMANTICS = frozenset({"measured_zero", "no_data", "not_measured"})
_PUBLIC_OUTCOMES = frozenset(
    {
        "answered",
        "answered_with_gaps",
        "needs_clarification",
        "not_found",
        "temporarily_unavailable",
        "unsupported",
        "denied",
        "failed",
    }
)
_NARRATIVE_MODES = frozenset({"provider", "deterministic_fallback"})
_STAGE_IDS = frozenset(
    {
        "interpreting",
        "resolving_subjects",
        "planning",
        "collecting",
        "synthesizing_frame",
        "narrating_optional",
        "projecting_answer",
    }
)
_STAGE_STATUSES = frozenset({"started", "completed", "failed", "skipped"})

# Bounds on the opaque JSONB payload each Wave 3.1 artifact carries. These are
# defense-in-depth (SQLite test targets have no DB-level byte-length CHECK);
# the contract's own bounded collection max_lengths are the primary bound.
_INTENT_PAYLOAD_MAX_BYTES = 16 * 1024
_RESOLUTION_PAYLOAD_MAX_BYTES = 8 * 1024
_SUBJECT_SET_PAYLOAD_MAX_BYTES = 16 * 1024
_SOURCE_OBSERVATION_PAYLOAD_MAX_BYTES = 16 * 1024
_INVESTIGATION_RESULT_PAYLOAD_MAX_BYTES = 8 * 1024
_FRAME_PAYLOAD_MAX_BYTES = 128 * 1024
_NARRATIVE_TEXT_MAX_BYTES = 8 * 1024
_NARRATIVE_PAYLOAD_MAX_BYTES = 16 * 1024


class DevPersistenceNotFound(LookupError):
    """The requested tenant-owned resource is absent or unauthorized."""


class DevPersistenceConflict(RuntimeError):
    """An idempotency key exists without its required durable companion row."""


class DevPersistenceValidationError(ValueError):
    """Input is outside the approved bounded persistence contract."""


class DevConcurrencyLimitExceeded(RuntimeError):
    """The exact per-user or per-organization active-run limit was reached."""


class DevRateLimitExceeded(RuntimeError):
    """The exact per-user or per-organization request window was reached."""


class DevMonthlyRequestLimitExceeded(RuntimeError):
    """The platform-managed organization request allowance was exhausted."""

    def __init__(self, reset_at: datetime) -> None:
        self.reset_at = reset_at
        super().__init__("Ask Dev monthly platform request allowance reached")


class DevMonthlyCostLimitExceeded(RuntimeError):
    """The platform-managed organization monetary allowance was exhausted."""

    def __init__(self, reset_at: datetime) -> None:
        self.reset_at = reset_at
        super().__init__("Ask Dev monthly platform cost allowance reached")


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


@dataclass(frozen=True, slots=True)
class TranscriptRecord:
    message: DevMessage
    run: DevRun


@dataclass(frozen=True, slots=True)
class TranscriptPage:
    records: Sequence[TranscriptRecord]
    has_more: bool


@dataclass(frozen=True, slots=True)
class DevAdmissionLimits:
    active_runs_per_user: int = 1
    active_runs_per_org: int = 5
    requests_per_user_per_15_minutes: int = 20
    requests_per_org_per_hour: int = 100

    def __post_init__(self) -> None:
        if (
            self.active_runs_per_user < 1
            or self.active_runs_per_user > 1
            or self.active_runs_per_org < 1
            or self.active_runs_per_org > 5
            or self.requests_per_user_per_15_minutes < 1
            or self.requests_per_user_per_15_minutes > 20
            or self.requests_per_org_per_hour < 1
            or self.requests_per_org_per_hour > 100
        ):
            raise ValueError("Ask Dev admission limits may only be configured downward")


@dataclass(frozen=True, slots=True)
class DevPlatformAllowance:
    monthly_request_limit: int
    monthly_cost_limit_microusd: int
    per_run_reservation_microusd: int = ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

    def __post_init__(self) -> None:
        if (
            self.monthly_request_limit < 1
            or self.monthly_cost_limit_microusd < 1
            or self.per_run_reservation_microusd < 1
            or self.per_run_reservation_microusd > self.monthly_cost_limit_microusd
        ):
            raise ValueError("Ask Dev platform allowance must be positive and bounded")


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


def _bounded_json(
    value: Mapping[str, Any], *, field: str, max_bytes: int
) -> dict[str, Any]:
    """A sensitive-key-checked, byte-bounded JSONB payload copy.

    Used for every Wave 3.1 (CHAOS-3299) opaque ``payload`` column: the
    contract's own pydantic validators already bound each collection's
    ``max_length``, but persistence re-checks byte size and forbidden keys
    independently rather than trusting the contract validator alone (the
    established double-check posture, e.g. ``append_tool_call``).
    """

    copied = _json_copy(dict(value), field=field)
    encoded = json.dumps(copied, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > max_bytes:
        raise DevPersistenceValidationError(f"{field} exceeds {max_bytes} bytes")
    return copied


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

    async def list_transcript_records(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        limit: int = 50,
        after: datetime | None = None,
        after_id: uuid.UUID | None = None,
    ) -> TranscriptPage:
        """Return only safe persisted user questions and validated answers."""

        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError("limit must be between 1 and 100")
        if (after is None) != (after_id is None):
            raise DevPersistenceValidationError(
                "after and after_id must be provided together"
            )
        conversation = await self.get_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        # Retention-zero conversations deliberately have no readable history,
        # including while their accepted run is still in flight.
        if conversation.retention_days == 0:
            raise DevPersistenceNotFound("conversation transcript not retained")

        conditions = [
            DevMessage.conversation_id == conversation_id,
            DevMessage.org_id == org_id,
            DevMessage.user_id == user_id,
        ]
        if after is not None and after_id is not None:
            conditions.append(
                or_(
                    DevMessage.created_at > after,
                    and_(
                        DevMessage.created_at == after,
                        DevMessage.id > after_id,
                    ),
                )
            )
        messages = list(
            (
                await self.session.scalars(
                    select(DevMessage)
                    .where(and_(*conditions))
                    .order_by(DevMessage.created_at, DevMessage.id)
                    .limit(limit + 1)
                )
            ).all()
        )
        has_more = len(messages) > limit
        messages = messages[:limit]
        if not messages:
            return TranscriptPage(records=(), has_more=False)

        user_message_ids = [
            message.id for message in messages if message.role == "user"
        ]
        answer_ids = [
            message.answer_id
            for message in messages
            if message.role == "assistant" and message.answer_id is not None
        ]
        runs = list(
            (
                await self.session.scalars(
                    select(DevRun).where(
                        DevRun.org_id == org_id,
                        DevRun.user_id == user_id,
                        DevRun.conversation_id == conversation_id,
                        or_(
                            DevRun.user_message_id.in_(user_message_ids),
                            DevRun.answer_id.in_(answer_ids),
                        ),
                    )
                )
            ).all()
        )
        by_user_message = {
            run.user_message_id: run for run in runs if run.user_message_id is not None
        }
        by_answer = {run.answer_id: run for run in runs if run.answer_id is not None}
        records: list[TranscriptRecord] = []
        for message in messages:
            if message.role == "user":
                run = by_user_message.get(message.id)
            elif message.answer_id is not None:
                run = by_answer.get(message.answer_id)
            else:
                run = None
            if run is None:
                raise DevPersistenceConflict(
                    "transcript message has no tenant-owned run"
                )
            records.append(TranscriptRecord(message=message, run=run))
        return TranscriptPage(records=records, has_more=has_more)

    async def get_message_run_by_client_id(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
    ) -> MessageRunResult:
        result = await self._message_run_by_client_id(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=client_message_id,
        )
        if result is None:
            raise DevPersistenceNotFound("message run not found")
        return result

    async def get_answer_frame(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> DevAnswerFrame | None:
        """Return the tenant-owned answer frame for one run, if any.

        Used by the replay path (``router._replayed_result``) to reconstruct
        a no-answer-payload outcome (``needs_clarification``/``not_found``/
        etc.) from the stored frame rather than a generic error, without
        rerunning any provider or tool (TRD v2 Section 12).
        """

        return await self.session.scalar(
            select(DevAnswerFrame).where(
                DevAnswerFrame.run_id == run_id,
                DevAnswerFrame.org_id == org_id,
                DevAnswerFrame.user_id == user_id,
            )
        )

    async def get_answer_message(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        answer_id: uuid.UUID,
    ) -> DevMessage:
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
        return answer

    async def list_prompt_history_messages(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        exclude_message_id: uuid.UUID,
        limit: int,
    ) -> Sequence[DevMessage]:
        """Return a bounded chronological suffix for safe prompt projection."""

        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError(
                "history limit must be between 1 and 100"
            )
        await self.get_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        messages = list(
            (
                await self.session.scalars(
                    select(DevMessage)
                    .where(
                        DevMessage.conversation_id == conversation_id,
                        DevMessage.org_id == org_id,
                        DevMessage.user_id == user_id,
                        DevMessage.id != exclude_message_id,
                    )
                    .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
                    .limit(limit)
                )
            ).all()
        )
        messages.reverse()
        return messages

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
        admission_limits: DevAdmissionLimits | None = None,
        provider_source: str | None = None,
        platform_allowance: DevPlatformAllowance | None = None,
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

        if retry_of_run_id is not None:
            retry_target = await self._owned_run(
                org_id=org_id,
                user_id=user_id,
                run_id=retry_of_run_id,
            )
            if retry_target is None or retry_target.conversation_id != conversation_id:
                raise DevPersistenceNotFound("retry run not found")
            if retry_target.state not in _TERMINAL_RUN_STATES:
                raise DevPersistenceValidationError("retry run is not terminal")

        if admission_limits is not None or platform_allowance is not None:
            await self._lock_admission_scope(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
            )
            existing = await self._message_run_by_client_id(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=client_message_id,
            )
            if existing is not None:
                return existing
            if admission_limits is not None:
                await self._enforce_admission_limits(
                    org_id=org_id,
                    user_id=user_id,
                    limits=admission_limits,
                )
            if platform_allowance is not None:
                if provider_source != "platform":
                    raise DevPersistenceValidationError(
                        "platform allowance requires a platform provider source"
                    )
                await self._enforce_platform_allowance(
                    org_id=org_id,
                    allowance=platform_allowance,
                )

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
            provider_source=provider_source,
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

        conversation.current_scope = scope
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
        if payload.get("schema_version") not in {"dev_answer.v1", "dev_answer.v2"}:
            raise DevPersistenceValidationError(
                "validated answer payload must use dev_answer.v1 or dev_answer.v2"
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

    async def record_run_diagnostics(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        preflight_outcome: str | None = None,
        legacy_guard_reason: str | None = None,
    ) -> DevRun | None:
        """Write the CHAOS-3292 content-free diagnostics on one run row.

        Deliberately **not** part of ``update_run``: that method rewrites every
        field it is given, clearing any it is not, so folding these in would
        make an ordinary state transition wipe them. Both values are validated
        as safe identifier tokens, so neither can carry question text, an
        entity name, or catalog content even if a caller passed one.

        A non-``None`` value never reverts to ``None``: the preflight records
        its outcome once, and the demoted legacy guard may later add its reason
        to the same row without erasing it.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if preflight_outcome is not None:
            run.preflight_outcome = _safe_token(
                preflight_outcome, field="preflight_outcome", max_bytes=32
            )
        if legacy_guard_reason is not None:
            run.legacy_guard_reason = _safe_token(
                legacy_guard_reason, field="legacy_guard_reason", max_bytes=64
            )
        await self.session.flush()
        return run

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

    # -- Wave 3.1 (CHAOS-3299) recorder methods ------------------------------
    # Each of these persists one bounded, validated artifact of the server-
    # owned intent -> resolution -> plan -> investigation -> frame ->
    # narrative lifecycle. All are called from PersistenceRunRecorder on the
    # live (non-replay) run branch only, before the terminal update_run
    # transition -- the same ordering `append_tool_call` already relies on
    # for 0-day ephemeral retention to cascade through fully-written rows
    # rather than orphaning a write after the conversation is already purged.

    async def record_intent(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        intent_id: str,
        cardinality: str,
        requires_clarification: bool,
        interpreter_version: str,
        payload: Mapping[str, Any],
    ) -> DevRunIntent:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if intent_id not in _QUESTION_INTENT_IDS:
            raise DevPersistenceValidationError("invalid intent_id")
        if cardinality not in _CARDINALITIES:
            raise DevPersistenceValidationError("invalid cardinality")
        record = DevRunIntent(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            intent_id=intent_id,
            cardinality=cardinality,
            requires_clarification=requires_clarification,
            interpreter_version=_safe_token(
                interpreter_version, field="interpreter_version", max_bytes=128
            )
            or "",
            payload=_bounded_json(
                payload, field="intent_payload", max_bytes=_INTENT_PAYLOAD_MAX_BYTES
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def append_resolution(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        entry_ordinal: int,
        mention_id: uuid.UUID,
        outcome: str,
        resolved_at: datetime,
        payload: Mapping[str, Any],
    ) -> DevRunResolution:
        """Append one immutable entity-resolution ledger entry.

        INSERT-only by construction: no update method for an existing
        ``(run_id, entry_ordinal)`` row is exposed, and a second insert for
        an already-used ordinal fails via the unique constraint rather than
        upserting.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if entry_ordinal < 0 or entry_ordinal > 99:
            raise DevPersistenceValidationError("resolution entry_ordinal out of range")
        if outcome not in _RESOLUTION_OUTCOMES:
            raise DevPersistenceValidationError("invalid resolution outcome")
        record = DevRunResolution(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            entry_ordinal=entry_ordinal,
            mention_id=mention_id,
            outcome=outcome,
            payload=_bounded_json(
                payload,
                field="resolution_payload",
                max_bytes=_RESOLUTION_PAYLOAD_MAX_BYTES,
            ),
            resolved_at=resolved_at,
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_subject_set(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        set_id: uuid.UUID,
        entity_kind: str,
        cohort_complete: bool,
        fingerprint: str,
        payload: Mapping[str, Any],
    ) -> DevRunSubjectSet:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if entity_kind not in _ENTITY_KINDS:
            raise DevPersistenceValidationError("invalid entity_kind")
        record = DevRunSubjectSet(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            set_id=set_id,
            entity_kind=entity_kind,
            cohort_complete=cohort_complete,
            fingerprint=_safe_token(fingerprint, field="fingerprint", max_bytes=128)
            or "",
            payload=_bounded_json(
                payload,
                field="subject_set_payload",
                max_bytes=_SUBJECT_SET_PAYLOAD_MAX_BYTES,
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def append_source_observation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        observation_id: uuid.UUID,
        source_class: str,
        requirement_level: str,
        observed_state: str,
        data_semantics: str,
        usable_fact_count: int,
        sample_count: int | None,
        subject_coverage: float,
        observed_at: datetime,
        payload: Mapping[str, Any],
    ) -> DevRunSourceObservation:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 24:
            raise DevPersistenceValidationError(
                "source observation ordinal out of range"
            )
        if source_class not in _SOURCE_CLASSES:
            raise DevPersistenceValidationError("invalid source_class")
        if requirement_level not in _REQUIREMENT_LEVELS:
            raise DevPersistenceValidationError("invalid requirement_level")
        if observed_state not in _SOURCE_OBSERVED_STATES:
            raise DevPersistenceValidationError("invalid observed_state")
        if data_semantics not in _DATA_SEMANTICS:
            raise DevPersistenceValidationError("invalid data_semantics")
        if usable_fact_count < 0:
            raise DevPersistenceValidationError("usable_fact_count must be nonnegative")
        if sample_count is not None and sample_count < 0:
            raise DevPersistenceValidationError("sample_count must be nonnegative")
        if not (0.0 <= subject_coverage <= 1.0):
            raise DevPersistenceValidationError("subject_coverage must be in [0, 1]")
        record = DevRunSourceObservation(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            observation_id=observation_id,
            source_class=source_class,
            requirement_level=requirement_level,
            observed_state=observed_state,
            data_semantics=data_semantics,
            usable_fact_count=usable_fact_count,
            sample_count=sample_count,
            subject_coverage=subject_coverage,
            payload=_bounded_json(
                payload,
                field="source_observation_payload",
                max_bytes=_SOURCE_OBSERVATION_PAYLOAD_MAX_BYTES,
            ),
            observed_at=observed_at,
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_investigation_result(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        result_id: uuid.UUID,
        relationship_closure_verified: bool,
        completed_at: datetime,
        payload: Mapping[str, Any],
    ) -> DevRunInvestigationResult:
        """Persist the ``dev_investigation_result.v1`` wrapper for one run.

        Reconciliation delta: not one of the 7 tables in the
        pre-implementation plan, added because the landed contract has this
        as a distinct top-level object (completed/skipped/failed plan steps
        plus relationship-closure verification) that the per-observation
        ``dev_run_source_observations`` rows alone cannot reconstruct.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        record = DevRunInvestigationResult(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            result_id=result_id,
            relationship_closure_verified=relationship_closure_verified,
            payload=_bounded_json(
                payload,
                field="investigation_result_payload",
                max_bytes=_INVESTIGATION_RESULT_PAYLOAD_MAX_BYTES,
            ),
            completed_at=completed_at,
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_frame(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        frame_id: uuid.UUID,
        public_outcome: str,
        payload: Mapping[str, Any],
    ) -> DevAnswerFrame:
        """Persist the canonical ``dev_answer_frame.v1`` for one run.

        One row per terminal run regardless of public outcome -- including
        ``needs_clarification``/``not_found``/``unsupported``/``denied``,
        confirmed by the landed no-answer field policy (see
        ``DevRunInvestigationResult`` and the frame model docstring).
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if public_outcome not in _PUBLIC_OUTCOMES:
            raise DevPersistenceValidationError("invalid public_outcome")
        record = DevAnswerFrame(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            frame_id=frame_id,
            public_outcome=public_outcome,
            payload=_bounded_json(
                payload, field="frame_payload", max_bytes=_FRAME_PAYLOAD_MAX_BYTES
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_narrative(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        narrative_id: uuid.UUID,
        frame_id: uuid.UUID,
        mode: str,
        provider_fingerprint: str | None,
        narrative_text: str,
        payload: Mapping[str, Any],
    ) -> DevRunNarrative:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if mode not in _NARRATIVE_MODES:
            raise DevPersistenceValidationError("invalid narrative mode")
        frame = await self.session.scalar(
            select(DevAnswerFrame).where(
                DevAnswerFrame.run_id == run.id,
                DevAnswerFrame.org_id == org_id,
                DevAnswerFrame.user_id == user_id,
            )
        )
        if frame is None or frame.frame_id != frame_id:
            raise DevPersistenceValidationError(
                "narrative frame_id must match the run's recorded answer frame"
            )
        text = _bounded_text(
            narrative_text, field="narrative_text", max_bytes=_NARRATIVE_TEXT_MAX_BYTES
        )
        if not text:
            raise DevPersistenceValidationError("narrative_text must not be empty")
        record = DevRunNarrative(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            narrative_id=narrative_id,
            frame_id=frame_id,
            mode=mode,
            provider_fingerprint=_digest(
                provider_fingerprint, field="provider_fingerprint"
            ),
            narrative_text=text,
            payload=_bounded_json(
                payload,
                field="narrative_payload",
                max_bytes=_NARRATIVE_PAYLOAD_MAX_BYTES,
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def append_stage_diagnostic(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        stage_id: str,
        status: str,
        latency_ms: int | None,
        counts: Mapping[str, Any],
    ) -> DevRunStageDiagnostic:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 9:
            raise DevPersistenceValidationError("stage diagnostic ordinal out of range")
        if stage_id not in _STAGE_IDS:
            raise DevPersistenceValidationError("invalid stage_id")
        if status not in _STAGE_STATUSES:
            raise DevPersistenceValidationError("invalid stage status")
        if latency_ms is not None and latency_ms < 0:
            raise DevPersistenceValidationError("latency_ms must be nonnegative")
        record = DevRunStageDiagnostic(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            stage_id=stage_id,
            status=status,
            latency_ms=latency_ms,
            counts=_safe_count_summary(counts, field="stage_diagnostic_counts"),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

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

    async def _lock_admission_scope(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> None:
        """Serialize admission in one fixed organization/user/conversation order."""

        locked_org = await self.session.scalar(
            select(Organization.id).where(Organization.id == org_id).with_for_update()
        )
        locked_user = await self.session.scalar(
            select(User.id).where(User.id == user_id).with_for_update()
        )
        locked_conversation = await self.session.scalar(
            select(DevConversation.id)
            .where(
                DevConversation.id == conversation_id,
                DevConversation.org_id == org_id,
                DevConversation.user_id == user_id,
                DevConversation.deleted_at.is_(None),
            )
            .with_for_update()
        )
        if locked_org is None or locked_user is None or locked_conversation is None:
            raise DevPersistenceNotFound("admission scope not found")

    async def _enforce_admission_limits(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limits: DevAdmissionLimits,
    ) -> None:
        now = self._now()
        active = DevRun.state.not_in(_TERMINAL_RUN_STATES)
        active_user = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                active,
            )
        )
        active_org = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                active,
            )
        )
        if (
            int(active_user or 0) >= limits.active_runs_per_user
            or int(active_org or 0) >= limits.active_runs_per_org
        ):
            raise DevConcurrencyLimitExceeded("Ask Dev active-run limit reached")

        user_window = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                DevRun.started_at >= now - timedelta(minutes=15),
            )
        )
        org_window = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.started_at >= now - timedelta(hours=1),
            )
        )
        if (
            int(user_window or 0) >= limits.requests_per_user_per_15_minutes
            or int(org_window or 0) >= limits.requests_per_org_per_hour
        ):
            raise DevRateLimitExceeded("Ask Dev request-rate limit reached")

    async def _enforce_platform_allowance(
        self,
        *,
        org_id: uuid.UUID,
        allowance: DevPlatformAllowance,
    ) -> None:
        now = self._now()
        window_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        if now.month == 12:
            reset_at = datetime(now.year + 1, 1, 1, tzinfo=UTC)
        else:
            reset_at = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
        terminal_with_cost = and_(
            DevRun.state.in_(_TERMINAL_RUN_STATES),
            DevRun.estimated_cost_microusd.is_not(None),
        )
        charged_cost = case(
            (terminal_with_cost, DevRun.estimated_cost_microusd),
            else_=allowance.per_run_reservation_microusd,
        )
        statement = select(
            func.count(DevRun.id), func.coalesce(func.sum(charged_cost), 0)
        ).where(
            DevRun.org_id == org_id,
            DevRun.provider_source == "platform",
            DevRun.started_at >= window_start,
            DevRun.started_at < reset_at,
        )
        request_count, charged_microusd = (await self.session.execute(statement)).one()
        if int(request_count or 0) >= allowance.monthly_request_limit:
            raise DevMonthlyRequestLimitExceeded(reset_at)
        if (
            int(charged_microusd or 0) + allowance.per_run_reservation_microusd
            > allowance.monthly_cost_limit_microusd
        ):
            raise DevMonthlyCostLimitExceeded(reset_at)

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
