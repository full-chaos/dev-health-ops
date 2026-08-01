"""Operational persistence for the Ask Dev conversation lifecycle.

These tables intentionally contain only validated product artifacts and bounded
audit metadata. Provider payloads, prompts, source records, credentials, and
private model reasoning do not belong in this module.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base

DEV_RETENTION_DAYS = frozenset({0, 30})


def _utc_now() -> datetime:
    return datetime.now(UTC)


class DevConversation(Base):
    """One user-owned Ask Dev conversation in an organization."""

    __tablename__ = "dev_conversations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    title: Mapped[str | None] = mapped_column(String(200), nullable=True)
    current_scope: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    retention_days: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=30
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "retention_days IN (0, 30)", name="ck_dev_conversations_retention_days"
        ),
        CheckConstraint(
            "title IS NULL OR length(title) <= 200",
            name="ck_dev_conversations_title_length",
        ),
        Index(
            "ix_dev_conversations_owner_updated",
            "org_id",
            "user_id",
            "updated_at",
        ),
        UniqueConstraint(
            "id", "org_id", "user_id", name="uq_dev_conversations_owner_identity"
        ),
        Index(
            "ix_dev_conversations_expiry",
            "expires_at",
            postgresql_where=text("expires_at IS NOT NULL"),
        ),
    )


class DevMessage(Base):
    """A user question or validated assistant answer artifact."""

    __tablename__ = "dev_messages"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    client_message_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    answer_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    answer_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    scope_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint("role IN ('user', 'assistant')", name="ck_dev_messages_role"),
        CheckConstraint(
            "(role = 'user' AND client_message_id IS NOT NULL "
            "AND content IS NOT NULL AND answer_id IS NULL AND answer_payload IS NULL) "
            "OR (role = 'assistant' AND client_message_id IS NULL "
            "AND answer_id IS NOT NULL AND answer_payload IS NOT NULL)",
            name="ck_dev_messages_role_payload",
        ),
        UniqueConstraint(
            "conversation_id",
            "client_message_id",
            name="uq_dev_messages_conversation_client_message",
        ),
        UniqueConstraint("answer_id", name="uq_dev_messages_answer_id"),
        UniqueConstraint(
            "answer_id",
            "org_id",
            "user_id",
            name="uq_dev_messages_answer_owner",
        ),
        UniqueConstraint(
            "id", "org_id", "user_id", name="uq_dev_messages_owner_identity"
        ),
        ForeignKeyConstraint(
            ["conversation_id", "org_id", "user_id"],
            [
                "dev_conversations.id",
                "dev_conversations.org_id",
                "dev_conversations.user_id",
            ],
            name="fk_dev_messages_conversation_owner",
            ondelete="CASCADE",
        ),
        Index(
            "ix_dev_messages_owner_conversation_created",
            "org_id",
            "user_id",
            "conversation_id",
            "created_at",
        ),
    )


class DevRun(Base):
    """Safe, content-free audit metadata for one accepted user submission."""

    __tablename__ = "dev_runs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    request_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False, unique=True)
    conversation_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_message_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID, nullable=True, unique=True
    )
    retry_of_run_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("dev_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    answer_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="accepted")
    terminal_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_source: Mapped[str | None] = mapped_column(String(16), nullable=True)
    provider_fingerprint: Mapped[str | None] = mapped_column(String(71), nullable=True)
    model_fingerprint: Mapped[str | None] = mapped_column(String(71), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tool_contract_version: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    metric_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    query_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_cost_microusd: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    tool_call_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    citation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metric_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    grounding_validation_status: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    safe_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # CHAOS-3292 content-free run diagnostics. Both are members of closed
    # server-owned vocabularies (never question or entity text): the subject
    # preflight's outcome, and the legacy CHAOS-3289 backstop's reason code
    # when it fires on a run the preflight already governed — which TRD §10
    # classes as a cutover defect worth alerting on rather than acting on.
    preflight_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    legacy_guard_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "state IN ('accepted', 'resolving_scope', 'interpreting', "
            "'resolving_subjects', 'model_decision', "
            "'tool_validation', 'tool_execution', 'answer_validation', "
            "'completed', 'insufficient_evidence', 'refused', 'failed', 'cancelled')",
            name="ck_dev_runs_state",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "org_id", "user_id"],
            [
                "dev_conversations.id",
                "dev_conversations.org_id",
                "dev_conversations.user_id",
            ],
            name="fk_dev_runs_conversation_owner",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["user_message_id", "org_id", "user_id"],
            ["dev_messages.id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_runs_user_message_owner",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["answer_id", "org_id", "user_id"],
            ["dev_messages.answer_id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_runs_answer_owner",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "org_id", "user_id", name="uq_dev_runs_owner_identity"),
        CheckConstraint(
            "provider_source IS NULL OR provider_source IN ('platform', 'byo')",
            name="ck_dev_runs_provider_source",
        ),
        CheckConstraint(
            "provider_fingerprint IS NULL OR "
            "(length(provider_fingerprint) = 71 "
            "AND provider_fingerprint LIKE 'sha256:%')",
            name="ck_dev_runs_provider_fingerprint",
        ),
        CheckConstraint(
            "model_fingerprint IS NULL OR "
            "(length(model_fingerprint) = 71 "
            "AND model_fingerprint LIKE 'sha256:%')",
            name="ck_dev_runs_model_fingerprint",
        ),
        CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name="ck_dev_runs_latency_nonnegative",
        ),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_dev_runs_input_tokens_nonnegative",
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_dev_runs_output_tokens_nonnegative",
        ),
        CheckConstraint(
            "estimated_cost_microusd IS NULL OR estimated_cost_microusd >= 0",
            name="ck_dev_runs_cost_nonnegative",
        ),
        CheckConstraint(
            "tool_call_count >= 0 AND citation_count >= 0 AND metric_count >= 0",
            name="ck_dev_runs_counts_nonnegative",
        ),
        Index(
            "ix_dev_runs_owner_conversation_started",
            "org_id",
            "user_id",
            "conversation_id",
            "started_at",
        ),
        Index("ix_dev_runs_owner_started", "org_id", "user_id", "started_at"),
        Index("ix_dev_runs_org_started", "org_id", "started_at"),
        Index("ix_dev_runs_state_started", "state", "started_at"),
    )


class DevToolCall(Base):
    """Bounded tool-call audit metadata without inputs or source results."""

    __tablename__ = "dev_tool_calls"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    conversation_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    ordinal: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    tool_id: Mapped[str] = mapped_column(String(96), nullable=False)
    tool_version: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_input_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    safe_scope_summary: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    item_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    byte_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_digest: Mapped[str | None] = mapped_column(String(71), nullable=True)
    evidence_ref_ids: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list
    )
    safe_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        CheckConstraint("ordinal >= 0", name="ck_dev_tool_calls_ordinal"),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_dev_tool_calls_status",
        ),
        CheckConstraint(
            "length(canonical_input_hash) = 71 "
            "AND canonical_input_hash LIKE 'sha256:%'",
            name="ck_dev_tool_calls_input_hash",
        ),
        CheckConstraint(
            "result_digest IS NULL OR "
            "(length(result_digest) = 71 AND result_digest LIKE 'sha256:%')",
            name="ck_dev_tool_calls_result_digest",
        ),
        CheckConstraint(
            "(latency_ms IS NULL OR latency_ms >= 0) "
            "AND (row_count IS NULL OR row_count >= 0) "
            "AND (item_count IS NULL OR item_count >= 0) "
            "AND (byte_count IS NULL OR byte_count >= 0)",
            name="ck_dev_tool_calls_counts_nonnegative",
        ),
        UniqueConstraint("run_id", "ordinal", name="uq_dev_tool_calls_run_ordinal"),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_tool_calls_run_owner",
            ondelete="CASCADE",
        ),
        Index(
            "ix_dev_tool_calls_owner_run",
            "org_id",
            "user_id",
            "run_id",
            "ordinal",
        ),
    )


class DevFeedback(Base):
    """User feedback on one persisted validated answer."""

    __tablename__ = "dev_feedback"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    answer_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    rating: Mapped[str] = mapped_column(String(16), nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "rating IN ('helpful', 'not_helpful')", name="ck_dev_feedback_rating"
        ),
        CheckConstraint(
            "comment IS NULL OR length(comment) <= 2048",
            name="ck_dev_feedback_comment_length",
        ),
        UniqueConstraint(
            "org_id", "user_id", "answer_id", name="uq_dev_feedback_user_answer"
        ),
        ForeignKeyConstraint(
            ["answer_id", "org_id", "user_id"],
            ["dev_messages.answer_id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_feedback_answer_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_feedback_owner_answer", "org_id", "user_id", "answer_id"),
    )


class DevConversationTombstone(Base):
    """Minimal audit proof that conversation content was removed."""

    __tablename__ = "dev_conversation_tombstones"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        GUID, nullable=False, unique=True
    )
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    retention_days: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    conversation_created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    deleted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "reason IN ('user_deleted', 'admin_purged', 'retention_expired', "
            "'ephemeral_completed')",
            name="ck_dev_conversation_tombstones_reason",
        ),
        CheckConstraint(
            "retention_days IN (0, 30)",
            name="ck_dev_conversation_tombstones_retention_days",
        ),
        Index(
            "ix_dev_conversation_tombstones_owner_deleted",
            "org_id",
            "user_id",
            "deleted_at",
        ),
    )
