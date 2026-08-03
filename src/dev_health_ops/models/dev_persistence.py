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
    DDL,
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
    event,
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
    # -- Wave 3.1 (CHAOS-3299) additive columns -----------------------------
    # contract_generation defaults 'v1' at the DB level so every pre-existing
    # row is correctly marked legacy the instant the column exists -- no
    # backfill UPDATE required.
    contract_generation: Mapped[str] = mapped_column(
        String(4), nullable=False, default="v1"
    )
    # Orthogonal to `state` (the internal admission/lifecycle FSM above):
    # public_outcome is the TRD v2 §10 dev_answer.v2 outcome vocabulary, set
    # once at terminal transition. Widening `state`/`ck_dev_runs_state` to the
    # §10 orchestrator stage names is out of this ticket's scope.
    public_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    compatibility_projection_version: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    plan_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    plan_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # Folded (orchestrator decision, CHAOS-3299): a dedicated
    # dev_run_investigation_results table would have duplicated what's
    # already owned elsewhere -- observations are persisted 1:N in
    # dev_run_source_observations, and dev_answer_frames is the replay
    # source of truth. The only post-terminal dev_investigation_result.v1
    # facts nothing else can reconstruct are which plan steps ran (a step
    # can be skipped without ever producing an observation) and whether
    # relationship closure was verified, so those two facts are persisted
    # directly here instead of a ninth table. NULL for v1 runs and for v2
    # runs before the investigation stage completes.
    plan_step_partition: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    relationship_closure_verified: Mapped[bool | None] = mapped_column(nullable=True)
    # CHAOS-3297 (0078): reserved for stack #4's narrative-fallback work.
    # NULL on every run stack #1 produces -- no narrative synthesis exists
    # yet. No CHECK constraint at this revision (see 0078's docstring);
    # narrative_mode's closed vocabulary already exists
    # (ck_dev_run_narratives_mode) but nothing here writes a value until a
    # narrative pipeline does.
    narrative_mode: Mapped[str | None] = mapped_column(String(24), nullable=True)
    narrative_failure_code: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    # CHAOS-3297 (0079): the exact validated v1 dev_error.v1 payload
    # (code/safe_message/retryable/remediation/limit_reset_at) a terminal
    # error carried, persisted verbatim so an idempotent replay can reuse it
    # byte-for-byte instead of reconstructing an approximation from the
    # frame (Codex review HIGH #1). NULL for every run that predates this
    # column and for every non-error terminal (`answer_id` is set instead).
    terminal_error_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "state IN ('accepted', 'resolving_scope', 'interpreting', "
            "'resolving_subjects', 'model_decision', "
            "'tool_validation', 'tool_execution', 'answer_validation', "
            "'completed', 'insufficient_evidence', 'refused', 'failed', 'cancelled')",
            name="ck_dev_runs_state",
        ),
        CheckConstraint(
            "contract_generation IN ('v1', 'v2')",
            name="ck_dev_runs_contract_generation",
        ),
        CheckConstraint(
            "public_outcome IS NULL OR public_outcome IN ('answered', "
            "'answered_with_gaps', 'needs_clarification', 'not_found', "
            "'temporarily_unavailable', 'unsupported', 'denied', 'failed')",
            name="ck_dev_runs_public_outcome",
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


class DevRunIntent(Base):
    """Wave 3.1 (CHAOS-3299): the authoritative, server-owned interpretation.

    One row per run (``dev_question_intent.v1``). ``payload`` is the full
    validated contract dump (bounded, sensitive-key-checked at the service
    layer); the columns below duplicate only the fields that need CHECK
    discipline or direct querying.
    """

    __tablename__ = "dev_run_intents"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    intent_id: Mapped[str] = mapped_column(String(48), nullable=False)
    cardinality: Mapped[str] = mapped_column(String(24), nullable=False)
    requires_clarification: Mapped[bool] = mapped_column(nullable=False)
    interpreter_version: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "intent_id IN ('entity_status', 'portfolio_status', 'remaining_work', "
            "'observed_change', 'registered_statistics', 'metric_comparison', "
            "'data_trust', 'project_health', 'team_health', "
            "'team_workload_balance', 'operational_deficiency_inventory', "
            "'bounded_investigation')",
            name="ck_dev_run_intents_intent_id",
        ),
        CheckConstraint(
            "cardinality IN ('singular', 'plural_cohort', 'organization_wide')",
            name="ck_dev_run_intents_cardinality",
        ),
        UniqueConstraint("run_id", name="uq_dev_run_intents_run"),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_intents_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_run_intents_owner_run", "org_id", "user_id", "run_id"),
    )


class DevRunResolution(Base):
    """Wave 3.1: one append-only entity-resolution ledger entry per run.

    ``dev_resolution_ledger.v1``'s ``entries`` are contiguous, strictly
    increasing, and never rewritten -- the service exposes no UPDATE path for
    an existing row, only INSERT of the next ``entry_ordinal``.
    """

    __tablename__ = "dev_run_resolutions"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    entry_ordinal: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    mention_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    resolved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "entry_ordinal >= 0 AND entry_ordinal <= 99",
            name="ck_dev_run_resolutions_entry_ordinal",
        ),
        CheckConstraint(
            "outcome IN ('exact_match', 'ambiguous_candidates', "
            "'no_authorized_match', 'catalog_unavailable', 'unsupported_kind')",
            name="ck_dev_run_resolutions_outcome",
        ),
        UniqueConstraint(
            "run_id", "entry_ordinal", name="uq_dev_run_resolutions_run_ordinal"
        ),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_resolutions_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_run_resolutions_owner_run", "org_id", "user_id", "run_id"),
    )


class DevRunSubjectSet(Base):
    """Wave 3.1: the committed plural/cohort subject set for one run.

    ``dev_subject_set.v1``. Present only for plural/cohort intents (0..1 per
    run).
    """

    __tablename__ = "dev_run_subject_sets"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    set_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    entity_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    cohort_complete: Mapped[bool] = mapped_column(nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "entity_kind IN ('repository', 'project', 'work_unit', 'issue', "
            "'pull_request', 'team')",
            name="ck_dev_run_subject_sets_entity_kind",
        ),
        UniqueConstraint("run_id", name="uq_dev_run_subject_sets_run"),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_subject_sets_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_run_subject_sets_owner_run", "org_id", "user_id", "run_id"),
    )


class DevRunSourceObservation(Base):
    """Wave 3.1: one ``dev_source_observation.v1`` deterministic-plan-step row.

    Kept separate from ``dev_tool_calls`` (model-issued tool-call audit)
    deliberately -- this is the deterministic source-adapter analogue with a
    different CHECK shape. One row per observation, ordinal-keyed.
    """

    __tablename__ = "dev_run_source_observations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    ordinal: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    observation_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    source_class: Mapped[str] = mapped_column(String(32), nullable=False)
    requirement_level: Mapped[str] = mapped_column(String(16), nullable=False)
    observed_state: Mapped[str] = mapped_column(String(32), nullable=False)
    data_semantics: Mapped[str] = mapped_column(String(16), nullable=False)
    usable_fact_count: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    subject_coverage: Mapped[float] = mapped_column(nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "ordinal >= 0 AND ordinal <= 24",
            name="ck_dev_run_source_observations_ordinal",
        ),
        CheckConstraint(
            "source_class IN ('status_change', 'work_item', 'work_graph', "
            "'pull_request', 'code_change', 'review', 'ci_run', 'test_report', "
            "'deployment', 'incident', 'operational_control', 'source_health', "
            "'health_profile', 'deficiency_inventory')",
            name="ck_dev_run_source_observations_source_class",
        ),
        CheckConstraint(
            "requirement_level IN ('mandatory', 'conditional', 'optional', "
            "'not_applicable')",
            name="ck_dev_run_source_observations_requirement_level",
        ),
        CheckConstraint(
            "observed_state IN ('available_current', 'available_stale', "
            "'available_unknown', 'unconfigured', 'unavailable', "
            "'unauthorized_or_not_visible', 'not_applicable', 'truncated')",
            name="ck_dev_run_source_observations_observed_state",
        ),
        CheckConstraint(
            "data_semantics IN ('measured_zero', 'no_data', 'not_measured')",
            name="ck_dev_run_source_observations_data_semantics",
        ),
        CheckConstraint(
            "usable_fact_count >= 0 AND (sample_count IS NULL OR sample_count >= 0)",
            name="ck_dev_run_source_observations_counts_nonnegative",
        ),
        CheckConstraint(
            "subject_coverage >= 0 AND subject_coverage <= 1",
            name="ck_dev_run_source_observations_coverage_range",
        ),
        UniqueConstraint(
            "run_id", "ordinal", name="uq_dev_run_source_observations_run_ordinal"
        ),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_source_observations_run_owner",
            ondelete="CASCADE",
        ),
        Index(
            "ix_dev_run_source_observations_owner_run", "org_id", "user_id", "run_id"
        ),
    )


class DevAnswerFrame(Base):
    """Wave 3.1: the canonical, server-owned ``dev_answer_frame.v1``.

    One row per terminal run, regardless of public outcome -- a
    ``needs_clarification``/``not_found``/``unsupported``/``denied`` run still
    produces a (minimal) frame, confirmed by the landed no-answer field
    policy (``NO_ANSWER_FRAME_FIELD_POLICY`` classifies every frame field,
    including ``frame_id``/``run_id`` as IDENTIFIER-shaped, rather than
    omitting the frame entirely). See ``DevRun.plan_step_partition`` for the
    related investigation-result step-completion bookkeeping.
    """

    __tablename__ = "dev_answer_frames"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    frame_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    public_outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "public_outcome IN ('answered', 'answered_with_gaps', "
            "'needs_clarification', 'not_found', 'temporarily_unavailable', "
            "'unsupported', 'denied', 'failed')",
            name="ck_dev_answer_frames_public_outcome",
        ),
        UniqueConstraint("run_id", name="uq_dev_answer_frames_run"),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_answer_frames_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_answer_frames_owner_run", "org_id", "user_id", "run_id"),
    )


class DevRunNarrative(Base):
    """Wave 3.1: the optional ``dev_narrative.v1`` presentation text for one run.

    0..1 per run, present only when narrative_mode != 'none'. Text is stored
    verbatim (not a digest) because the contract guarantees it is
    presentation-only text mapped to existing frame section/fact IDs -- it
    cannot introduce new facts, numbers, or subjects (TRD v2 §4.5), so it is
    not raw provider output, prompt, or chain-of-thought.
    """

    __tablename__ = "dev_run_narratives"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    narrative_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    frame_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    mode: Mapped[str] = mapped_column(String(24), nullable=False)
    provider_fingerprint: Mapped[str | None] = mapped_column(String(71), nullable=True)
    narrative_text: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "mode IN ('provider', 'deterministic_fallback')",
            name="ck_dev_run_narratives_mode",
        ),
        CheckConstraint(
            "provider_fingerprint IS NULL OR "
            "(length(provider_fingerprint) = 71 "
            "AND provider_fingerprint LIKE 'sha256:%')",
            name="ck_dev_run_narratives_provider_fingerprint",
        ),
        UniqueConstraint("run_id", name="uq_dev_run_narratives_run"),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_narratives_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_run_narratives_owner_run", "org_id", "user_id", "run_id"),
    )


# -- DB-enforced payload invariant (CHAOS-3297 Codex review round 9).
#
# Three rounds of session-level guards (an AST scanner, then
# before_insert/before_update mapper events, then a do_orm_execute
# session-level hook covering every Core DML shape SQLAlchemy 2.x has)
# were each defeated by a write shape the previous one could not see --
# most recently a Core-table UPDATE issued through the Session (where
# `bind_mapper` is None, since the statement targets a bare `Table`, not
# a mapped class the listener can look up) and an
# `INSERT ... ON CONFLICT DO UPDATE` whose conflict `SET` clause is never
# inspected by anything that validates the INSERT values. Codex's
# conclusion, and the standing line here: stop extending a parser over
# SQLAlchemy's statement shapes. The invariant moves to the one boundary
# every one of those paths -- ORM, Core, executemany, upsert, even a raw
# connection -- must cross to become a row: the database itself.
#
# A BEFORE INSERT/UPDATE trigger on each payload-bearing table validates
# the FINAL row about to be written, whatever produced it:
#   * payload IS NOT NULL
#   * payload->>'schema_version' equals the table's one contract version
#   * payload's own identity fields equal this row's own columns --
#     frame_id/run_id/public_outcome (dev_answer_frames), narrative_id/
#     run_id/frame_id/mode (dev_run_narratives) -- mirroring
#     record_frame's/record_narrative's own cross-checks.
#
# Scope note: `provider_fingerprint` is NOT cross-checked at the trigger
# level. record_narrative's own cross-check compares a SHA-256 digest of
# `payload.provider_metadata.model_fingerprint` against the column, and
# computing that digest inside a trigger needs an extension neither
# engine has installed by default (pgcrypto on Postgres; no built-in
# SQLite equivalent at all) -- a new production dependency this round
# was not asked to take on. The session listener
# (`persistence/service.py::_validate_run_narrative_payload`) still
# performs that specific check on every session-mediated write, which is
# every write this application's own code ever issues; it is a residual,
# documented gap only for a write that goes around the session AND
# forges a self-consistent-but-wrong provider_fingerprint, which is
# strictly narrower than either of Codex's round-9 repros.
#
# This does not replace full contract validation (every field
# DevAnswerFrameContract/DevNarrativeContract enforce beyond identity) --
# that remains the session listener's job, now correctly described as
# fast-fail UX (a caller going through the ORM gets a clean
# DevPersistenceValidationError before ever reaching the database), not
# the load-bearing guard. The trigger is unconditional and total; the
# listener is a nicer error message for the 99% of writes that go
# through it.
#
# Registered via `event.listen(<table>, "after_create", DDL(...)
# .execute_if(dialect=...))` so `Base.metadata.create_all` -- what every
# unit test's in-memory SQLite fixture already calls -- installs the
# SQLite arm automatically. The Postgres arm additionally ships as
# Alembic migration 0080, since production databases are never built via
# `create_all`.

_DEV_ANSWER_FRAME_SCHEMA_VERSION = "dev_answer_frame.v1"
_DEV_NARRATIVE_SCHEMA_VERSION = "dev_narrative.v1"

DEV_ANSWER_FRAMES_TRIGGER_FUNCTION_POSTGRESQL = f"""
CREATE OR REPLACE FUNCTION dev_answer_frames_validate_payload() RETURNS trigger AS $$
BEGIN
    IF NEW.payload IS NULL THEN
        RAISE EXCEPTION 'dev_answer_frames.payload must not be null' USING ERRCODE = '23514';
    END IF;
    IF NEW.payload->>'schema_version' IS DISTINCT FROM '{_DEV_ANSWER_FRAME_SCHEMA_VERSION}' THEN
        RAISE EXCEPTION 'dev_answer_frames.payload.schema_version does not equal {_DEV_ANSWER_FRAME_SCHEMA_VERSION}' USING ERRCODE = '23514';
    END IF;
    IF (NEW.payload->>'frame_id')::uuid IS DISTINCT FROM NEW.frame_id THEN
        RAISE EXCEPTION 'dev_answer_frames.payload.frame_id does not match the row''s frame_id' USING ERRCODE = '23514';
    END IF;
    IF (NEW.payload->>'run_id')::uuid IS DISTINCT FROM NEW.run_id THEN
        RAISE EXCEPTION 'dev_answer_frames.payload.run_id does not match the row''s run_id' USING ERRCODE = '23514';
    END IF;
    IF NEW.payload->>'public_outcome' IS DISTINCT FROM NEW.public_outcome THEN
        RAISE EXCEPTION 'dev_answer_frames.payload.public_outcome does not match the row''s public_outcome' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

#: asyncpg (the async Postgres driver this project uses) prepares each
#: statement individually and rejects multiple SQL commands in one
#: `execute()` call ("cannot insert multiple commands into a prepared
#: statement") -- confirmed empirically against a live Postgres database.
#: The DROP and CREATE must be two separate DDL objects (and, further
#: down, two separate `event.listen` registrations), never one string.
DEV_ANSWER_FRAMES_TRIGGER_DROP_POSTGRESQL = """
DROP TRIGGER IF EXISTS dev_answer_frames_validate_payload_trigger ON dev_answer_frames;
"""

DEV_ANSWER_FRAMES_TRIGGER_CREATE_POSTGRESQL = """
CREATE TRIGGER dev_answer_frames_validate_payload_trigger
    BEFORE INSERT OR UPDATE ON dev_answer_frames
    FOR EACH ROW EXECUTE FUNCTION dev_answer_frames_validate_payload();
"""

#: SQLite has no functions/procedures -- the same WHEN predicate, negated,
#: is duplicated onto one INSERT trigger and one UPDATE trigger (SQLite
#: triggers fire on exactly one event each; there is no "INSERT OR
#: UPDATE"). `IS NOT` is NULL-safe (SQLite's `IS DISTINCT FROM`
#: equivalent) -- a payload missing a key extracts NULL and correctly
#: reads as a mismatch, not an error. GUID columns are stored as
#: undashed lowercase hex (see `GUID.process_bind_param`); the JSON
#: payload's own copy is the dashed ServerHandle form, so the extracted
#: value has its dashes stripped before comparing.
#:
#: CHAOS-3297 Codex review round 10 MEDIUM: `json_extract` reads the
#: FIRST occurrence of a duplicate JSON object key; Python's `json`
#: decoder (what the application actually validates against) and
#: Postgres's `->>` operator on `json`/`jsonb` both keep the LAST --
#: confirmed empirically. A payload engineered with a matching
#: schema_version/frame_id/run_id/public_outcome first and a duplicate,
#: mismatched copy of one of them after would read as valid to this
#: trigger (checking the first copy) while the application (loading the
#: last copy) sees something else, and Postgres would reject the same
#: bytes outright -- a real dialect divergence, not merely a style
#: difference. Rejecting outright, not attempting to replicate
#: last-occurrence semantics in SQL: a well-formed payload from the real
#: contract encoder can never contain a duplicate key at all (JSON
#: objects are Python dicts on the way out; a dict has no duplicate
#: keys), so this can only ever fire on an already-adversarial payload --
#: there is no legitimate write this could reject. `json_each` -- unlike
#: `json_extract` -- enumerates every occurrence of a repeated key, so
#: `GROUP BY key HAVING COUNT(*) > 1` reliably detects one; `json_each`
#: of a NULL payload yields zero rows (verified), so this is safe to OR
#: in unconditionally rather than needing to be gated behind the
#: `NEW.payload IS NULL` check above it.
#:
#: CHAOS-3297 Codex review round 11 HIGH: round 10's fix was itself
#: incomplete -- confirmed empirically -- because `json_extract`'s PATH
#: matching (`'$.frame_id'`) truncates an object label at an embedded
#: NUL (U+0000) the same way a C string does, while `json_each`'s own
#: `key` column (and exact string equality, `key = 'frame_id'`) does NOT
#: truncate: `json_extract('{"frame_id\x00XXXX": "a", "frame_id": "b"}',
#: '$.frame_id')` returns `'a'` (the NUL-aliased key's value), while
#: `(SELECT value FROM json_each(...) WHERE key = 'frame_id')` correctly
#: returns `'b'` (the real key's value) -- matching what Python's `json`
#: decoder and Postgres's `->>` both see, since neither truncates at NUL
#: either. A payload with a NUL-suffixed alias of a protected key first
#: (carrying a matching value) and the real, exact key after (carrying a
#: mismatched value) passed both the round-10 value cross-checks (which
#: still used `json_extract` path matching, reading the aliased match)
#: AND the round-10 duplicate-key count (which correctly does NOT see
#: two occurrences of the SAME key, since `'frame_id\x00XXXX' != 'frame_id'`
#: under exact equality -- these are genuinely different key names, not a
#: duplicate). Two changes close this:
#:  1) every value extraction below is now `(SELECT value FROM
#:     json_each(NEW.payload) WHERE key = '<name>')` -- exact-key
#:     equality via json_each, never json_extract path syntax, for both
#:     the row-binding cross-checks and (already true since round 10)
#:     the duplicate-key count. A payload with a genuine duplicate of the
#:     SAME exact key still gets one arbitrary row back from this scalar
#:     subquery (verified: no error), but that case is independently
#:     caught by the duplicate-key EXISTS clause below regardless of
#:     which row the extraction subquery would have picked.
#:  2) defense in depth: ANY top-level key containing a NUL byte is
#:     rejected outright, whether or not it happens to alias a protected
#:     key this round -- closing the *class* of NUL-truncation quirks,
#:     not just this one path. `instr(key, char(0)) > 0` correctly
#:     detects an embedded NUL in `json_each`'s own `key` column (unlike
#:     `length(key)`, which -- confirmed empirically -- ALSO truncates at
#:     NUL the same way `json_extract` does); scoped to keys only
#:     (verified a NUL inside a *value* does not false-positive here).
DEV_ANSWER_FRAMES_TRIGGER_SQLITE = f"""
CREATE TRIGGER IF NOT EXISTS dev_answer_frames_validate_payload_insert
BEFORE INSERT ON dev_answer_frames
FOR EACH ROW
WHEN NEW.payload IS NULL
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'schema_version')
        IS NOT '{_DEV_ANSWER_FRAME_SCHEMA_VERSION}'
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'frame_id'), '-', ''
    ) IS NOT NEW.frame_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'run_id'), '-', ''
    ) IS NOT NEW.run_id
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'public_outcome')
        IS NOT NEW.public_outcome
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload)
        WHERE key IN ('schema_version', 'frame_id', 'run_id', 'public_outcome')
        GROUP BY key
        HAVING COUNT(*) > 1
    )
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload) WHERE instr(key, char(0)) > 0
    )
BEGIN
    SELECT RAISE(ABORT, 'dev_answer_frames.payload invalid or does not match the row');
END;
"""

DEV_ANSWER_FRAMES_TRIGGER_SQLITE_UPDATE = f"""
CREATE TRIGGER IF NOT EXISTS dev_answer_frames_validate_payload_update
BEFORE UPDATE ON dev_answer_frames
FOR EACH ROW
WHEN NEW.payload IS NULL
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'schema_version')
        IS NOT '{_DEV_ANSWER_FRAME_SCHEMA_VERSION}'
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'frame_id'), '-', ''
    ) IS NOT NEW.frame_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'run_id'), '-', ''
    ) IS NOT NEW.run_id
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'public_outcome')
        IS NOT NEW.public_outcome
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload)
        WHERE key IN ('schema_version', 'frame_id', 'run_id', 'public_outcome')
        GROUP BY key
        HAVING COUNT(*) > 1
    )
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload) WHERE instr(key, char(0)) > 0
    )
BEGIN
    SELECT RAISE(ABORT, 'dev_answer_frames.payload invalid or does not match the row');
END;
"""

DEV_RUN_NARRATIVES_TRIGGER_FUNCTION_POSTGRESQL = f"""
CREATE OR REPLACE FUNCTION dev_run_narratives_validate_payload() RETURNS trigger AS $$
BEGIN
    IF NEW.payload IS NULL THEN
        RAISE EXCEPTION 'dev_run_narratives.payload must not be null' USING ERRCODE = '23514';
    END IF;
    IF NEW.payload->>'schema_version' IS DISTINCT FROM '{_DEV_NARRATIVE_SCHEMA_VERSION}' THEN
        RAISE EXCEPTION 'dev_run_narratives.payload.schema_version does not equal {_DEV_NARRATIVE_SCHEMA_VERSION}' USING ERRCODE = '23514';
    END IF;
    IF (NEW.payload->>'narrative_id')::uuid IS DISTINCT FROM NEW.narrative_id THEN
        RAISE EXCEPTION 'dev_run_narratives.payload.narrative_id does not match the row''s narrative_id' USING ERRCODE = '23514';
    END IF;
    IF (NEW.payload->>'run_id')::uuid IS DISTINCT FROM NEW.run_id THEN
        RAISE EXCEPTION 'dev_run_narratives.payload.run_id does not match the row''s run_id' USING ERRCODE = '23514';
    END IF;
    IF (NEW.payload->>'frame_id')::uuid IS DISTINCT FROM NEW.frame_id THEN
        RAISE EXCEPTION 'dev_run_narratives.payload.frame_id does not match the row''s frame_id' USING ERRCODE = '23514';
    END IF;
    IF NEW.payload->>'mode' IS DISTINCT FROM NEW.mode THEN
        RAISE EXCEPTION 'dev_run_narratives.payload.mode does not match the row''s mode' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

DEV_RUN_NARRATIVES_TRIGGER_DROP_POSTGRESQL = """
DROP TRIGGER IF EXISTS dev_run_narratives_validate_payload_trigger ON dev_run_narratives;
"""

DEV_RUN_NARRATIVES_TRIGGER_CREATE_POSTGRESQL = """
CREATE TRIGGER dev_run_narratives_validate_payload_trigger
    BEFORE INSERT OR UPDATE ON dev_run_narratives
    FOR EACH ROW EXECUTE FUNCTION dev_run_narratives_validate_payload();
"""

#: See the identical round-10/round-11 comment above
#: `DEV_ANSWER_FRAMES_TRIGGER_SQLITE` -- same duplicate-protected-key
#: rejection and same NUL-truncation-safe exact-key extraction, this
#: table's own protected key set.
DEV_RUN_NARRATIVES_TRIGGER_SQLITE = f"""
CREATE TRIGGER IF NOT EXISTS dev_run_narratives_validate_payload_insert
BEFORE INSERT ON dev_run_narratives
FOR EACH ROW
WHEN NEW.payload IS NULL
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'schema_version')
        IS NOT '{_DEV_NARRATIVE_SCHEMA_VERSION}'
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'narrative_id'), '-', ''
    ) IS NOT NEW.narrative_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'run_id'), '-', ''
    ) IS NOT NEW.run_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'frame_id'), '-', ''
    ) IS NOT NEW.frame_id
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'mode') IS NOT NEW.mode
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload)
        WHERE key IN ('schema_version', 'narrative_id', 'run_id', 'frame_id', 'mode')
        GROUP BY key
        HAVING COUNT(*) > 1
    )
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload) WHERE instr(key, char(0)) > 0
    )
BEGIN
    SELECT RAISE(ABORT, 'dev_run_narratives.payload invalid or does not match the row');
END;
"""

DEV_RUN_NARRATIVES_TRIGGER_SQLITE_UPDATE = f"""
CREATE TRIGGER IF NOT EXISTS dev_run_narratives_validate_payload_update
BEFORE UPDATE ON dev_run_narratives
FOR EACH ROW
WHEN NEW.payload IS NULL
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'schema_version')
        IS NOT '{_DEV_NARRATIVE_SCHEMA_VERSION}'
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'narrative_id'), '-', ''
    ) IS NOT NEW.narrative_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'run_id'), '-', ''
    ) IS NOT NEW.run_id
    OR REPLACE(
        (SELECT value FROM json_each(NEW.payload) WHERE key = 'frame_id'), '-', ''
    ) IS NOT NEW.frame_id
    OR (SELECT value FROM json_each(NEW.payload) WHERE key = 'mode') IS NOT NEW.mode
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload)
        WHERE key IN ('schema_version', 'narrative_id', 'run_id', 'frame_id', 'mode')
        GROUP BY key
        HAVING COUNT(*) > 1
    )
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.payload) WHERE instr(key, char(0)) > 0
    )
BEGIN
    SELECT RAISE(ABORT, 'dev_run_narratives.payload invalid or does not match the row');
END;
"""

event.listen(
    DevAnswerFrame.__table__,
    "after_create",
    DDL(DEV_ANSWER_FRAMES_TRIGGER_FUNCTION_POSTGRESQL).execute_if(dialect="postgresql"),
)
event.listen(
    DevAnswerFrame.__table__,
    "after_create",
    DDL(DEV_ANSWER_FRAMES_TRIGGER_DROP_POSTGRESQL).execute_if(dialect="postgresql"),
)
event.listen(
    DevAnswerFrame.__table__,
    "after_create",
    DDL(DEV_ANSWER_FRAMES_TRIGGER_CREATE_POSTGRESQL).execute_if(dialect="postgresql"),
)
event.listen(
    DevAnswerFrame.__table__,
    "after_create",
    DDL(DEV_ANSWER_FRAMES_TRIGGER_SQLITE).execute_if(dialect="sqlite"),
)
event.listen(
    DevAnswerFrame.__table__,
    "after_create",
    DDL(DEV_ANSWER_FRAMES_TRIGGER_SQLITE_UPDATE).execute_if(dialect="sqlite"),
)
event.listen(
    DevRunNarrative.__table__,
    "after_create",
    DDL(DEV_RUN_NARRATIVES_TRIGGER_FUNCTION_POSTGRESQL).execute_if(
        dialect="postgresql"
    ),
)
event.listen(
    DevRunNarrative.__table__,
    "after_create",
    DDL(DEV_RUN_NARRATIVES_TRIGGER_DROP_POSTGRESQL).execute_if(dialect="postgresql"),
)
event.listen(
    DevRunNarrative.__table__,
    "after_create",
    DDL(DEV_RUN_NARRATIVES_TRIGGER_CREATE_POSTGRESQL).execute_if(dialect="postgresql"),
)
event.listen(
    DevRunNarrative.__table__,
    "after_create",
    DDL(DEV_RUN_NARRATIVES_TRIGGER_SQLITE).execute_if(dialect="sqlite"),
)
event.listen(
    DevRunNarrative.__table__,
    "after_create",
    DDL(DEV_RUN_NARRATIVES_TRIGGER_SQLITE_UPDATE).execute_if(dialect="sqlite"),
)


class DevRunStageDiagnostic(Base):
    """Wave 3.1: content-free per-stage timing/count/status diagnostics.

    One row per orchestrator stage (TRD v2 §10:
    interpreting/resolving_subjects/planning/collecting/synthesizing_frame/
    narrating_optional/projecting_answer) per run. ``counts`` reuses the
    existing ``_safe_count_summary`` bounded-keys/nonnegative discipline
    already built for ``dev_tool_calls.safe_scope_summary``.
    """

    __tablename__ = "dev_run_stage_diagnostics"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    ordinal: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    stage_id: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    counts: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "ordinal >= 0 AND ordinal <= 9",
            name="ck_dev_run_stage_diagnostics_ordinal",
        ),
        CheckConstraint(
            "stage_id IN ('interpreting', 'resolving_subjects', 'planning', "
            "'collecting', 'synthesizing_frame', 'narrating_optional', "
            "'projecting_answer')",
            name="ck_dev_run_stage_diagnostics_stage_id",
        ),
        CheckConstraint(
            "status IN ('started', 'completed', 'failed', 'skipped')",
            name="ck_dev_run_stage_diagnostics_status",
        ),
        CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name="ck_dev_run_stage_diagnostics_latency_nonnegative",
        ),
        UniqueConstraint(
            "run_id", "ordinal", name="uq_dev_run_stage_diagnostics_run_ordinal"
        ),
        ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_stage_diagnostics_run_owner",
            ondelete="CASCADE",
        ),
        Index("ix_dev_run_stage_diagnostics_owner_run", "org_id", "user_id", "run_id"),
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
