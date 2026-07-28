"""Add Ask Dev conversation, run, feedback, retention, and audit persistence.

Revision ID: 0068
Revises: 0067
Create Date: 2026-07-28 00:00:00

The migration is additive. Operational rollback disables Ask Dev and leaves
these tables dormant; downgrade exists for isolated pre-release rehearsal only.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0068"
down_revision: str | None = "0067"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "dev_conversations",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("title", sa.String(length=200), nullable=True),
        sa.Column("current_scope", _JSON, nullable=False),
        sa.Column("retention_days", sa.SmallInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "retention_days IN (0, 30)", name="ck_dev_conversations_retention_days"
        ),
        sa.CheckConstraint(
            "title IS NULL OR length(title) <= 200",
            name="ck_dev_conversations_title_length",
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id", "org_id", "user_id", name="uq_dev_conversations_owner_identity"
        ),
    )
    op.create_index(
        "ix_dev_conversations_owner_updated",
        "dev_conversations",
        ["org_id", "user_id", sa.text("updated_at DESC")],
    )
    op.create_index(
        "ix_dev_conversations_expiry",
        "dev_conversations",
        ["expires_at"],
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )

    op.create_table(
        "dev_messages",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("conversation_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("client_message_id", _UUID, nullable=True),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("content", sa.Text(), nullable=True),
        sa.Column("answer_id", _UUID, nullable=True),
        sa.Column("answer_payload", _JSON, nullable=True),
        sa.Column("scope_snapshot", _JSON, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "role IN ('user', 'assistant')", name="ck_dev_messages_role"
        ),
        sa.CheckConstraint(
            "(role = 'user' AND client_message_id IS NOT NULL "
            "AND content IS NOT NULL AND answer_id IS NULL AND answer_payload IS NULL) "
            "OR (role = 'assistant' AND client_message_id IS NULL "
            "AND answer_id IS NOT NULL AND answer_payload IS NOT NULL)",
            name="ck_dev_messages_role_payload",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id", "org_id", "user_id"],
            [
                "dev_conversations.id",
                "dev_conversations.org_id",
                "dev_conversations.user_id",
            ],
            name="fk_dev_messages_conversation_owner",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "conversation_id",
            "client_message_id",
            name="uq_dev_messages_conversation_client_message",
        ),
        sa.UniqueConstraint("answer_id", name="uq_dev_messages_answer_id"),
        sa.UniqueConstraint(
            "answer_id", "org_id", "user_id", name="uq_dev_messages_answer_owner"
        ),
        sa.UniqueConstraint(
            "id", "org_id", "user_id", name="uq_dev_messages_owner_identity"
        ),
    )
    op.create_index(
        "ix_dev_messages_owner_conversation_created",
        "dev_messages",
        ["org_id", "user_id", "conversation_id", "created_at"],
    )

    op.create_table(
        "dev_runs",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("request_id", _UUID, nullable=False),
        sa.Column("conversation_id", _UUID, nullable=False),
        sa.Column("user_message_id", _UUID, nullable=True),
        sa.Column("retry_of_run_id", _UUID, nullable=True),
        sa.Column("answer_id", _UUID, nullable=True),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("terminal_reason", sa.String(length=64), nullable=True),
        sa.Column("provider_source", sa.String(length=16), nullable=True),
        sa.Column("provider_fingerprint", sa.String(length=71), nullable=True),
        sa.Column("model_fingerprint", sa.String(length=71), nullable=True),
        sa.Column("prompt_version", sa.String(length=128), nullable=True),
        sa.Column("tool_contract_version", sa.String(length=128), nullable=True),
        sa.Column("metric_version", sa.String(length=128), nullable=True),
        sa.Column("query_version", sa.String(length=128), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("estimated_cost_microusd", sa.BigInteger(), nullable=True),
        sa.Column("tool_call_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("citation_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metric_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("grounding_validation_status", sa.String(length=32), nullable=True),
        sa.Column("safe_error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "state IN ('accepted', 'resolving_scope', 'model_decision', "
            "'tool_validation', 'tool_execution', 'answer_validation', "
            "'completed', 'insufficient_evidence', 'refused', 'failed', 'cancelled')",
            name="ck_dev_runs_state",
        ),
        sa.CheckConstraint(
            "provider_source IS NULL OR provider_source IN ('platform', 'byo')",
            name="ck_dev_runs_provider_source",
        ),
        sa.CheckConstraint(
            "provider_fingerprint IS NULL OR "
            "(length(provider_fingerprint) = 71 "
            "AND provider_fingerprint LIKE 'sha256:%')",
            name="ck_dev_runs_provider_fingerprint",
        ),
        sa.CheckConstraint(
            "model_fingerprint IS NULL OR "
            "(length(model_fingerprint) = 71 "
            "AND model_fingerprint LIKE 'sha256:%')",
            name="ck_dev_runs_model_fingerprint",
        ),
        sa.CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name="ck_dev_runs_latency_nonnegative",
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_dev_runs_input_tokens_nonnegative",
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_dev_runs_output_tokens_nonnegative",
        ),
        sa.CheckConstraint(
            "estimated_cost_microusd IS NULL OR estimated_cost_microusd >= 0",
            name="ck_dev_runs_cost_nonnegative",
        ),
        sa.CheckConstraint(
            "tool_call_count >= 0 AND citation_count >= 0 AND metric_count >= 0",
            name="ck_dev_runs_counts_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id", "org_id", "user_id"],
            [
                "dev_conversations.id",
                "dev_conversations.org_id",
                "dev_conversations.user_id",
            ],
            name="fk_dev_runs_conversation_owner",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_message_id", "org_id", "user_id"],
            ["dev_messages.id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_runs_user_message_owner",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["answer_id", "org_id", "user_id"],
            ["dev_messages.answer_id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_runs_answer_owner",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["retry_of_run_id"], ["dev_runs.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_id", name="uq_dev_runs_request_id"),
        sa.UniqueConstraint("user_message_id", name="uq_dev_runs_user_message_id"),
        sa.UniqueConstraint(
            "id", "org_id", "user_id", name="uq_dev_runs_owner_identity"
        ),
    )
    op.create_index(
        "ix_dev_runs_owner_conversation_started",
        "dev_runs",
        ["org_id", "user_id", "conversation_id", "started_at"],
    )
    op.create_index("ix_dev_runs_state_started", "dev_runs", ["state", "started_at"])

    op.create_table(
        "dev_tool_calls",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("conversation_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("ordinal", sa.SmallInteger(), nullable=False),
        sa.Column("tool_id", sa.String(length=96), nullable=False),
        sa.Column("tool_version", sa.String(length=64), nullable=False),
        sa.Column("canonical_input_hash", sa.String(length=71), nullable=False),
        sa.Column("safe_scope_summary", _JSON, nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("item_count", sa.Integer(), nullable=True),
        sa.Column("byte_count", sa.Integer(), nullable=True),
        sa.Column("result_digest", sa.String(length=71), nullable=True),
        sa.Column("evidence_ref_ids", _JSON, nullable=False),
        sa.Column("safe_error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("ordinal >= 0", name="ck_dev_tool_calls_ordinal"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_dev_tool_calls_status",
        ),
        sa.CheckConstraint(
            "length(canonical_input_hash) = 71 "
            "AND canonical_input_hash LIKE 'sha256:%'",
            name="ck_dev_tool_calls_input_hash",
        ),
        sa.CheckConstraint(
            "result_digest IS NULL OR "
            "(length(result_digest) = 71 AND result_digest LIKE 'sha256:%')",
            name="ck_dev_tool_calls_result_digest",
        ),
        sa.CheckConstraint(
            "(latency_ms IS NULL OR latency_ms >= 0) "
            "AND (row_count IS NULL OR row_count >= 0) "
            "AND (item_count IS NULL OR item_count >= 0) "
            "AND (byte_count IS NULL OR byte_count >= 0)",
            name="ck_dev_tool_calls_counts_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_tool_calls_run_owner",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "ordinal", name="uq_dev_tool_calls_run_ordinal"),
    )
    op.create_index(
        "ix_dev_tool_calls_owner_run",
        "dev_tool_calls",
        ["org_id", "user_id", "run_id", "ordinal"],
    )

    op.create_table(
        "dev_feedback",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("answer_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("rating", sa.String(length=16), nullable=False),
        sa.Column("reasons", _JSON, nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "rating IN ('helpful', 'not_helpful')", name="ck_dev_feedback_rating"
        ),
        sa.CheckConstraint(
            "comment IS NULL OR length(comment) <= 2048",
            name="ck_dev_feedback_comment_length",
        ),
        sa.ForeignKeyConstraint(
            ["answer_id", "org_id", "user_id"],
            ["dev_messages.answer_id", "dev_messages.org_id", "dev_messages.user_id"],
            name="fk_dev_feedback_answer_owner",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id", "user_id", "answer_id", name="uq_dev_feedback_user_answer"
        ),
    )
    op.create_index(
        "ix_dev_feedback_owner_answer",
        "dev_feedback",
        ["org_id", "user_id", "answer_id"],
    )

    # No foreign keys by design: tombstones must survive user/org content
    # deletion and contain only opaque identifiers plus lifecycle metadata.
    op.create_table(
        "dev_conversation_tombstones",
        sa.Column("id", _UUID, nullable=False),
        sa.Column("conversation_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
        sa.Column("actor_user_id", _UUID, nullable=True),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("retention_days", sa.SmallInteger(), nullable=False),
        sa.Column(
            "conversation_created_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "reason IN ('user_deleted', 'admin_purged', 'retention_expired', "
            "'ephemeral_completed')",
            name="ck_dev_conversation_tombstones_reason",
        ),
        sa.CheckConstraint(
            "retention_days IN (0, 30)",
            name="ck_dev_conversation_tombstones_retention_days",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "conversation_id", name="uq_dev_conversation_tombstones_conversation"
        ),
    )
    op.create_index(
        "ix_dev_conversation_tombstones_owner_deleted",
        "dev_conversation_tombstones",
        ["org_id", "user_id", "deleted_at"],
    )


def downgrade() -> None:
    op.drop_table("dev_conversation_tombstones")
    op.drop_table("dev_feedback")
    op.drop_table("dev_tool_calls")
    op.drop_table("dev_runs")
    op.drop_table("dev_messages")
    op.drop_table("dev_conversations")
