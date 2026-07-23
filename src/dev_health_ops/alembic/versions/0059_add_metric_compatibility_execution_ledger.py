"""Add the metric compatibility execution ledger.

Revision ID: 0059
Revises: 0058
"""

from __future__ import annotations

from alembic import op

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE metric_compatibility_executions (
            id uuid PRIMARY KEY,
            worker_kind text NOT NULL
                CHECK (worker_kind IN ('daily', 'remaining')),
            operation text NOT NULL
                CHECK (operation IN ('partition', 'finalize')),
            run_id uuid NOT NULL,
            partition_id uuid NULL,
            family text NOT NULL CHECK (length(family) BETWEEN 1 AND 64),
            generation text NOT NULL CHECK (length(generation) BETWEEN 1 AND 128),
            scope_digest text NOT NULL CHECK (
                length(scope_digest) = 64
                AND scope_digest ~ '^[0-9a-f]{64}$'
            ),
            claim_token uuid NOT NULL,
            state text NOT NULL
                CHECK (state IN (
                    'executing', 'succeeded', 'ambiguous', 'retry_authorized'
                )),
            attempt_count integer NOT NULL DEFAULT 1
                CHECK (attempt_count >= 1),
            output_evidence jsonb NULL,
            failure_detail text NULL CHECK (
                failure_detail IS NULL OR length(failure_detail) BETWEEN 1 AND 1024
            ),
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            completed_at timestamptz NULL,
            CHECK (
                (operation = 'partition' AND partition_id IS NOT NULL)
                OR
                (operation = 'finalize' AND partition_id IS NULL)
            ),
            CHECK (
                (state = 'succeeded' AND completed_at IS NOT NULL
                    AND output_evidence IS NOT NULL)
                OR
                (state <> 'succeeded' AND completed_at IS NULL)
            )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE metric_compatibility_execution_repairs (
            id uuid PRIMARY KEY,
            execution_id uuid NOT NULL
                REFERENCES metric_compatibility_executions(id) ON DELETE CASCADE,
            expected_state text NOT NULL
                CHECK (expected_state IN ('executing', 'ambiguous')),
            expected_attempt_count integer NOT NULL
                CHECK (expected_attempt_count >= 1),
            resolution text NOT NULL
                CHECK (resolution IN ('retry_safe', 'confirm_succeeded')),
            review_evidence text NOT NULL
                CHECK (length(review_evidence) BETWEEN 1 AND 2048),
            output_evidence jsonb NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CHECK (
                (resolution = 'confirm_succeeded' AND output_evidence IS NOT NULL)
                OR
                (resolution = 'retry_safe' AND output_evidence IS NULL)
            ),
            UNIQUE (
                execution_id, expected_state, expected_attempt_count, resolution
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_metric_compatibility_executions_run
        ON metric_compatibility_executions
            (worker_kind, run_id, partition_id, operation)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS metric_compatibility_execution_repairs")
    op.execute("DROP TABLE IF EXISTS metric_compatibility_executions")
