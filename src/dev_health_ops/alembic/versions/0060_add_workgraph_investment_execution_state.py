"""Add fenced work-graph and investment execution requests.

Revision ID: 0060
Revises: 0059
"""

from __future__ import annotations

from alembic import op

revision: str = "0060"
down_revision: str | None = "0059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE work_graph_execution_requests (
            id uuid PRIMARY KEY,
            org_id uuid NOT NULL,
            kind text NOT NULL CHECK (kind IN (
                'workgraph.build', 'investment.materialize', 'investment.dispatch',
                'investment.chunk', 'investment.finalize'
            )),
            scope jsonb NOT NULL,
            model_ref text NULL CHECK (model_ref IS NULL OR length(model_ref) <= 128),
            prompt_ref text NULL CHECK (prompt_ref IS NULL OR length(prompt_ref) <= 128),
            llm_concurrency integer NOT NULL CHECK (llm_concurrency BETWEEN 1 AND 16),
            spend_limit_microunits bigint NOT NULL CHECK (spend_limit_microunits >= 0),
            correlation_id text NOT NULL CHECK (length(correlation_id) BETWEEN 1 AND 128),
            idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 256),
            state text NOT NULL DEFAULT 'pending' CHECK (state IN (
                'pending', 'running', 'succeeded', 'failed', 'ambiguous', 'canceled'
            )),
            claim_token uuid NULL,
            lease_expires_at timestamptz NULL,
            attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CHECK ((state = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
                OR (state <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE work_graph_execution_ledger (
            request_id uuid PRIMARY KEY REFERENCES work_graph_execution_requests(id) ON DELETE CASCADE,
            claim_token uuid NOT NULL,
            state text NOT NULL CHECK (state IN (
                'executing', 'succeeded', 'failed', 'ambiguous', 'repaired'
            )),
            attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
            output_evidence jsonb NULL,
            failure_detail text NULL CHECK (failure_detail IS NULL OR length(failure_detail) BETWEEN 1 AND 1024),
            last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            completed_at timestamptz NULL,
            CHECK ((state = 'succeeded' AND completed_at IS NOT NULL AND output_evidence IS NOT NULL)
                OR (state <> 'succeeded' AND completed_at IS NULL))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE work_graph_execution_repairs (
            id uuid PRIMARY KEY,
            request_id uuid NOT NULL REFERENCES work_graph_execution_requests(id) ON DELETE CASCADE,
            expected_attempt_count integer NOT NULL CHECK (expected_attempt_count >= 1),
            resolution text NOT NULL CHECK (resolution IN ('retry_safe', 'confirm_succeeded')),
            review_evidence text NOT NULL CHECK (length(review_evidence) BETWEEN 1 AND 2048),
            output_evidence jsonb NULL,
            created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            UNIQUE (request_id, expected_attempt_count, resolution),
            CHECK ((resolution = 'confirm_succeeded' AND output_evidence IS NOT NULL)
                OR (resolution = 'retry_safe' AND output_evidence IS NULL))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_work_graph_execution_claim
        ON work_graph_execution_requests (kind, state, lease_expires_at)
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION forbid_work_graph_terminal_mutation()
        RETURNS trigger AS $$
        BEGIN
            IF OLD.state IN ('succeeded', 'failed', 'canceled') THEN
                RAISE EXCEPTION 'terminal work graph execution request is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER work_graph_execution_terminal_immutable
        BEFORE UPDATE ON work_graph_execution_requests
        FOR EACH ROW EXECUTE FUNCTION forbid_work_graph_terminal_mutation()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS work_graph_execution_terminal_immutable ON work_graph_execution_requests"
    )
    op.execute("DROP FUNCTION IF EXISTS forbid_work_graph_terminal_mutation")
    op.execute("DROP TABLE IF EXISTS work_graph_execution_repairs")
    op.execute("DROP TABLE IF EXISTS work_graph_execution_ledger")
    op.execute("DROP TABLE IF EXISTS work_graph_execution_requests")
