"""Add durable completion fences for ordered worker handoffs.

Revision ID: 0063
Revises: 0062
"""

from __future__ import annotations

from alembic import op

revision: str = "0063"
down_revision: str | None = "0062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE worker_job_completion_fences (
            completion_key text PRIMARY KEY
                CHECK (
                    length(completion_key) BETWEEN 1 AND 256
                    AND completion_key ~ '^[a-z][a-z0-9_]{0,95}:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                ),
            completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
        )
        """
    )
    op.execute(
        """
        ALTER TABLE worker_job_outbox
        ADD COLUMN prerequisite_completion_key text NULL
            CHECK (
                prerequisite_completion_key IS NULL
                OR (
                    length(prerequisite_completion_key) BETWEEN 1 AND 256
                    AND prerequisite_completion_key ~ '^[a-z][a-z0-9_]{0,95}:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                )
            )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_worker_job_outbox_prerequisite
        ON worker_job_outbox (prerequisite_completion_key)
        WHERE prerequisite_completion_key IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_worker_job_outbox_prerequisite")
    op.execute(
        "ALTER TABLE worker_job_outbox DROP COLUMN IF EXISTS prerequisite_completion_key"
    )
    op.execute("DROP TABLE IF EXISTS worker_job_completion_fences")
