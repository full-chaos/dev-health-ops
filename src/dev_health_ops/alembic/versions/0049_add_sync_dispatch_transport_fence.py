"""Add the database-backed sync dispatch transport ownership fence.

Revision ID: 0049
Revises: 0048
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0049"
down_revision: str | None = "0048"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KINDS = (
    "dispatch_sync_run",
    "finalize_sync_run",
    "post_sync",
    "reference_discovery",
)


def upgrade() -> None:
    """Seed Celery ownership and bind outbox claims to a route generation."""
    routes = op.create_table(
        "sync_dispatch_transport_routes",
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("transport", sa.String(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rollback_transport", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "kind IN ('dispatch_sync_run', 'finalize_sync_run', "
            "'post_sync', 'reference_discovery')",
            name="ck_sync_dispatch_transport_routes_kind",
        ),
        sa.CheckConstraint(
            "transport IN ('celery', 'river')",
            name="ck_sync_dispatch_transport_routes_transport",
        ),
        sa.CheckConstraint(
            "rollback_transport = 'celery'",
            name="ck_sync_dispatch_transport_routes_rollback",
        ),
        sa.CheckConstraint(
            "generation >= 1",
            name="ck_sync_dispatch_transport_routes_generation",
        ),
        sa.CheckConstraint(
            "(paused AND paused_at IS NOT NULL) OR (NOT paused AND paused_at IS NULL)",
            name="ck_sync_dispatch_transport_routes_pause_timestamp",
        ),
        sa.PrimaryKeyConstraint("kind"),
    )
    now = datetime.now(UTC)
    op.bulk_insert(
        routes,
        [
            {
                "kind": kind,
                "transport": "celery",
                "generation": 1,
                "paused": False,
                "paused_at": None,
                "rollback_transport": "celery",
                "created_at": now,
                "updated_at": now,
            }
            for kind in _KINDS
        ],
    )

    op.add_column(
        "sync_dispatch_outbox",
        sa.Column("claim_transport", sa.Text(), nullable=True),
    )
    op.add_column(
        "sync_dispatch_outbox",
        sa.Column("claim_route_generation", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "sync_dispatch_outbox",
        sa.Column("dispatched_transport", sa.Text(), nullable=True),
    )
    op.add_column(
        "sync_dispatch_outbox",
        sa.Column("dispatched_route_generation", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "sync_dispatch_outbox",
        sa.Column("transport_job_id", sa.Text(), nullable=True),
    )

    # A worker holding a pre-migration Celery lease may finish safely after the
    # deploy. Expired leases remain bound so their next reclaim can overwrite
    # the pair atomically.
    op.execute(
        sa.text(
            """
            UPDATE sync_dispatch_outbox
            SET claim_transport = 'celery',
                claim_route_generation = 1
            WHERE claim_token IS NOT NULL
              AND claim_expires_at IS NOT NULL
            """
        )
    )
    op.execute(
        """
        UPDATE sync_dispatch_outbox
        SET claim_token = NULL,
            claim_expires_at = NULL,
            claim_transport = NULL,
            claim_route_generation = NULL
        WHERE (claim_token IS NULL) <> (claim_expires_at IS NULL)
        """
    )
    op.execute(
        """
        UPDATE sync_dispatch_outbox
        SET dispatched_transport = 'celery',
            dispatched_route_generation = 1
        WHERE status = 'dispatched'
          AND last_error IS DISTINCT FROM 'feature_disabled'
        """
    )

    # Route ownership changes must invalidate older claims. Prevent accidental
    # generation regression and require a generation bump for every material
    # route-state change.
    op.execute(
        """
        CREATE FUNCTION enforce_sync_dispatch_route_generation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.generation < OLD.generation THEN
                RAISE EXCEPTION 'sync dispatch route generation cannot decrease';
            END IF;
            IF (
                NEW.transport IS DISTINCT FROM OLD.transport
                OR NEW.paused IS DISTINCT FROM OLD.paused
                OR NEW.paused_at IS DISTINCT FROM OLD.paused_at
                OR NEW.rollback_transport IS DISTINCT FROM OLD.rollback_transport
            ) AND NEW.generation <= OLD.generation THEN
                RAISE EXCEPTION
                    'sync dispatch route state change requires generation increase';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_sync_dispatch_route_generation
        BEFORE UPDATE ON sync_dispatch_transport_routes
        FOR EACH ROW
        EXECUTE FUNCTION enforce_sync_dispatch_route_generation()
        """
    )
    op.execute(
        """
        CREATE FUNCTION enforce_sync_dispatch_outbox_route_fence()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            active_transport text;
            active_generation bigint;
        BEGIN
            IF (NEW.claim_token IS NULL) <> (NEW.claim_expires_at IS NULL) THEN
                RAISE EXCEPTION
                    'sync dispatch claim token and expiry must change together';
            END IF;

            IF NEW.claim_token IS NOT NULL
               AND (
                   NEW.claim_transport IS NULL
                   OR NEW.claim_route_generation IS NULL
               ) THEN
                SELECT transport, generation
                INTO active_transport, active_generation
                FROM sync_dispatch_transport_routes
                WHERE kind = NEW.kind
                  AND transport = 'celery'
                  AND paused = FALSE;
                IF NOT FOUND THEN
                    RAISE EXCEPTION
                        'sync dispatch kind has no active celery route';
                END IF;
                NEW.claim_transport := active_transport;
                NEW.claim_route_generation := active_generation;
            END IF;

            IF NEW.status = 'dispatched'
               AND NEW.last_error IS DISTINCT FROM 'feature_disabled' THEN
                NEW.dispatched_transport := COALESCE(
                    NEW.dispatched_transport,
                    NEW.claim_transport,
                    OLD.claim_transport
                );
                NEW.dispatched_route_generation := COALESCE(
                    NEW.dispatched_route_generation,
                    NEW.claim_route_generation,
                    OLD.claim_route_generation
                );
            ELSE
                NEW.dispatched_transport := NULL;
                NEW.dispatched_route_generation := NULL;
                NEW.transport_job_id := NULL;
            END IF;

            IF NEW.claim_token IS NULL THEN
                NEW.claim_transport := NULL;
                NEW.claim_route_generation := NULL;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_sync_dispatch_outbox_route_fence
        BEFORE INSERT OR UPDATE ON sync_dispatch_outbox
        FOR EACH ROW
        EXECUTE FUNCTION enforce_sync_dispatch_outbox_route_fence()
        """
    )
    op.create_check_constraint(
        "ck_sync_dispatch_outbox_claim_route_coherence",
        "sync_dispatch_outbox",
        "("
        "claim_token IS NULL AND claim_expires_at IS NULL "
        "AND claim_transport IS NULL AND claim_route_generation IS NULL"
        ") OR ("
        "claim_token IS NOT NULL AND claim_expires_at IS NOT NULL "
        "AND claim_transport IS NOT NULL AND claim_route_generation IS NOT NULL"
        ")",
    )
    op.create_check_constraint(
        "ck_sync_dispatch_outbox_dispatched_route_coherence",
        "sync_dispatch_outbox",
        "("
        "status = 'dispatched' AND ("
        "("
        "last_error = 'feature_disabled' AND dispatched_transport IS NULL "
        "AND dispatched_route_generation IS NULL AND transport_job_id IS NULL"
        ") OR ("
        "(last_error IS NULL OR last_error <> 'feature_disabled') AND "
        "dispatched_transport IS NOT NULL "
        "AND dispatched_route_generation IS NOT NULL"
        ")"
        ")"
        ") OR ("
        "status <> 'dispatched' AND dispatched_transport IS NULL "
        "AND dispatched_route_generation IS NULL AND transport_job_id IS NULL"
        ")",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_sync_dispatch_outbox_dispatched_route_coherence",
        "sync_dispatch_outbox",
        type_="check",
    )
    op.drop_constraint(
        "ck_sync_dispatch_outbox_claim_route_coherence",
        "sync_dispatch_outbox",
        type_="check",
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_sync_dispatch_outbox_route_fence "
        "ON sync_dispatch_outbox"
    )
    op.execute("DROP FUNCTION IF EXISTS enforce_sync_dispatch_outbox_route_fence()")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_sync_dispatch_route_generation "
        "ON sync_dispatch_transport_routes"
    )
    op.execute("DROP FUNCTION IF EXISTS enforce_sync_dispatch_route_generation()")
    for column in (
        "transport_job_id",
        "dispatched_route_generation",
        "dispatched_transport",
        "claim_route_generation",
        "claim_transport",
    ):
        op.drop_column("sync_dispatch_outbox", column)
    op.drop_table("sync_dispatch_transport_routes")
