"""Durable rate-limit observation store (CHAOS-2742 / CHAOS-2758).

Persists one normalized row per provider rate-limit event that defers a sync
unit, so a later consumer (cross-unit cooldown gating, CHAOS-2760) can consult
recent provider/integration/route-family cooldowns without re-discovering the
limit against the provider itself.

Postgres, not ClickHouse -- deliberately. Every durable store the dispatch
path already consults is Postgres: ``SyncDispatchOutbox``
(``models/integrations.py``), the per-unit rate-limit deferral columns on
``sync_run_units`` (migration 0022), ``SyncComputeCheckpoint``
(``models/checkpoints.py``), and ``BudgetGuard``'s reservation itself runs
inside a Postgres advisory-lock transaction (``sync/budget_guard.py``). A
future cooldown-gating consumer (CHAOS-2760) needs to read this table from
inside that same advisory-lock transaction, which only Postgres supports here
-- ClickHouse is a separate analytics cluster with no transactional join to
the dispatch guard. See ``docs/providers/rate-limit-policy.md`` "Observation
store" section for the full justification.

Only normalized fields are persisted -- never raw provider headers (leak /
bloat risk). Header capture stays best-effort and provider-local; this table
never carries them.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class ProviderRateLimitObservation(Base):
    """One row per rate-limit observation that deferred a sync unit.

    Written atomically with the deferring unit's RETRYING stamp
    (``workers/sync_units.py`` -- the ``except RateLimitException`` branch of
    ``run_sync_unit``), in the same session/transaction: the observation and
    the deferral must commit together or not at all, so this table never
    holds an "orphan" row for a deferral that didn't actually happen (and vice
    versa).

    No foreign keys to ``sync_runs`` / ``sync_run_units`` / ``Integration``:
    this is a durable, independently-retained observation log (see the
    beat-scheduled prune task, ``workers/sync_reconciler.py``), not a
    dependent child row -- it must survive its parent run/unit being pruned
    or deleted, mirroring ``SyncRunUnit.integration_id`` (also FK-less, per
    ``models/integrations.py``).
    """

    __tablename__ = "provider_rate_limit_observations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    host: Mapped[str | None] = mapped_column(Text, nullable=True)
    integration_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    sync_run_unit_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    route_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    dimension: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_after_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    reset_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        # Cooldown lookup: "what's the most recent observation for this
        # provider/integration/route-family" (CHAOS-2760 consumer).
        Index(
            "ix_provider_rate_limit_observations_cooldown",
            "provider",
            "integration_id",
            "route_family",
            "observed_at",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ProviderRateLimitObservation("
            f"org={self.org_id!r}, provider={self.provider!r}, "
            f"route_family={self.route_family!r}, observed_at={self.observed_at})>"
        )
