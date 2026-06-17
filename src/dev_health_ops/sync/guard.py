"""Dispatch-layer guard contract (CHAOS-2285 folded -> CHAOS-2512).

FROZEN CONTRACT — tier/plan + concurrency enforcement at the dispatch layer.

Today tier limits are enforced only at the API boundary; the scheduler and
worker dispatch paths bypass them. :meth:`DispatchGuard.authorize_run` MUST be
invoked at the TOP of ``dispatch_sync_run`` (after planning, before any unit is
queued) so that API, scheduler, and backfill all pass through one guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class GuardDecision:
    """Outcome of an authorize check.

    ``capped_unit_ids`` lets the guard deny a subset (e.g. over a cost-class
    concurrency cap) while letting the rest of the run proceed; an empty tuple
    with ``allowed=True`` means the whole run is authorized.
    """

    allowed: bool
    reason: str | None = None
    capped_unit_ids: tuple[str, ...] = field(default_factory=tuple)


class DispatchGuard:
    """Gates total unit count, cost-class totals, and active concurrency."""

    @staticmethod
    def authorize_run(session: Session, sync_run_id: str) -> GuardDecision:
        """Authorize (or cap) a planned run before its units are queued.

        Reads the persisted plan and meters against org/provider/cost-class
        budgets. Implemented in CHAOS-2512.
        """

        raise NotImplementedError("CHAOS-2512: implement DispatchGuard")
