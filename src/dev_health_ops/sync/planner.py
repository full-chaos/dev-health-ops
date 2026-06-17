"""Sync planner contract (CHAOS-2511).

FROZEN CONTRACT — this module's public types and signatures are the interface
between the planner (Wave 1, CHAOS-2511) and the dispatcher/unit-worker
(Wave 2, CHAOS-2512/2513). Implement BEHIND these signatures; do not change the
DTO shapes without updating every consumer.

Responsibilities (CHAOS-2511):
  * Load enabled sources + enabled datasets for an integration.
  * Skip unsupported provider/dataset pairs (see ``sync.datasets``).
  * Resolve incremental windows from per-(source, dataset) watermarks.
  * Resolve backfill windows via ``backfill.chunker``.
  * Assign cost class per dataset.
  * Persist the FULL plan (SyncRun + all SyncRunUnit rows, status=planned)
    BEFORE any dispatch. Dispatch is a separate, idempotent step.

Invariants:
  * Disabled source -> zero units. Disabled dataset -> zero units.
  * Backfill units carry mode="backfill" and must never update watermarks.
  * total_units on the persisted SyncRun equals len(unit_ids).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from dev_health_ops.sync.datasets import get_dataset_spec

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class WatermarkKey:
    """Generalized watermark identity (CHAOS-2509)."""

    org_id: str
    source_id: str
    dataset_key: str


@dataclass(frozen=True)
class SyncPlanRequest:
    """Input to :func:`plan_sync_run`.

    ``source_ids`` / ``dataset_keys`` of ``None`` mean "all enabled". Explicit
    tuples filter to the given subset (still intersected with enabled rows).
    """

    integration_id: str
    mode: str  # one of models.integrations.SyncRunMode
    triggered_by: str
    source_ids: tuple[str, ...] | None = None
    dataset_keys: tuple[str, ...] | None = None
    since: datetime | None = None
    before: datetime | None = None


@dataclass(frozen=True)
class PlannedUnit:
    """Frozen description of one execution unit prior to persistence.

    Mirrors the ``SyncRunUnit`` columns. Celery payloads carry the persisted
    ``unit_id`` ONLY (never this object, never credentials).
    """

    org_id: str
    integration_id: str
    source_id: str
    provider: str
    dataset_key: str
    cost_class: str
    mode: str
    window_start: datetime | None
    window_end: datetime | None
    processor_flags: Mapping[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class SyncRunPlan:
    """Result of :func:`plan_sync_run` — the persisted run + its unit ids."""

    sync_run_id: str
    total_units: int
    unit_ids: tuple[str, ...]


def plan_sync_run(session: Session, request: SyncPlanRequest) -> SyncRunPlan:
    """Expand an integration into persisted SyncRun + SyncRunUnit rows.

    Persists everything with status=planned and returns the run id + unit ids.
    Implemented in CHAOS-2511. The dispatcher (CHAOS-2512) consumes the result
    via :func:`dev_health_ops.workers.sync_units.dispatch_sync_run`.
    """

    raise NotImplementedError("CHAOS-2511: implement plan_sync_run")


def map_datasets_to_legacy_targets(
    provider: str, dataset_keys: Iterable[str]
) -> frozenset[str]:
    """Fan-in seam: union the legacy post-sync targets for completed datasets.

    ``finalize_sync_run`` (CHAOS-2512) calls this to translate the dataset keys
    of successful units back into the legacy ``sync_targets`` vocabulary that
    ``_dispatch_post_sync_tasks`` understands, so metrics fan-out stays unchanged.
    Registry-owned mapping — do NOT hand-roll string mapping in finalize.
    """

    targets: set[str] = set()
    for dataset_key in dataset_keys:
        spec = get_dataset_spec(provider, dataset_key)
        if spec is not None:
            targets.update(spec.legacy_targets)
    return frozenset(targets)
