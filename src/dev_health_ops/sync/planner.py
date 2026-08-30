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
  * Backfill -> incremental composition (CHAOS-2570): because backfill never
    seeds a watermark, the first incremental after a backfill cold-starts;
    continuity is provided by the cold-start depth (CHAOS-2569,
    ``window_start = now - initial_sync_depth``). No date gap results as long as
    the first incremental runs within ``initial_sync_depth`` of the backfill's
    ``before``. The no-gap guarantee is therefore BOUNDED to that depth window:
    backfill stays watermark-free (CHAOS-2514) and no ``backfilled-through``
    marker is introduced. If the first incremental is delayed beyond
    ``initial_sync_depth`` after ``before``, the residual gap
    ``[before, now - depth]`` is an accepted, tracked limitation (CHAOS-2588)
    whose fix would require such a marker. See docs/architecture/data-pipeline.md.
  * HEAVY incremental window ratchet (CHAOS-3412): unit cost is linear in window
    span, so a HEAVY-cost-class dataset spanning a wide ``initial_sync_depth``
    (or a long-stale watermark) can never fit the sync budget as ONE window — it
    is deferred forever and, having never succeeded, never stamps a watermark.
    INCREMENTAL windows for HEAVY datasets are therefore capped at
    ``SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS`` (default 7): the window END moves
    in to ``window_start + cap``; depth resolution and its tier ``backfill_days``
    cap are untouched. A capped run is a healthy PARTIAL run — the success path
    stamps the watermark at the unit's window END (``before_at``), so the next
    scheduled tick resumes exactly there and coverage ratchets forward one capped
    window per tick with no gap. Catch-up is paced by the scheduler cadence.
    The cap must EXCEED ``SYNC_WATERMARK_OVERLAP``: the window starts at
    ``watermark - overlap``, so a cap <= overlap ends at or before the watermark
    and the monotonic write is discarded, stalling the ratchet while every run
    reports success. ``_effective_heavy_max_window_days`` clamps the cap above
    the overlap and logs a warning naming both values.
    See docs/operate/run/ingestion-and-backfills.md.
  * Watermark-stamping window rules (CHAOS-3412), enforced once in
    ``_watermark_stamping_window`` for BOTH modes whose success path stamps a
    watermark (INCREMENTAL and FULL_RESYNC — ``sync_units`` gates on exactly
    those two): (a) the end is clamped to ``now``, because a future ``before``
    would persist a FUTURE watermark and the next run would start in the future
    and silently skip everything up to it; (b) an empty or inverted window
    (``end <= start``, i.e. already synced past the requested end) plans ZERO
    units, because ``window_span_days`` floors a negative span to 1 day, so an
    inverted unit is admitted at the cheapest possible cost, fetches nothing, and
    finalizes SUCCESS — a false coverage claim. BACKFILL is deliberately excluded:
    it never stamps a watermark (CHAOS-2514) and validates its own bounds.
    (c) A resolved start AHEAD of ``now`` means the stored watermark is corrupt
    (skewed provider timestamp, or a future end persisted by pre-CHAOS-3412
    code). Rule (b) alone would make that FATAL — zero units, FAILED forever, and
    monotonic writes mean nothing can lower it. So the start is clamped back to a
    bounded recovery window and warned about, and ``sync.watermarks`` permits the
    single downward correction needed to restore a sane value.
  * FULL_RESYNC HEAVY windows are deliberately UNCAPPED (CHAOS-3412, decided).
    The heavy cap exists so repeated incremental ticks make progress; a one-shot
    full resync has no next tick, so a capped window would cover the cap's span
    and finalize SUCCESS — claiming a resync that did not happen. The wide-window
    exposure is bounded instead by the budget exhaustion path, which terminalizes
    visibly and names the scoped-backfill remedy. Pinned by
    ``test_full_resync_heavy_window_is_deliberately_uncapped``.
  * total_units on the persisted SyncRun equals len(unit_ids).
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, time, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError

from dev_health_ops.backfill.chunker import chunk_date_range
from dev_health_ops.credentials.fingerprint import (
    AUTH_SOURCE_ENVIRONMENT,
    AUTH_SOURCE_INTEGRATION_CREDENTIAL,
    credential_fingerprint,
)
from dev_health_ops.models import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncConfiguration,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.providers.github.work_item_options import (
    snapshot_github_work_item_runtime_options,
)
from dev_health_ops.sync.canonical_incident_gate import (
    require_canonical_incident_feature_sync,
    sync_datasets_require_canonical_incident_feature,
)
from dev_health_ops.sync.datasets import (
    CostClass,
    DatasetKey,
    DatasetSpec,
    WatermarkBehavior,
    get_dataset_spec,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.executed_proof_ledger import (
    record_executed_proof_attempts,
)
from dev_health_ops.sync.family_flags import (
    FAMILY_CANONICAL_DATASET_KEY,
    PR_SOCIAL_CANONICAL_DATASET_KEY,
    PR_SOCIAL_DATASETS,
    TESTOPS_CANONICAL_DATASET_KEY,
    TESTOPS_DATASETS,
    WORK_ITEM_DATASETS,
    family_dataset_flag,
)
from dev_health_ops.sync.family_flags import (
    family_dataset_keys_from_flags as family_dataset_keys_from_flags,
)
from dev_health_ops.sync.guard import _resolve_total_unit_cap
from dev_health_ops.sync.pagerduty_repair import (
    repair_pagerduty_operational_integration,
)
from dev_health_ops.sync.watermarks import (
    _watermark_overlap_seconds,
    get_watermark_with_overlap,
)
from dev_health_ops.tracing import current_trace_parent
from dev_health_ops.workers.provider_unit_route import routes_to_river

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_SECONDS_PER_DAY = 86_400

# Recovery lookback when a stored watermark is found AHEAD of now (corrupt).
# Small on purpose: the goal is to get ONE unit planned so the success path can
# re-stamp a sane watermark, not to re-crawl history. See
# _watermark_stamping_window.
_FUTURE_WATERMARK_RECOVERY_SECONDS = 3600

# (cap_days, overlap_seconds) pairs already warned about this process — see
# _effective_heavy_max_window_days. Cleared by tests via
# reset_heavy_cap_clamp_warnings.
_WARNED_CAP_CLAMPS: set[tuple[int, int]] = set()


def reset_heavy_cap_clamp_warnings() -> None:
    """Clear the once-per-process clamp-warning cache (test hook)."""
    _WARNED_CAP_CLAMPS.clear()


@dataclass(frozen=True)
class WatermarkKey:
    """Generalized watermark identity (CHAOS-2509)."""

    org_id: str
    source_id: str
    dataset_key: str


@dataclass(frozen=True)
class BackfillSelector:
    """Explicit backfill scope for date and repository/unit selectors."""

    since: datetime
    before: datetime
    source_ids: tuple[str, ...] | None = None
    dataset_keys: tuple[str, ...] | None = None


@dataclass(frozen=True)
class SyncPlanRequest:
    """Input to :func:`plan_sync_run`.

    ``source_ids`` / ``dataset_keys`` of ``None`` mean "all enabled". Explicit
    tuples filter to the given subset (still intersected with enabled rows).
    ``backfill_selector`` is the newer structured form used by partial
    backfill callers; the flat fields remain as compatibility aliases.
    """

    integration_id: str
    org_id: str
    mode: str  # one of models.integrations.SyncRunMode
    triggered_by: str
    backfill_selector: BackfillSelector | None = None
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
    dispatch_required: bool = True
    terminal_reason: str = ""


class SyncPlanUnitCapExceededError(ValueError):
    """Plan-time guard: the expanded plan exceeds the org's run unit cap.

    Raised BEFORE any SyncRun/SyncRunUnit row is persisted so an oversized
    backfill/sync fails fast at the API boundary instead of being accepted
    (202) and then hard-denied asynchronously by ``DispatchGuard`` — which
    would terminalize the run as FAILED after the fact. Subclasses
    ``ValueError`` so existing planner error handling (admin routers map
    ``ValueError`` -> HTTP 400) surfaces it without extra wiring.
    """

    def __init__(self, *, planned_units: int, total_cap: int) -> None:
        self.planned_units = planned_units
        self.total_cap = total_cap
        super().__init__(
            f"sync plan exceeds the run unit cap: {planned_units}/{total_cap} "
            "units; reduce the backfill date range or narrow sources/datasets"
        )


def plan_sync_run(session: Session, request: SyncPlanRequest) -> SyncRunPlan:
    """Expand an integration into persisted SyncRun + SyncRunUnit rows.

    Persists everything with status=planned and returns the run id + unit ids.
    Implemented in CHAOS-2511. The dispatcher (CHAOS-2512) consumes the result
    via :func:`dev_health_ops.workers.sync_units.dispatch_sync_run`.
    """

    integration = _load_integration(session, request.integration_id, request.org_id)
    repair_outcome = repair_pagerduty_operational_integration(session, integration)
    _repair_github_work_item_runtime_options(session, integration)
    mode = _validate_mode(request.mode)
    _validate_backfill_selector_compatibility(request)
    request = _normalize_backfill_selector(request)
    if repair_outcome is not None:
        return _terminalize_pagerduty_disabled_plan(
            session=session,
            integration=integration,
            request=request,
            mode=mode,
            reason=repair_outcome,
        )
    _reconcile_explicit_requested_datasets(session, integration, request)
    gate_datasets = _load_enabled_datasets(session, integration, request.dataset_keys)
    if sync_datasets_require_canonical_incident_feature(
        str(integration.provider),
        (str(dataset.dataset_key) for dataset in gate_datasets),
    ):
        require_canonical_incident_feature_sync(session, integration.org_id)
    sources = _load_enabled_sources(session, integration, request.source_ids)
    datasets = _load_enabled_datasets(session, integration, request.dataset_keys)
    now = datetime.now(timezone.utc)

    planned_units = _build_planned_units(
        session=session,
        request=request,
        integration=integration,
        sources=sources,
        datasets=datasets,
        mode=mode,
        now=now,
    )

    # Plan-time mirror of DispatchGuard's total-unit cap (CHAOS-2512): deny
    # oversized plans BEFORE persisting anything. Without this, the run and
    # all its units are persisted, dispatch hard-denies asynchronously, and
    # the caller only learns from a FAILED run after the fact.
    total_cap = _resolve_total_unit_cap(session, str(integration.org_id))
    if len(planned_units) > total_cap:
        raise SyncPlanUnitCapExceededError(
            planned_units=len(planned_units), total_cap=total_cap
        )

    credential_id: uuid.UUID | None = None
    credential_fp: str | None = None
    auth_source: str | None = None
    if planned_units:
        credential_id, credential_fp, auth_source = _resolve_credential_stamp(
            session, integration
        )
    elif _zero_unit_plan_needs_credential_stamp(integration):
        # Freeze auth for a zero-unit jira plan whose UNCONDITIONAL
        # reference-discovery ledger row (seeded below by
        # seed_reference_discovery_ledger for every mode, backfill included)
        # will still make a real, credential-requiring Jira API call
        # (CHAOS-4593; see _zero_unit_plan_needs_credential_stamp for the
        # full root cause and why this stays jira-only rather than every
        # provider, and for the pinned tests/carve-outs this preserves).
        credential_id, credential_fp, auth_source = _resolve_credential_stamp(
            session, integration
        )
        # Codex review (CHAOS-4593, round 1, P2): plan_sync_run does not own
        # this session's transaction boundary -- every caller commits
        # separately, sometime after this function returns -- so counting
        # here, inline, would publish a stamp for a plan that a later flush
        # or the caller's own commit could still roll back. Mirrors the
        # ALREADY-existing deferred-increment pattern this codebase uses for
        # exactly this reason (`workers/sync_units.py`'s
        # ZERO_UNIT_FINALIZATIONS_TOTAL, "DECIDED here, not incremented
        # here"): register a one-shot `after_commit` hook instead of
        # incrementing inline, so a rolled-back transaction never inflates
        # the counter.
        _defer_zero_unit_credential_stamp_telemetry(
            session,
            org_id=str(integration.org_id),
            integration_id=str(integration.id),
            provider=str(integration.provider),
            mode=mode,
        )

    sync_run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by=request.triggered_by,
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=len(planned_units),
        completed_units=0,
        failed_units=0,
        credential_id=credential_id,
        credential_fingerprint=credential_fp,
        auth_source=auth_source,
        # CHAOS-3996: captured once, here, so every coordinator dispatch this
        # run produces can join back to the same trace regardless of how long
        # the run takes or how many dispatch/finalize/post_sync/
        # reference_discovery cycles it goes through.
        trace_parent=current_trace_parent(),
    )
    session.add(sync_run)
    session.flush()

    unit_rows = [
        SyncRunUnit(
            org_id=unit.org_id,
            sync_run_id=sync_run.id,
            integration_id=integration.id,
            source_id=_coerce_uuid(unit.source_id, "source_id"),
            provider=unit.provider,
            dataset_key=unit.dataset_key,
            cost_class=unit.cost_class,
            mode=unit.mode,
            since_at=unit.window_start,
            before_at=unit.window_end,
            status=SyncRunUnitStatus.PLANNED.value,
            attempts=0,
            processor_flags=dict(unit.processor_flags),
        )
        for unit in planned_units
    ]
    session.add_all(unit_rows)
    session.flush()
    # CHAOS-4114: this is the Python half of the executed-proof ledger's
    # "attempted" write, and it is the path an operator reaches through the
    # admin sync endpoints and the backfill runner. It writes sync_run_units
    # through the ORM rather than through SQL, which is exactly why sweeping
    # "the write paths" by grepping for INSERT statements misses it. It runs
    # in the SAME transaction as the unit rows on purpose -- see the module
    # docstring for why a separately committed ledger fails OPEN.
    record_executed_proof_attempts(
        session,
        [(unit.provider, unit.dataset_key) for unit in unit_rows],
        now=now,
    )
    seed_reference_discovery_ledger(
        session,
        org_id=str(integration.org_id),
        sync_run_id=str(sync_run.id),
        now=now,
    )

    return SyncRunPlan(
        sync_run_id=str(sync_run.id),
        total_units=len(unit_rows),
        unit_ids=tuple(str(unit.id) for unit in unit_rows),
    )


def seed_reference_discovery_ledger(
    session: Session, *, org_id: str, sync_run_id: str, now: datetime
) -> None:
    """Arm the reference-discovery ledger + outbox wakeup for a sync run.

    Extracted from :func:`plan_sync_run` (CHAOS-4498) so this exact
    seeding -- a ``SyncRunReferenceDiscovery`` row plus the
    ``OUTBOX_KIND_DISCOVERY`` wakeup that ``NativeReferenceDiscoveryService``
    (Go) claims and processes through ``TeamCatalogDiscoveryExecutor`` -- has
    exactly one implementation. ``plan_sync_run`` calls this unconditionally
    for every mode (backfill included); :func:`seed_reference_discovery_run`
    below is the second, standalone caller for operator backfills that don't
    go through the full unit-planning path.
    """
    session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=sync_run_id,
            status="planned",
            attempts=0,
            available_at=now,
        )
    )
    session.flush()
    upsert_outbox_wakeup(
        session,
        sync_run_id=sync_run_id,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=now,
        now=now,
    )


def seed_reference_discovery_run(
    session: Session,
    *,
    integration_id: str,
    org_id: str,
    triggered_by: str,
    mode: str = SyncRunMode.BACKFILL.value,
) -> str:
    """Create a minimal, zero-unit SyncRun anchor and arm its reference-
    discovery ledger row (CHAOS-4498).

    For a caller (the operator backfill tool) that needs strict reference
    discovery -- routed through the SAME native-Go-collector-or-Python-
    bridge seam (``TeamCatalogDiscoveryExecutor``) every sync-time dispatch
    uses -- without planning a full unit set of its own (the legacy backfill
    path fetches work items itself via ``run_work_items_sync_job``, not
    through River units). The returned run has ``total_units=0``; the
    existing zero-unit dispatch/finalize outbox chain (proven idempotent,
    see lane-4431's 2026-08-29 close-out) carries it from PLANNED to a
    terminal ``sync_runs.status`` on its own once reference discovery
    stamps success -- no new Go or dispatch code needed. That terminal
    status is ``FAILED``, not ``SUCCESS`` (live-verified, CHAOS-4502):
    ``aggregateRunStatus``/``_aggregate_run_status`` treat any zero-unit
    run as a loud failure by design (CHAOS-4159, "never a silent
    success"), which this anchor collides with even though nothing
    actually failed. Harmless to the caller -- the reference-discovery
    ledger's own ``status``/``result`` are correct, and
    ``run_backfill_for_config``'s return value reports the discovery
    outcome, never this row's status -- but it does mean the anchor
    shows up in failed-sync-run counts/dashboards until CHAOS-4502 gives
    it its own terminal state.

    Credentials are resolved and frozen exactly as :func:`plan_sync_run`
    does (:func:`_resolve_credential_stamp`), because the Python-side
    Fallback populate path (jira, or any future non-native provider) reads
    them via ``resolve_run_auth`` off this same run row.
    """
    integration = _load_integration(session, integration_id, org_id)
    credential_id, credential_fp, auth_source = _resolve_credential_stamp(
        session, integration
    )
    now = datetime.now(timezone.utc)
    sync_run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by=triggered_by,
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
        credential_id=credential_id,
        credential_fingerprint=credential_fp,
        auth_source=auth_source,
        trace_parent=current_trace_parent(),
    )
    session.add(sync_run)
    session.flush()
    seed_reference_discovery_ledger(
        session,
        org_id=str(integration.org_id),
        sync_run_id=str(sync_run.id),
        now=now,
    )
    return str(sync_run.id)


def _terminalize_pagerduty_disabled_plan(
    *,
    session: Session,
    integration: Integration,
    request: SyncPlanRequest,
    mode: str,
    reason: str,
) -> SyncRunPlan:
    """Persist a terminal PagerDuty repair outcome without dispatch artifacts."""
    completed_at = datetime.now(timezone.utc)
    sync_run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by=request.triggered_by,
        mode=mode,
        status=SyncRunStatus.FAILED.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
        completed_at=completed_at,
        result={"error_category": "pagerduty_sync_disabled"},
        error=reason,
    )
    session.add(sync_run)
    session.flush()
    return SyncRunPlan(
        sync_run_id=str(sync_run.id),
        total_units=0,
        unit_ids=(),
        dispatch_required=False,
        terminal_reason=reason,
    )


def _validate_backfill_selector_compatibility(request: SyncPlanRequest) -> None:
    """Reject mixed structured/legacy backfill scopes before planning."""
    selector = request.backfill_selector
    if selector is None:
        return
    if any(
        value is not None
        for value in (
            request.since,
            request.before,
            request.source_ids,
            request.dataset_keys,
        )
    ):
        raise ValueError("backfill selector cannot be mixed with legacy flat fields")


def _normalize_backfill_selector(request: SyncPlanRequest) -> SyncPlanRequest:
    """Project the structured backfill selector onto the legacy flat fields."""
    selector = request.backfill_selector
    if selector is None:
        return request
    return replace(
        request,
        source_ids=selector.source_ids,
        dataset_keys=selector.dataset_keys,
        since=selector.since,
        before=selector.before,
    )


def _load_integration(
    session: Session, integration_id: str, org_id: str
) -> Integration:
    integration_uuid = _coerce_uuid(integration_id, "integration_id")
    integration = (
        session.query(Integration)
        .filter(Integration.id == integration_uuid, Integration.org_id == org_id)
        .one_or_none()
    )
    if integration is None:
        raise ValueError(f"Integration not found for org {org_id}: {integration_id}")
    return integration


_ZERO_UNIT_CREDENTIAL_STAMP_PENDING_KEY = (
    "_chaos_4593_zero_unit_credential_stamp_pending"
)
_ZERO_UNIT_CREDENTIAL_STAMP_LISTENERS_INSTALLED_KEY = (
    "_chaos_4593_zero_unit_credential_stamp_listeners_installed"
)


def _drain_zero_unit_credential_stamp_telemetry(session: Session) -> None:
    """``after_commit`` drain: emit + clear whatever is queued in
    ``session.info``, a no-op when nothing is pending (CHAOS-4593)."""
    pending = session.info.pop(_ZERO_UNIT_CREDENTIAL_STAMP_PENDING_KEY, None)
    if not pending:
        return
    from dev_health_ops.metrics.prometheus import (
        SYNC_ZERO_UNIT_PLAN_CREDENTIAL_STAMPED_TOTAL,
    )

    for item in pending:
        SYNC_ZERO_UNIT_PLAN_CREDENTIAL_STAMPED_TOTAL.labels(
            provider=item["provider"]
        ).inc()
        logger.info(
            "sync.planner.zero_unit_plan_stamped_for_strict_discovery", extra=item
        )


def _cancel_zero_unit_credential_stamp_telemetry(session: Session) -> None:
    """``after_rollback`` drain: discard whatever is queued, unemitted."""
    session.info.pop(_ZERO_UNIT_CREDENTIAL_STAMP_PENDING_KEY, None)


def _defer_zero_unit_credential_stamp_telemetry(
    session: Session,
    *,
    org_id: str,
    integration_id: str,
    provider: str,
    mode: str,
) -> None:
    """Increment the CHAOS-4593 zero-unit credential-stamp counter/log ONLY
    after this ``session``'s enclosing transaction actually commits.

    ``plan_sync_run`` does not own the transaction boundary -- every caller
    (the admin router, the scheduler, ``create_sync_execution_trigger``)
    commits ``session`` on its own, sometime after this function returns --
    so an inline increment would survive a later rollback and publish a
    stamp for a plan that was never actually persisted, and (if the
    rollback's listener is never cleared) could misattribute telemetry to a
    later, unrelated commit on a reused session.

    Two codex review rounds (CHAOS-4593) landed on this final shape after
    two unsafe attempts:

    * Round 2 attempt: each handler called ``event.remove()`` on itself from
      inside its own dispatch -- crashes SQLAlchemy with "deque mutated
      during iteration" (regression test below pins this).
    * Round 3 attempt: a shared cancelled-flag with both listeners
      registered ``once=True`` avoided that crash and the misattribution,
      but ``once=True`` in SQLAlchemy 2.0 only gates re-EXECUTION -- it does
      NOT deregister the listener (verified empirically:
      ``event.contains()`` still returns ``True`` after the one-shot fires).
      Every zero-unit plan on a long-lived, reused session would therefore
      permanently grow that session's listener list by two closures.

    This shape installs at most ONE (non-``once``) ``after_commit`` /
    ``after_rollback`` pair PER SESSION, EVER -- guarded by a sentinel in
    ``session.info`` that is never cleared -- and every call just appends a
    plain dict to a ``session.info`` list. The listener count is therefore
    bounded at 2 regardless of how many zero-unit plans that session plans;
    the drain functions are idempotent no-ops when nothing is pending, so
    firing on an unrelated commit/rollback is harmless by construction
    (there is nothing of THIS plan's left to emit or cancel incorrectly).
    """
    pending = session.info.setdefault(_ZERO_UNIT_CREDENTIAL_STAMP_PENDING_KEY, [])
    pending.append(
        {
            "org_id": org_id,
            "integration_id": integration_id,
            "provider": provider,
            "mode": mode,
        }
    )
    if session.info.get(_ZERO_UNIT_CREDENTIAL_STAMP_LISTENERS_INSTALLED_KEY):
        return
    session.info[_ZERO_UNIT_CREDENTIAL_STAMP_LISTENERS_INSTALLED_KEY] = True

    from sqlalchemy import event

    event.listen(session, "after_commit", _drain_zero_unit_credential_stamp_telemetry)
    event.listen(
        session, "after_rollback", _cancel_zero_unit_credential_stamp_telemetry
    )


def _zero_unit_plan_needs_credential_stamp(integration: Integration) -> bool:
    """Whether a ZERO-unit :func:`plan_sync_run` plan should still resolve
    its credential stamp (CHAOS-4593).

    ``seed_reference_discovery_ledger`` arms a reference-discovery job
    unconditionally -- for every mode, including a zero-unit plan (e.g. every
    ``IntegrationSource`` temporarily disabled). The original ``if
    planned_units:`` gate assumed a zero-unit plan "has no later phase that
    can consume credentials" -- true for most providers' best-effort
    (non-strict) discovery, which skips cleanly on missing credentials, but
    false for Jira: ``workers/team_autoimport_jira.py``'s ``populate()``
    raises on missing credentials even with zero import categories selected
    (it still resolves dispatch-blocking sprint keys unconditionally in
    strict mode). A zero-unit jira plan that skips this stamp leaves
    ``auth_source`` NULL, ``_load_discovery_context`` resolves ``{}``
    credentials, and the strict populate() raises "missing Jira credentials
    for strict reference discovery" on every attempt -- the run never
    leaves PLANNED, retrying until it exhausts attempts (live-reproduced,
    org 70d529e0, run 527271e6-ac3d-4c24-af35-2147bde8d59c).

    Scoped to jira only, NOT every provider: GitHub's populate()
    (``workers/team_autoimport_github.py``) has the identical
    strict-mode-raises-on-missing-credentials shape, so it plausibly carries
    the same latent bug for a zero-unit GitHub plan -- but that is unproven
    here and out of THIS ticket's scope (flagged as a follow-up, parent
    CHAOS-4198, rather than folded in speculatively). Widening this beyond
    jira would also break the existing pinned
    ``test_disabled_source_produces_zero_units_without_hydrating_credentials``
    /``test_disabled_dataset_produces_zero_units_without_hydrating_credentials``
    (both use the default "github" provider and assert NO credential
    hydration for a zero-unit plan) and PagerDuty's credential-less carve-out
    (CHAOS-4498 codex round 2, P1 --
    ``test_load_discovery_context_preserves_credential_free_zero_unit_planner_run``:
    ``_resolve_credential_stamp`` itself raises for a NULL ``credential_id``
    there, and that raise must stay reachable only for executable
    (``planned_units > 0``) runs).
    """
    return str(integration.provider).lower() == "jira"


def _resolve_credential_stamp(
    session: Session, integration: Integration
) -> tuple[uuid.UUID | None, str, str]:
    """Resolve the run-level auth stamp for :func:`plan_sync_run` (CHAOS-2755).

    Returns ``(credential_id, credential_fingerprint, auth_source)``:

      * ``Integration.credential_id`` is NULL -> environment auth. The stamp
        carries ``credential_id=None``, ``auth_source='environment'`` and a
        best-effort fingerprint of the resolved env credentials. This is
        deliberately distinguishable from a legacy NULL-stamped run (whose
        ``auth_source`` column itself is NULL).
      * Otherwise the referenced ``IntegrationCredential`` is stamped. Its
        ``is_active`` flag is enforced HERE, at plan time only — a run stamped
        against an active credential deliberately tolerates that credential
        being deactivated mid-run (that asymmetry is exactly what "freezing"
        means; see docs/architecture/sync-unit-model.md). We do NOT persist the
        full-payload secret hash; only the safe-scope content witness.
    """
    # Imported lazily: task_utils pulls in worker/encryption machinery, and the
    # planner is imported from those layers — a module-level import would risk a
    # cycle (mirrors the lazy TierLimitService import below).
    from dev_health_ops.models import IntegrationCredential
    from dev_health_ops.workers.task_utils import (
        _credential_mapping,
        _resolve_env_credentials,
    )

    provider = str(integration.provider).lower()
    integration_id = str(integration.id)

    if integration.credential_id is None:
        if provider == "pagerduty":
            raise ValueError(
                "PagerDuty sync requires an active organization-scoped credential"
            )
        env_credentials = dict(_resolve_env_credentials(provider))
        fingerprint = credential_fingerprint(
            env_credentials, credential_id=None, integration_id=integration_id
        )
        return None, fingerprint, AUTH_SOURCE_ENVIRONMENT

    credential = (
        session.query(IntegrationCredential)
        .filter(
            IntegrationCredential.id == integration.credential_id,
            IntegrationCredential.org_id == integration.org_id,
            IntegrationCredential.provider == provider,
        )
        .one_or_none()
    )
    if credential is None:
        raise ValueError(
            "Integration credential not found at plan time: "
            f"{integration.credential_id}"
        )
    if not credential.is_active:
        raise ValueError(
            f"Integration credential is inactive: {integration.credential_id}"
        )

    decrypted = _credential_mapping(credential)
    fingerprint = credential_fingerprint(
        decrypted,
        credential_id=str(integration.credential_id),
        integration_id=integration_id,
    )
    return integration.credential_id, fingerprint, AUTH_SOURCE_INTEGRATION_CREDENTIAL


def _load_enabled_sources(
    session: Session,
    integration: Integration,
    source_ids: tuple[str, ...] | None,
) -> list[IntegrationSource]:
    query = session.query(IntegrationSource).filter(
        IntegrationSource.org_id == integration.org_id,
        IntegrationSource.integration_id == integration.id,
        IntegrationSource.is_enabled.is_(True),
    )
    if source_ids is not None:
        source_uuids = tuple(
            _coerce_uuid(source_id, "source_id") for source_id in source_ids
        )
        if not source_uuids:
            return []
        query = query.filter(IntegrationSource.id.in_(source_uuids))
    return list(query.order_by(IntegrationSource.full_name, IntegrationSource.id).all())


def _repair_github_work_item_runtime_options(
    session: Session, integration: Integration
) -> None:
    """Durably default legacy GitHub work-item datasets before unit planning."""
    if str(integration.provider).lower() != "github":
        return
    integration_options = dict(integration.config or {})
    dataset = (
        session.query(IntegrationDataset)
        .filter(
            IntegrationDataset.org_id == integration.org_id,
            IntegrationDataset.integration_id == integration.id,
            IntegrationDataset.dataset_key == DatasetKey.WORK_ITEMS.value,
        )
        .one_or_none()
    )
    dataset_options = dict(dataset.options or {}) if dataset is not None else {}
    canonical = snapshot_github_work_item_runtime_options(
        {**integration_options, **dataset_options}
    )
    repaired_integration_options = {**integration_options, **canonical}
    if repaired_integration_options != integration_options:
        integration.config = repaired_integration_options

    if dataset is None:
        session.flush()
        return
    repaired_dataset_options = {**dataset_options, **canonical}
    if repaired_dataset_options != dataset_options:
        dataset.options = repaired_dataset_options
    session.flush()


def _reconcile_explicit_requested_datasets(
    session: Session,
    integration: Integration,
    request: SyncPlanRequest,
) -> None:
    """Enable any explicitly requested dataset that has no row yet.

    Every dataset — including ``security`` (CHAOS-3400) — is opt-in and
    reconciled uniformly here: a caller (operator, backfill, or scheduled
    trigger) must name a dataset in ``request.dataset_keys`` for it to be
    created/enabled. Nothing auto-selects a dataset the caller didn't ask for.
    """
    dataset_keys = request.dataset_keys
    if dataset_keys is None:
        return

    provider = str(integration.provider).lower()
    requested_keys = {
        dataset_key
        for dataset_key in dataset_keys
        if (spec := get_dataset_spec(provider, dataset_key)) is not None
        and spec.supported
    }
    if not requested_keys:
        return

    existing_keys = {
        dataset.dataset_key
        for dataset in session.query(IntegrationDataset)
        .filter(
            IntegrationDataset.org_id == integration.org_id,
            IntegrationDataset.integration_id == integration.id,
            IntegrationDataset.dataset_key.in_(requested_keys),
        )
        .all()
    }
    for dataset_key in requested_keys - existing_keys:
        _insert_dataset_if_missing(
            session,
            IntegrationDataset(
                org_id=integration.org_id,
                integration_id=integration.id,
                dataset_key=dataset_key,
                is_enabled=True,
                options={},
            ),
        )


def _insert_dataset_if_missing(
    session: Session,
    dataset: IntegrationDataset,
) -> None:
    try:
        with session.begin_nested():
            session.add(dataset)
            session.flush()
    except IntegrityError:
        existing = (
            session.query(IntegrationDataset)
            .filter(
                IntegrationDataset.org_id == dataset.org_id,
                IntegrationDataset.integration_id == dataset.integration_id,
                IntegrationDataset.dataset_key == dataset.dataset_key,
            )
            .one_or_none()
        )
        if existing is None:
            raise


def _load_enabled_datasets(
    session: Session,
    integration: Integration,
    dataset_keys: tuple[str, ...] | None,
) -> list[IntegrationDataset]:
    query = session.query(IntegrationDataset).filter(
        IntegrationDataset.org_id == integration.org_id,
        IntegrationDataset.integration_id == integration.id,
        IntegrationDataset.is_enabled.is_(True),
    )
    if dataset_keys is not None:
        if not dataset_keys:
            return []
        query = query.filter(IntegrationDataset.dataset_key.in_(dataset_keys))
    return list(query.order_by(IntegrationDataset.dataset_key).all())


def _prs_dataset_enabled(provider: str, datasets: list[IntegrationDataset]) -> bool:
    """True if any enabled dataset maps to the legacy ``prs`` target.

    The github work-items sync ingests PRs as work items only when the PRS
    dataset family is enabled (CHAOS-646); the legacy worker passed
    ``include_pull_requests=("prs" in sync_targets)``. The unitized work-items
    unit cannot see sibling datasets at run time, so the planner stamps this as
    a ``sync_prs`` processor flag on the work-items unit (consumed by
    ``processors/dataset_adapters._work_item_kwargs``).
    """
    for dataset in datasets:
        spec = get_dataset_spec(provider, dataset.dataset_key)
        if spec is not None and "prs" in spec.legacy_targets:
            return True
    return False


def _build_planned_units(
    *,
    session: Session,
    request: SyncPlanRequest,
    integration: Integration,
    sources: list[IntegrationSource],
    datasets: list[IntegrationDataset],
    mode: str,
    now: datetime,
) -> list[PlannedUnit]:
    planned_units: list[PlannedUnit] = []
    for source in sources:
        provider = source.provider
        prs_enabled = _prs_dataset_enabled(provider, datasets)
        family_specs: list[tuple[IntegrationDataset, DatasetSpec]] = []
        pr_social_specs: list[tuple[IntegrationDataset, DatasetSpec]] = []
        testops_specs: list[tuple[IntegrationDataset, DatasetSpec]] = []
        for dataset in datasets:
            spec = get_dataset_spec(provider, dataset.dataset_key)
            if spec is None or not spec.supported:
                continue

            # CHAOS-2721 (AD-3) / CHAOS-4078: family datasets are collapsed
            # into a single composite unit per (source, window) below instead
            # of one unit each. Defer them all here, before the routes_to_river
            # gate -- an alias member is never independently routable by
            # design (that is exactly what makes it an alias), so a per-alias
            # gate would silently drop it instead of folding it.
            if dataset.dataset_key in _WORK_ITEM_FAMILY_DATASETS:
                family_specs.append((dataset, spec))
                continue
            if dataset.dataset_key in _PR_SOCIAL_DATASETS:
                pr_social_specs.append((dataset, spec))
                continue
            if dataset.dataset_key in _TESTOPS_DATASETS:
                testops_specs.append((dataset, spec))
                continue

            # CHAOS-4054: a unit is only ever minted for an identity the
            # capability matrix says is independently plannable. A pair that
            # is not route-ready is not shipped -- minting one guarantees an
            # unserviceable unit (CHAOS-4047).
            #
            # This reads the checked-in matrix ONLY. The transitional
            # switch-consultation this replaces was correct while the
            # WORKER_*_ENABLED plane existed; that plane is gone, so plan-time
            # admission is now a pure capability fact, symmetric with the Go
            # scheduler's BuildScheduledPlan.
            if not routes_to_river(provider, dataset.dataset_key):
                continue

            processor_flags = dict(spec.processor_flags)

            windows = _resolve_windows(
                session=session,
                request=request,
                mode=mode,
                org_id=integration.org_id,
                source_provider=provider,
                watermark_source_key=source.external_id,
                dataset_key=dataset.dataset_key,
                watermark_behavior=spec.watermark_behavior,
                now=now,
                integration=integration,
                dataset=dataset,
            )
            for window_start, window_end in windows:
                planned_units.append(
                    PlannedUnit(
                        org_id=integration.org_id,
                        integration_id=str(integration.id),
                        source_id=str(source.id),
                        provider=provider,
                        dataset_key=dataset.dataset_key,
                        cost_class=spec.default_cost_class.value,
                        mode=mode,
                        window_start=window_start,
                        window_end=window_end,
                        processor_flags=dict(processor_flags),
                    )
                )

        family_units = _build_work_item_family_units(
            session=session,
            request=request,
            integration=integration,
            source=source,
            provider=provider,
            mode=mode,
            now=now,
            family_specs=family_specs,
            prs_enabled=prs_enabled,
        )
        # Individual family ALIASES are deliberately unchecked above -- their
        # admission is the atomic-family collapse's business. The CANONICAL
        # claim this collapse emits ("work-items") is an ordinary plannable
        # identity, so gate it here, after collapse, the same way every
        # non-family dataset already is.
        family_units = [
            unit
            for unit in family_units
            if unit.dataset_key != _FAMILY_CANONICAL_DATASET_KEY
            or routes_to_river(provider, unit.dataset_key)
        ]
        planned_units.extend(family_units)

        # CHAOS-4078: fold the PR-social (prs/pr-reviews/pr-comments -> prs)
        # and TestOps (cicd/tests -> cicd) alias families onto their canonical
        # writer, the same shape as the work-item family above but
        # non-atomic: only the datasets this org actually enabled contribute
        # a window and a completion flag.
        for canonical_key, specs in (
            (_PR_SOCIAL_CANONICAL_DATASET_KEY, pr_social_specs),
            (_TESTOPS_CANONICAL_DATASET_KEY, testops_specs),
        ):
            fold_units = _build_fold_family_units(
                session=session,
                request=request,
                integration=integration,
                source=source,
                provider=provider,
                mode=mode,
                now=now,
                family_specs=specs,
                canonical_dataset_key=canonical_key,
            )
            fold_units = [
                unit
                for unit in fold_units
                if routes_to_river(provider, unit.dataset_key)
            ]
            planned_units.extend(fold_units)
    return planned_units


# ---------------------------------------------------------------------------
# D1 depth resolver — reusable by WS-C and WS-D
# ---------------------------------------------------------------------------

_DEFAULT_INITIAL_SYNC_DEPTH_DAYS: int = 30


# ---------------------------------------------------------------------------
# HEAVY incremental window ratchet (CHAOS-3412)
# ---------------------------------------------------------------------------

_DEFAULT_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS: int = 7


def _incremental_heavy_max_window_days() -> int:
    """Max INCREMENTAL window span (days) for a HEAVY-cost-class dataset.

    Reads ``SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS``; falls back to 7 days.

    CHAOS-3412: unit cost is linear in window span (see
    ``providers/github/budget._scaled_units``). A HEAVY dataset cold-starting on
    a wide ``initial_sync_depth`` (or resuming from a long-stale watermark) used
    to plan ONE window covering the whole span, which could not fit the sync
    budget — the unit was deferred, no watermark was stamped, and the next tick
    recomputed the identical unfittable span forever. Capping the span makes
    each tick affordable; the success path stamps the watermark at the unit's
    window END, so successive ticks ratchet forward until the dataset is caught
    up. 7 days matches the proven ``_backfill_windows`` chunk size.
    """
    raw = os.getenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
    if raw is not None:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            # Non-integer env override: fall through to the default below.
            pass
    return _DEFAULT_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS


def _effective_heavy_max_window_days() -> int:
    """The HEAVY window cap, clamped to strictly exceed the watermark overlap.

    CHAOS-3412: the incremental read subtracts ``SYNC_WATERMARK_OVERLAP`` from
    the stored watermark, so a capped HEAVY window spans
    ``[W - overlap, W - overlap + cap]``. When ``overlap >= cap`` that end lands
    at or before ``W`` — and ``set_watermark`` enforces a monotonic advance
    (``max(existing, new)``, see ``sync.watermarks``), so the write is silently
    DISCARDED. Every later tick then re-plans the identical slice, re-fetches it,
    and reports SUCCESS while the watermark never moves: the same permanent
    stall this ticket exists to kill, wearing a different hat.

    Clamp the cap to ``floor(overlap_days) + 1``, which is strictly greater than
    the overlap for any real overlap value, so every successful capped run
    advances the watermark by a positive amount.

    The clamp is LOUD but never fatal. Refusing to plan would reproduce the
    do-nothing failure mode; a visibly clamped window is wider and more expensive
    than the operator asked for, but it makes progress, and the warning names
    both values so the misconfiguration can be corrected.
    """
    cap_days = _incremental_heavy_max_window_days()
    overlap_seconds = _watermark_overlap_seconds()
    if overlap_seconds <= 0:
        return cap_days
    # floor(overlap_days) + 1 > overlap_days for every real overlap.
    min_cap_days = overlap_seconds // _SECONDS_PER_DAY + 1
    if min_cap_days <= cap_days:
        return cap_days
    # Warn ONCE per distinct (cap, overlap) per process. This helper runs once
    # per (source x heavy dataset), so an unguarded warning would emit hundreds
    # of identical lines per plan on a large org and bury whatever else the
    # operator needs to see. Both values are restart-loaded env settings, so a
    # single line per process is the complete signal.
    warn_key = (cap_days, overlap_seconds)
    if warn_key in _WARNED_CAP_CLAMPS:
        return min_cap_days
    _WARNED_CAP_CLAMPS.add(warn_key)
    logger.warning(
        "sync.planner.heavy_window_cap_clamped_below_watermark_overlap",
        extra={
            "configured_cap_days": cap_days,
            "watermark_overlap_seconds": overlap_seconds,
            "effective_cap_days": min_cap_days,
            "reason": (
                "SYNC_WATERMARK_OVERLAP >= SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS "
                "would stall the HEAVY incremental ratchet: every capped window "
                "would end at or before its own watermark and the monotonic "
                "watermark write would be discarded. Widening the cap so each "
                "run makes progress. Lower SYNC_WATERMARK_OVERLAP or raise "
                "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS to remove this clamp."
            ),
        },
    )
    return min_cap_days


def heavy_ratchet_net_advance_seconds() -> int:
    """How far a successful capped HEAVY tick moves the watermark, in seconds.

    CHAOS-3430: this is NOT the window cap. The incremental read subtracts
    ``SYNC_WATERMARK_OVERLAP`` from the stored watermark, so a capped window
    spans ``[W - overlap, W - overlap + cap]`` and the next watermark lands at
    its END — a net forward movement of ``cap - overlap`` per tick, not ``cap``.

    Anything estimating how many scheduled ticks a trailing dataset still needs
    must divide by THIS, not by the cap. Dividing by the cap understates the
    remaining catch-up by the ratio between the two: with a six-day overlap on a
    seven-day cap, by a factor of seven.

    Exposed from the planner rather than recomputed by callers so the estimate
    can never drift from the window arithmetic it describes — the same reason
    :func:`_effective_heavy_max_window_days` is the single source for the cap.
    The clamp in that function guarantees a positive result; the floor here is a
    belt-and-braces guard so a caller dividing by this can never hit zero.
    """
    cap_seconds = _effective_heavy_max_window_days() * _SECONDS_PER_DAY
    return max(1, cap_seconds - _watermark_overlap_seconds())


def _watermark_stamping_window(
    window_start: datetime | None,
    window_end: datetime,
    now: datetime,
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    """Normalize a window for any mode whose SUCCESS path stamps a watermark.

    Both INCREMENTAL and FULL_RESYNC stamp the watermark at the unit's window END
    (``sync_units.py`` gates on exactly those two modes), so both inherit the same
    two failure modes. They are enforced HERE, once, rather than per branch —
    CHAOS-3412 found the same defect in both branches, and one shared rule is also
    one rule for any reimplementation of this contract to mirror.

    1. The end is clamped to ``now``. A future ``before`` would otherwise persist
       a FUTURE watermark, and the next run would start in the future and silently
       skip everything up to it.
    2. An empty or inverted window (``end <= start``) plans ZERO units. Persisting
       ``since_at >= before_at`` is worse than useless: ``window_span_days`` floors
       a negative span to 1 (``sync/budget_types.py``) and ``_scaled_units``
       multiplies by that floor, so an inverted unit is admitted at the CHEAPEST
       possible cost, fetches nothing, and finalizes SUCCESS — a false coverage
       claim. ``set_watermark`` is monotonic, so the stamp itself is harmless; the
       unit is the problem.

    BACKFILL is deliberately NOT routed through here: it never stamps a watermark
    (CHAOS-2514) and validates its own bounds in ``_backfill_windows``.
    """
    window_end = min(window_end, now)
    if window_start is not None:
        start = _as_utc(window_start)
        if start > now:
            # CHAOS-3412: a resolved start AHEAD of now can only come from a
            # corrupt watermark (a provider-supplied watermark_at derived from a
            # source record with a skewed/bad timestamp, or a future window end
            # persisted by pre-fix planner code). Left alone it is fatal: rule
            # (b) below would plan ZERO units, the run finalizes FAILED, and with
            # no unit there is nothing to re-stamp the watermark — a permanent
            # stall, and ``set_watermark`` is monotonic so nothing else can lower
            # it either. Clamp back far enough that a real window plans, and warn.
            recovery_seconds = max(
                _watermark_overlap_seconds(), _FUTURE_WATERMARK_RECOVERY_SECONDS
            )
            healed = now - timedelta(seconds=recovery_seconds)
            logger.warning(
                "sync.planner.future_watermark_clamped",
                extra={
                    "resolved_window_start": start.isoformat(),
                    "now": now.isoformat(),
                    "watermark_overlap_seconds": _watermark_overlap_seconds(),
                    "healed_window_start": healed.isoformat(),
                    "reason": (
                        "resolved window start is ahead of now, which means the "
                        "stored watermark is corrupt. Planning a bounded recovery "
                        "window so a unit runs and re-stamps a sane watermark. "
                        "Records between the true last-synced point and this "
                        "recovery window are NOT re-fetched — run a bounded "
                        "backfill if that span matters."
                    ),
                },
            )
            window_start = healed
            start = healed
        if window_end <= start:
            return ()
    return ((window_start, window_end),)


def _is_heavy_dataset(provider: str, dataset_key: str) -> bool:
    """True when the (provider, dataset) pair is registered CostClass.HEAVY."""
    spec = get_dataset_spec(provider, dataset_key)
    return spec is not None and spec.default_cost_class is CostClass.HEAVY


# ---------------------------------------------------------------------------
# Linear backfill chunk policy (CHAOS-2710, rebalanced in CHAOS-2717)
# ---------------------------------------------------------------------------

_DEFAULT_LINEAR_BACKFILL_MAX_WINDOW_DAYS: int = 14

_LINEAR_WORK_ITEM_DATASETS: frozenset[str] = frozenset(
    {
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
    }
)


def _linear_backfill_max_window_days() -> int:
    """Return the max chunk window (days) for Linear work-item-family backfills.

    Reads LINEAR_BACKFILL_MAX_WINDOW_DAYS from the environment; falls back to a
    default of 14 days. CHAOS-2717 bounds each window's issue crawl to its own
    slice via the provider's updatedAt gte/lte filter, so a window no longer
    re-scans to now. The window size then trades two opposing budgets:
    smaller windows multiply the per-window fixed overhead (teams + cycles are
    re-fetched per unit) and push the per-hour request count back toward Linear's
    rate limit, while larger windows lengthen a single unit's crawl and risk the
    worker lease/soft-timeout budget (see docs/ops/workers.md). 14 days is the
    balance; operators can override per tenant.
    """
    raw = os.getenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS")
    if raw is not None:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            # Non-integer env override: fall through to the conservative default below.
            pass
    return _DEFAULT_LINEAR_BACKFILL_MAX_WINDOW_DAYS


def _is_linear_work_item_family(provider: str, dataset_key: str) -> bool:
    """True when the provider is linear AND the dataset is in the work-item family."""
    return provider == "linear" and dataset_key in _LINEAR_WORK_ITEM_DATASETS


# ---------------------------------------------------------------------------
# CHAOS-2721 (AD-3): work-item-family plan-time collapse
# ---------------------------------------------------------------------------
#
# The five work-item-family datasets are all produced by a SINGLE
# ``run_work_items_sync_job`` crawl (labels/projects/history/comments are
# bookkeeping over the same issue crawl). Emitting one unit per dataset re-ran
# the full ingest 5x. The planner instead emits ONE composite unit (canonical
# ``dataset_key="work-items"``) carrying a boolean ``family_dataset_<key>`` flag
# per participating dataset; the worker fans those back out into per-dataset
# watermarks + audit metadata on success. GitHub's activated Go route is the
# deliberate exception: every canonical GitHub unit claims all five aliases,
# even when one is caught up, because the route owns one indivisible writer
# family. GitLab, Jira, and Linear retain their contributing-dataset flags.
_WORK_ITEM_FAMILY_DATASET_ORDER = WORK_ITEM_DATASETS
_WORK_ITEM_FAMILY_DATASETS: frozenset[str] = frozenset(_WORK_ITEM_FAMILY_DATASET_ORDER)
_FAMILY_CANONICAL_DATASET_KEY = FAMILY_CANONICAL_DATASET_KEY
# Compatibility alias for route validation consumers. The implementation lives
# in the pure family-flags module so coverage imports do not initialize planner
# dependencies.
_family_dataset_flag = family_dataset_flag

# CHAOS-4078: the PR-social (prs/pr-reviews/pr-comments) and TestOps
# (cicd/tests) alias families. Unlike the work-item family above, folding
# these is non-atomic (FamilyExecutionMode.FOLD_CONTRIBUTING) -- see
# _build_fold_family_units.
_PR_SOCIAL_DATASETS: frozenset[str] = frozenset(PR_SOCIAL_DATASETS)
_PR_SOCIAL_CANONICAL_DATASET_KEY = PR_SOCIAL_CANONICAL_DATASET_KEY
_TESTOPS_DATASETS: frozenset[str] = frozenset(TESTOPS_DATASETS)
_TESTOPS_CANONICAL_DATASET_KEY = TESTOPS_CANONICAL_DATASET_KEY


def _build_fold_family_units(
    *,
    session: Session,
    request: SyncPlanRequest,
    integration: Integration,
    source: IntegrationSource,
    provider: str,
    mode: str,
    now: datetime,
    family_specs: list[tuple[IntegrationDataset, DatasetSpec]],
    canonical_dataset_key: str,
) -> list[PlannedUnit]:
    """Collapse a non-atomic alias family onto its canonical writer
    (CHAOS-4078: PR-social prs/pr-reviews/pr-comments -> prs, TestOps
    cicd/tests -> cicd).

    Shaped like ``_build_work_item_family_units`` (same window-merge
    machinery), but deliberately NOT atomic: only the datasets this org
    actually enabled contribute a window and receive a
    ``family_dataset_<key>`` completion flag. An org that enables only
    ``pr-comments`` gets one ``prs`` unit carrying `family_dataset_pr_comments`
    alone -- no sibling is forced along the way the work-item family forces
    all five.
    """
    if not family_specs:
        return []

    canonical_spec = get_dataset_spec(provider, canonical_dataset_key)
    if canonical_spec is None:
        # Provider does not support this family's canonical dataset (should
        # not happen for github/gitlab, the only two PR-social/TestOps
        # providers). Stay defensive: never synthesize a unit for a dataset
        # the provider cannot run.
        return []

    # Each family member owns its own watermark identity (org, source, key),
    # so resolve windows independently using each member's OWN configured
    # row, then merge (earliest start, latest end). This is what keeps
    # watermark loading correct without any special-casing: a
    # ``pr-comments``-only selection reads the ``pr-comments`` watermark row
    # directly, never a canonical-keyed row that selection never wrote.
    resolved = [
        (
            dataset,
            _resolve_windows(
                session=session,
                request=request,
                mode=mode,
                org_id=integration.org_id,
                source_provider=provider,
                watermark_source_key=source.external_id,
                dataset_key=dataset.dataset_key,
                watermark_behavior=spec.watermark_behavior,
                now=now,
                integration=integration,
                dataset=dataset,
            ),
        )
        for dataset, spec in family_specs
    ]
    # A family member already synced past the requested end resolves to ZERO
    # windows; drop it before the merge (mirrors _build_work_item_family_units).
    contributing = [(dataset, windows) for dataset, windows in resolved if windows]
    if not contributing:
        return []
    composite_windows = _merge_family_windows([windows for _, windows in contributing])

    processor_flags: dict[str, bool] = dict(canonical_spec.processor_flags)
    # CHAOS-4078: fan the completion flag -- and each member's own processor
    # flags -- back only to the datasets that actually CONTRIBUTED a window
    # this tick, never every configured member. A caught-up sibling (e.g.
    # "tests" already past the requested end while "cicd" still has work)
    # must not be stamped as processed: it did not run and its watermark
    # must not be touched. This is the opposite of the work-item family's
    # unconditional all-members stamp, which is a deliberate exception for
    # that one atomic family, not the default here.
    #
    # The stamped unit keeps the CANONICAL identity's own cost class
    # unconditionally, never a member's -- CHAOS-4078 review round 3: an
    # earlier version of this fold tried to upgrade to the heaviest
    # contributing member's class (e.g. stamping "heavy" under the canonical
    # "cicd" dataset_key when "tests" contributed), but the Go worker's
    # providersync.Unit.Validate() requires cost_class to exactly match the
    # checked-in capability registry's value for (provider, dataset_key) --
    # "cicd" is registered MEDIUM, full stop, so a "heavy"-stamped cicd unit
    # fails claim validation after being marked running and strands the run.
    # The heavy incremental-window ratchet already caps "tests"'s own window
    # correctly via its own spec in _resolve_windows above; that per-member
    # window sizing is untouched by this.
    contributing_keys = {dataset.dataset_key for dataset, _ in contributing}
    for dataset, spec in family_specs:
        if dataset.dataset_key not in contributing_keys:
            continue
        processor_flags[family_dataset_flag(dataset.dataset_key)] = True
        processor_flags.update(spec.processor_flags)

    return [
        PlannedUnit(
            org_id=integration.org_id,
            integration_id=str(integration.id),
            source_id=str(source.id),
            provider=provider,
            dataset_key=canonical_dataset_key,
            cost_class=canonical_spec.default_cost_class.value,
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            processor_flags=dict(processor_flags),
        )
        for window_start, window_end in composite_windows
    ]


def _is_non_project_jira_source(session: Session, source: IntegrationSource) -> bool:
    """Whether ``source`` is the known-bad shape CHAOS-4582 fixed at the
    writer: a jira ``source_type='project'`` row whose ``external_id`` is
    not a real per-project key. Signals, in order, any one sufficient to
    resolve the question:

    1. ``metadata_.explicit_project_scope`` -- the writer's own marker
       (``_non_git_source_rows``) for a NEW row created from an explicit
       ``project_key``/``project_id``. Always wins: never suppress it.
    2. ``metadata_.org_wide_placeholder`` -- the SAME typed marker Linear's
       writer already sets for ITS org-wide mode; recognized here in case a
       future writer ever legitimately reuses it for jira.
    3. If ``external_id`` equals the provider name itself
       (case-insensitive) -- the exact literal ("JIRA") the pre-fix writer
       fell back to when a config had no explicit project scope (live
       evidence, org 70d529e0, CHAOS-4582) -- look up the LEGACY row's own
       ``sync_configurations.sync_options`` (via
       ``metadata_.planner_managed_sync_config_id``, which every non-git
       source carries) for an explicit ``project_key``/``project_id``.
       Codex review (CHAOS-4582, P2): a row created BEFORE this PR has no
       ``explicit_project_scope`` marker even if a caller legitimately named
       a real project "JIRA" -- the writer's old code produced an identical
       shape for both cases, so external_id alone can never disambiguate a
       PRE-EXISTING row. The persisted sync_options this row was ACTUALLY
       created from can: if the config it came from explicitly asked for a
       project literally named "JIRA" (case-insensitive), this is that
       project, not the fallback -- planning proceeds. This closes the gap
       for legacy data without a migration; a config config the source no
       longer references (renamed org, deleted config) falls through to the
       known-bad classification, matching this ticket's live evidence
       (org 70d529e0's bad row's config carries no project scope at all).
    """
    metadata = getattr(source, "metadata_", None) or {}
    if metadata.get("explicit_project_scope"):
        return False
    if metadata.get("org_wide_placeholder"):
        return True
    if str(source.external_id or "").strip().lower() != "jira":
        return False
    config_id = metadata.get("planner_managed_sync_config_id")
    if config_id:
        config = (
            session.query(SyncConfiguration)
            .filter(SyncConfiguration.id == config_id)
            .one_or_none()
        )
        if config is not None:
            sync_options = config.sync_options or {}
            # Mirrors api/admin/routers/sync.py's _non_git_explicit_source_id
            # precedence exactly (project_id > project_key > team_id > repo).
            # Not imported directly: that module imports FROM this one
            # (BackfillSelector), so the reverse import would be circular.
            explicit_id = (
                sync_options.get("project_id")
                or sync_options.get("project_key")
                or sync_options.get("team_id")
                or sync_options.get("repo")
            )
            if explicit_id is not None and str(explicit_id).strip().lower() == "jira":
                return False
    return True


def _build_work_item_family_units(
    *,
    session: Session,
    request: SyncPlanRequest,
    integration: Integration,
    source: IntegrationSource,
    provider: str,
    mode: str,
    now: datetime,
    family_specs: list[tuple[IntegrationDataset, DatasetSpec]],
    prs_enabled: bool,
) -> list[PlannedUnit]:
    """Collapse the enabled work-item-family datasets into ONE composite unit
    per (source, window) (CHAOS-2721, AD-3)."""
    if not family_specs:
        return []

    if provider.strip().lower() == "jira" and _is_non_project_jira_source(
        session, source
    ):
        # CHAOS-4582 (defense-in-depth): the writer (_non_git_source_rows,
        # api/admin/routers/sync.py) no longer materializes this shape for a
        # NEW jira config, but a pre-existing row (or a future writer bug of
        # the same class) could still reach here. Refuse to plan rather than
        # emit a unit that is guaranteed to fail every attempt -- Jira's
        # work-items route requires a real per-project source (unlike
        # Linear's org-wide search mode), so JQL built from a non-project
        # external_id (e.g. the literal provider name) 400s against Jira
        # every time. Log loud with the source id so an operator can find
        # and fix the source, then return zero units -- CONTAINED to this
        # one source, never aborting the rest of this integration's plan.
        logger.error(
            "sync.plan.jira_source_not_a_project org_id=%s source_id=%s "
            "external_id=%r: refusing to plan a work-items unit for a "
            "non-project Jira source (error_category=jira_source_not_a_project)",
            integration.org_id,
            source.id,
            source.external_id,
        )
        return []

    canonical_spec = get_dataset_spec(provider, _FAMILY_CANONICAL_DATASET_KEY)
    if canonical_spec is None:
        # Provider has no work-items dataset in the registry (should not happen
        # for the four work-item providers). Stay defensive: do not synthesize a
        # unit for a dataset the provider cannot run.
        return []

    # Each family dataset owns its own watermark identity (org, source, key), so
    # resolve windows independently then merge index-aligned (earliest start so
    # the single crawl covers every enabled dataset; over-fetch is safe because
    # set_watermark is monotonic).
    resolved = [
        (
            dataset,
            _resolve_windows(
                session=session,
                request=request,
                mode=mode,
                org_id=integration.org_id,
                source_provider=provider,
                watermark_source_key=source.external_id,
                dataset_key=dataset.dataset_key,
                watermark_behavior=spec.watermark_behavior,
                now=now,
                integration=integration,
                dataset=dataset,
            ),
        )
        for dataset, spec in family_specs
    ]
    # CHAOS-3412: a family dataset already synced past the requested end resolves
    # to ZERO windows. Drop it before the index-aligned merge — otherwise its
    # empty tuple reads as a window-count mismatch and ``_merge_family_windows``
    # raises, taking down a plan that is merely partially caught up. The
    # remaining datasets still collapse into one composite; the dropped dataset
    # simply has nothing to do this tick and keeps its (later) watermark, which
    # the monotonic ``set_watermark`` preserves.
    contributing = [(dataset, windows) for dataset, windows in resolved if windows]
    if not contributing:
        return []
    composite_windows = _merge_family_windows([windows for _, windows in contributing])

    processor_flags: dict[str, bool] = dict(canonical_spec.processor_flags)
    # CHAOS-3606: the native work-item route has one all-five-alias writer. A
    # caught-up sibling still has no independent Python owner while this
    # canonical unit runs, so its flag records atomic route ownership rather
    # than whether that sibling contributed a window to this tick's merge.
    # CHAOS-4054: this is now unconditional. It used to be gated on each
    # provider's family switch, which meant the claim shape a unit carried
    # depended on a deployment variable rather than on which writer owns it.
    # The merged window above remains derived only from non-empty inputs.
    family_flag_datasets = _WORK_ITEM_FAMILY_DATASET_ORDER
    for dataset_key in family_flag_datasets:
        processor_flags[family_dataset_flag(dataset_key)] = True
    if provider == "github":
        # CHAOS-646: thread the PRS-as-work-items signal onto the composite so
        # ``_work_item_kwargs`` sets ``include_pull_requests`` correctly.
        processor_flags["sync_prs"] = prs_enabled

    return [
        PlannedUnit(
            org_id=integration.org_id,
            integration_id=str(integration.id),
            source_id=str(source.id),
            provider=provider,
            dataset_key=_FAMILY_CANONICAL_DATASET_KEY,
            cost_class=canonical_spec.default_cost_class.value,
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            processor_flags=dict(processor_flags),
        )
        for window_start, window_end in composite_windows
    ]


def _merge_family_windows(
    per_dataset_windows: list[tuple[tuple[datetime | None, datetime | None], ...]],
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    """Index-aligned merge of each enabled family dataset's resolved windows.

    The family shares one chunk schedule per (provider, mode): incremental and
    full_resync resolve exactly one window; backfill resolves an identical chunk
    count (chunk policy keys on provider + family membership, not the specific
    dataset_key). So the per-dataset tuples are the same length and no window is
    dropped. Only the start can differ (per-dataset incremental watermark) — take
    the earliest so the single crawl covers every enabled dataset.
    """
    if not per_dataset_windows:
        return ()
    lengths = {len(windows) for windows in per_dataset_windows}
    if len(lengths) > 1:
        raise ValueError(
            "work-item-family datasets resolved to mismatched window counts: "
            f"{sorted(lengths)}"
        )
    merged: list[tuple[datetime | None, datetime | None]] = []
    for slice_windows in zip(*per_dataset_windows):
        starts = [start for start, _end in slice_windows]
        ends = [end for _start, end in slice_windows]
        merged.append((_earliest_bound(starts), _latest_bound(ends)))
    return tuple(merged)


def _earliest_bound(bounds: list[datetime | None]) -> datetime | None:
    # ``None`` means "no lower bound" (crawl from the beginning) and therefore
    # wins as the earliest start.
    concrete = [bound for bound in bounds if bound is not None]
    if len(concrete) != len(bounds):
        return None
    return min(concrete) if concrete else None


def _latest_bound(bounds: list[datetime | None]) -> datetime | None:
    # ``None`` means "no upper bound" (open-ended) and wins as the latest end.
    # window_end is concrete for every family mode today; stay symmetric anyway.
    concrete = [bound for bound in bounds if bound is not None]
    if len(concrete) != len(bounds):
        return None
    return max(concrete) if concrete else None


def resolve_initial_sync_depth(
    session: Session,
    integration: Integration,
    dataset: IntegrationDataset,
) -> int:
    """Return the effective initial-sync depth in days for a (integration, dataset) pair.

    Resolution order (D1):
      1. ``IntegrationDataset.options["initial_sync_depth"]`` — per-dataset override
      2. ``Integration.config["initial_sync_depth"]`` — integration-level setting
      3. Default: 30 days
    Then cap by the org's tier ``backfill_days`` limit (None = unlimited).
    """
    # 1. Dataset-level override
    dataset_depth = (dataset.options or {}).get("initial_sync_depth")
    if dataset_depth is not None:
        depth = int(dataset_depth)
    else:
        # 2. Integration-level config
        integration_depth = (integration.config or {}).get("initial_sync_depth")
        if integration_depth is not None:
            depth = int(integration_depth)
        else:
            # 3. Default
            depth = _DEFAULT_INITIAL_SYNC_DEPTH_DAYS

    # Apply tier backfill_days cap
    tier_cap = _get_tier_backfill_days_cap(session, integration.org_id)
    if tier_cap is not None:
        depth = min(depth, tier_cap)

    return max(depth, 1)


def _get_tier_backfill_days_cap(session: Session, org_id: str) -> int | None:
    """Return the tier backfill_days cap for the org, or None if unlimited.

    None means the tier is genuinely unlimited (enterprise) — do NOT cap.
    The only failure this function handles directly is a non-UUID org_id
    (e.g. test fixtures): returns the community default (30) so depth is
    bounded rather than unbounded.
    Missing-table OperationalErrors are swallowed inside TierLimitService, but
    PostgreSQL still marks the transaction failed after the underlying query
    error. Keep that failure inside a planner-owned savepoint so the outer
    planning transaction can continue to flush SyncRun/SyncRunUnit rows.
    """
    try:
        import uuid as _uuid

        from dev_health_ops.api.services.licensing import TierLimitService

        org_uuid = _uuid.UUID(str(org_id))  # raises ValueError for non-UUID strings
        nested = session.begin_nested()
        try:
            svc = TierLimitService(session)
            cap = svc.get_limit(org_uuid, "backfill_days")
        except Exception:
            nested.rollback()
            return _DEFAULT_INITIAL_SYNC_DEPTH_DAYS
        else:
            nested.rollback()
        # None is the SUCCESS value for unlimited/enterprise tiers — do not cap.
        if cap is None:
            return None
        return int(cap)
    except ValueError:
        # Non-UUID org_id (e.g. test fixtures): return community default.
        return _DEFAULT_INITIAL_SYNC_DEPTH_DAYS


def _resolve_windows(
    *,
    session: Session,
    request: SyncPlanRequest,
    mode: str,
    org_id: str,
    source_provider: str,
    watermark_source_key: str,
    dataset_key: str,
    watermark_behavior: WatermarkBehavior,
    now: datetime,
    integration: Integration,
    dataset: IntegrationDataset,
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    if mode == SyncRunMode.INCREMENTAL.value:
        window_start: datetime | None = None
        if watermark_behavior == WatermarkBehavior.INCREMENTAL:
            window_start = get_watermark_with_overlap(
                session, org_id, watermark_source_key, dataset_key
            )
            if window_start is None:
                # Cold-start: INCREMENTAL dataset with no watermark yet — use depth.
                depth = resolve_initial_sync_depth(session, integration, dataset)
                window_start = now - timedelta(days=depth)
        # WatermarkBehavior.NONE datasets keep window_start=None (registered behavior).
        window_end = _request_before_or_now(request, now)
        if window_start is not None and _is_heavy_dataset(source_provider, dataset_key):
            # CHAOS-3412 ratchet: cap the span so a HEAVY unit can fit the sync
            # budget. Applies to BOTH cold-start and behind-watermark cases — a
            # long-idle org ratchets forward one capped window per tick. Depth
            # resolution (and its tier backfill_days cap) is untouched; only the
            # window END moves in.
            # ``window_start`` may be naive when it came straight off a stored
            # watermark row; normalize for the comparison only — the value
            # persisted as ``since_at`` is left exactly as resolved.
            capped_end = _as_utc(window_start) + timedelta(
                days=_effective_heavy_max_window_days()
            )
            window_end = min(window_end, capped_end)
        return _watermark_stamping_window(window_start, window_end, now)

    if mode == SyncRunMode.BACKFILL.value:
        return _backfill_windows(
            request, provider=source_provider, dataset_key=dataset_key
        )

    if mode == SyncRunMode.FULL_RESYNC.value:
        # full_resync: use configured depth for all datasets (CHAOS-2569).
        depth = resolve_initial_sync_depth(session, integration, dataset)
        window_start_fr = now - timedelta(days=depth)
        return _watermark_stamping_window(
            window_start_fr, _request_before_or_now(request, now), now
        )

    return ((None, _request_before_or_now(request, now)),)


def _backfill_windows(
    request: SyncPlanRequest,
    *,
    provider: str = "",
    dataset_key: str = "",
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    if request.since is None or request.before is None:
        raise ValueError("Backfill sync planning requires since and before")

    since = _as_utc(request.since)
    before = _as_utc(request.before)
    if since >= before:
        raise ValueError("Backfill since must be before")

    if _is_linear_work_item_family(provider, dataset_key):
        chunk_days = _linear_backfill_max_window_days()
    else:
        chunk_days = 7
    chunks = chunk_date_range(
        since=since.date(), before=before.date(), chunk_days=chunk_days
    )
    return tuple(
        _chunk_to_window(chunk_since, chunk_before, since, before)
        for chunk_since, chunk_before in chunks
    )


def _chunk_to_window(
    chunk_since,
    chunk_before,
    requested_since: datetime,
    requested_before: datetime,
) -> tuple[datetime, datetime]:
    window_start = (
        requested_since
        if chunk_since == requested_since.date()
        else datetime.combine(chunk_since, time.min, tzinfo=timezone.utc)
    )
    window_end = (
        requested_before
        if chunk_before == requested_before.date()
        else datetime.combine(chunk_before, time.max, tzinfo=timezone.utc)
    )
    return window_start, window_end


def _request_before_or_now(request: SyncPlanRequest, now: datetime) -> datetime:
    if request.before is None:
        return now
    return _as_utc(request.before)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _validate_mode(mode: str) -> str:
    valid_modes = {item.value for item in SyncRunMode}
    if mode not in valid_modes:
        raise ValueError(f"Unsupported sync run mode: {mode}")
    return mode


def _coerce_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: {value}") from exc


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
