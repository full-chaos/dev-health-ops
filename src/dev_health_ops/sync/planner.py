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
  * total_units on the persisted SyncRun equals len(unit_ids).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import TYPE_CHECKING

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
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.datasets import (
    DatasetKey,
    DatasetSpec,
    WatermarkBehavior,
    get_dataset_spec,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.watermarks import get_watermark_with_overlap

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
    org_id: str
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

    integration = _load_integration(session, request.integration_id, request.org_id)
    mode = _validate_mode(request.mode)
    # Freeze this run's auth at plan time (CHAOS-2755): resolve the credential
    # ONCE here so every later phase reads the run-stamped credential and a
    # mid-run credential edit can never produce a mixed-auth run.
    credential_id, credential_fp, auth_source = _resolve_credential_stamp(
        session, integration
    )
    sources = _load_enabled_sources(session, integration, request.source_ids)
    dataset_keys = _ensure_security_dataset_for_scheduled_code_host_sync(
        session, integration, request
    )
    datasets = _load_enabled_datasets(session, integration, dataset_keys)
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
    session.add(
        SyncRunReferenceDiscovery(
            org_id=integration.org_id,
            sync_run_id=sync_run.id,
            status="planned",
            attempts=0,
            available_at=now,
        )
    )
    session.flush()
    upsert_outbox_wakeup(
        session,
        sync_run_id=sync_run.id,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=now,
        now=now,
    )

    return SyncRunPlan(
        sync_run_id=str(sync_run.id),
        total_units=len(unit_rows),
        unit_ids=tuple(str(unit.id) for unit in unit_rows),
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

    provider = str(integration.provider)
    integration_id = str(integration.id)

    if integration.credential_id is None:
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


_CODE_HOST_SECURITY_PROVIDERS = frozenset({"github", "gitlab"})
_SCHEDULED_SECURITY_TRIGGER = "schedule"


def _ensure_security_dataset_for_scheduled_code_host_sync(
    session: Session,
    integration: Integration,
    request: SyncPlanRequest,
) -> tuple[str, ...] | None:
    """Ensure normal scheduled code-host syncs also plan security ingestion.

    Historical sync configs only ran the security dataset when users explicitly
    selected the legacy ``security`` target. Normal scheduled GitHub/GitLab syncs
    should refresh repository security alerts alongside the rest of the code-host
    crawl, without overriding an operator-disabled security dataset row.
    """

    provider = str(integration.provider).lower()
    requested = request.dataset_keys
    if provider not in _CODE_HOST_SECURITY_PROVIDERS:
        return requested
    if request.triggered_by != _SCHEDULED_SECURITY_TRIGGER:
        return requested

    if requested is not None and DatasetKey.SECURITY.value not in requested:
        requested = (*requested, DatasetKey.SECURITY.value)

    security_dataset = (
        session.query(IntegrationDataset)
        .filter(
            IntegrationDataset.org_id == integration.org_id,
            IntegrationDataset.integration_id == integration.id,
            IntegrationDataset.dataset_key == DatasetKey.SECURITY.value,
        )
        .one_or_none()
    )
    if security_dataset is None:
        session.add(
            IntegrationDataset(
                org_id=integration.org_id,
                integration_id=integration.id,
                dataset_key=DatasetKey.SECURITY.value,
                is_enabled=True,
                options={"auto_enabled_by": "scheduled_code_host_sync"},
            )
        )
        session.flush()
    return requested


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
        for dataset in datasets:
            spec = get_dataset_spec(provider, dataset.dataset_key)
            if spec is None or not spec.supported:
                continue

            # CHAOS-2721 (AD-3): work-item-family datasets are collapsed into a
            # single composite unit per (source, window) below, instead of one
            # unit each. Defer them; a single work-items crawl already emits the
            # whole family.
            if dataset.dataset_key in _WORK_ITEM_FAMILY_DATASETS:
                family_specs.append((dataset, spec))
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

        planned_units.extend(
            _build_work_item_family_units(
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
        )
    return planned_units


# ---------------------------------------------------------------------------
# D1 depth resolver — reusable by WS-C and WS-D
# ---------------------------------------------------------------------------

_DEFAULT_INITIAL_SYNC_DEPTH_DAYS: int = 30


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
# per enabled dataset; the worker fans those back out into per-dataset
# watermarks + audit metadata on success. Provider-agnostic: github/gitlab/jira/
# linear all expose these keys.
_WORK_ITEM_FAMILY_DATASET_ORDER: tuple[str, ...] = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)
_WORK_ITEM_FAMILY_DATASETS: frozenset[str] = frozenset(_WORK_ITEM_FAMILY_DATASET_ORDER)
_FAMILY_CANONICAL_DATASET_KEY = "work-items"
_FAMILY_DATASET_FLAG_PREFIX = "family_dataset_"


def _family_dataset_flag(dataset_key: str) -> str:
    """Boolean processor-flag name marking an enabled work-item-family dataset."""
    return _FAMILY_DATASET_FLAG_PREFIX + dataset_key.replace("-", "_")


def family_dataset_keys_from_flags(
    processor_flags: Mapping[str, object] | None,
) -> list[str]:
    """Enabled work-item-family dataset keys encoded on a collapsed composite
    unit's ``processor_flags`` (CHAOS-2721), in canonical order.

    ``SyncTaskBootstrap.load`` bool-coerces every processor_flags value, so the
    composite cannot carry a *list* of enabled datasets — each is encoded as its
    own boolean ``family_dataset_<key>`` flag. This reader validates against the
    known family keys so a stray/unknown flag can never advance a bogus
    watermark or pollute audit metadata.
    """
    flags = processor_flags or {}
    return [
        key
        for key in _WORK_ITEM_FAMILY_DATASET_ORDER
        if bool(flags.get(_family_dataset_flag(key)))
    ]


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
    per_dataset_windows = [
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
        )
        for dataset, spec in family_specs
    ]
    composite_windows = _merge_family_windows(per_dataset_windows)

    processor_flags: dict[str, bool] = dict(canonical_spec.processor_flags)
    for dataset, _spec in family_specs:
        processor_flags[_family_dataset_flag(dataset.dataset_key)] = True
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
        return ((window_start, _request_before_or_now(request, now)),)

    if mode == SyncRunMode.BACKFILL.value:
        return _backfill_windows(
            request, provider=source_provider, dataset_key=dataset_key
        )

    if mode == SyncRunMode.FULL_RESYNC.value:
        # full_resync: use configured depth for all datasets (CHAOS-2569).
        depth = resolve_initial_sync_depth(session, integration, dataset)
        window_start_fr = now - timedelta(days=depth)
        return ((window_start_fr, _request_before_or_now(request, now)),)

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
    if since > before:
        raise ValueError("Backfill since must be before or equal to before")

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
