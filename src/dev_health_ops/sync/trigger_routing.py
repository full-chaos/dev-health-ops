"""SyncConfiguration to fan-out planner request routing.

Every ``SyncConfiguration`` is created integration-native (see
``api/admin/routers/sync.py``):

* a *parent* config gets ``integration_id`` set to its Integration;
* a *child* config gets both ``integration_id`` (the parent's
  Integration) and ``source_id`` (its own IntegrationSource).

The admin "Sync Now" endpoint and scheduled-sync beat route configs through the
fan-out planner (``plan_sync_run`` + ``dispatch_sync_run``). A config with no
linked integration cannot be routed and callers fail or skip explicitly instead
of falling back to deleted legacy workers.

The outbox and reconciler relay now recover committed PLANNED runs. This module's `mark_sync_run_failed` helper is kept as a legacy, best-effort fallback for the non-outbox path.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.models import SyncRunMode
from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.sync.planner import SyncPlanRequest

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dev_health_ops.models.settings import SyncConfiguration

logger = logging.getLogger(__name__)

_PLANNER_TAG_KEY = "planner_managed_sync_config_id"


# ---------------------------------------------------------------------------
# D5: full-resync intent mapping (CHAOS-2579)
# ---------------------------------------------------------------------------

_FULL_RESYNC_INTENT_ALIASES: frozenset[str] = frozenset(
    {"full_resync", "full-resync", "resync", "full_sync", "full-sync"}
)


def map_sync_mode(intent: str) -> str:
    """Map a caller-supplied sync intent string to a canonical ``SyncRunMode`` value.

    Callers that want a full-resync pass any of the recognised aliases; all
    other strings are returned unchanged so the planner's ``_validate_mode``
    gate catches invalid values.

    This is the single source of truth for full-resync intent mapping (D5,
    CHAOS-2579).  Trigger surfaces (admin API, scheduler) MUST call this
    before constructing a :class:`~dev_health_ops.sync.planner.SyncPlanRequest`
    when the user requests a full resync.
    """
    if intent in _FULL_RESYNC_INTENT_ALIASES:
        return SyncRunMode.FULL_RESYNC.value
    return intent


def _dataset_keys_for_config(config: SyncConfiguration) -> tuple[str, ...]:
    """Map a config's legacy ``sync_targets`` to integration dataset keys.

    Mirrors the dataset-key mapping used at create time so a child config's
    trigger replays exactly the datasets it covers.
    """
    targets = {str(t) for t in (config.sync_targets or []) if t is not None}
    if not targets:
        return ()
    keys: list[str] = []
    for spec in supported_datasets(config.provider):
        if targets.intersection(spec.legacy_targets):
            keys.append(spec.dataset_key)
    return tuple(keys)


def plan_request_for_config(
    config: SyncConfiguration,
    *,
    triggered_by: str,
    mode: str = "incremental",
) -> SyncPlanRequest | None:
    """Build a :class:`SyncPlanRequest` for an integration-linked config, else ``None``.

    Returns ``None`` when the config has no linked integration (no
    ``integration_id``). The integration planner is the only routing
    path (the legacy worker was removed in CHAOS-2647), so the caller must fail
    or skip when no planner route exists.

    Routing semantics:

    * **Parent config** (no ``source_id``): leave source and dataset
      scope unset. Callers that route planner-managed parents through
      :func:`planner_request_for_config_if_routed` narrow sources to the
      config-tagged ``IntegrationSource`` rows in that session-aware wrapper;
      direct callers keep the historic all-enabled integration fan-out.
    * **Child config** (``source_id`` set): scope the run to that one
      source, and to the dataset keys derived from the child's legacy targets so
      the trigger covers exactly what the child used to.

    Mode resolution (D4 / D5 compat): if the caller passes the default
    ``mode="incremental"`` and the config's ``sync_options`` carries a truthy
    ``full_resync`` flag (legacy worker semantics), the mode is promoted to
    ``SyncRunMode.FULL_RESYNC`` via :func:`map_sync_mode`. An explicit
    ``mode="backfill"`` or ``mode="full_resync"`` from the caller is never
    overridden.
    """
    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        return None

    source_id = getattr(config, "source_id", None)
    source_ids: tuple[str, ...] | None = None
    dataset_keys: tuple[str, ...] | None = None
    if source_id is not None:
        source_ids = (str(source_id),)
        child_dataset_keys = _dataset_keys_for_config(config)
        # Empty => fall back to all enabled datasets rather than an empty run.
        dataset_keys = child_dataset_keys or None

    # Promote incremental -> full_resync when the config's sync_options carry
    # the legacy full_resync flag (preserves the removed legacy worker's
    # full_resync promotion semantics). Only promote when the caller passed the
    # default "incremental" mode; an explicit backfill or full_resync from the
    # caller is never overridden.
    if mode == SyncRunMode.INCREMENTAL.value:
        sync_options = getattr(config, "sync_options", None) or {}
        if bool(sync_options.get("full_resync")):
            mode = map_sync_mode("full_resync")

    return SyncPlanRequest(
        integration_id=str(integration_id),
        org_id=str(config.org_id),
        mode=mode,
        triggered_by=triggered_by,
        source_ids=source_ids,
        dataset_keys=dataset_keys,
    )


def _planner_scoped_source_ids(
    session: Session, config: SyncConfiguration
) -> tuple[str, ...]:
    """Return enabled source ids explicitly tagged for a planner-managed parent."""
    from dev_health_ops.models.integrations import IntegrationSource

    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        return ()

    config_id = str(config.id)
    enabled_sources = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == config.org_id,
            IntegrationSource.integration_id == integration_id,
            IntegrationSource.is_enabled.is_(True),
        )
        .order_by(IntegrationSource.full_name, IntegrationSource.id)
        .all()
    )
    return tuple(
        str(source.id)
        for source in enabled_sources
        if str((source.metadata_ or {}).get(_PLANNER_TAG_KEY)) == config_id
    )


def planner_request_for_config_if_routed(
    session: Session,
    config: SyncConfiguration,
    *,
    triggered_by: str,
    mode: str = "incremental",
) -> SyncPlanRequest | None:
    """Build a planner request if the config routes to the integration planner.

    Planner-managed parent configs are session-scoped to enabled
    ``IntegrationSource`` rows tagged with that config id. This preserves the
    distinction between ``None`` (legacy all-enabled fan-out) and ``()`` (no
    user-selected enabled sources) while leaving child configs and flag-routed
    non-planner-managed parents on their existing semantics.
    """
    request = plan_request_for_config(config, triggered_by=triggered_by, mode=mode)
    if (
        request is not None
        and bool(getattr(config, "planner_managed", False))
        and getattr(config, "source_id", None) is None
        # PagerDuty is account-scoped. Its one canonical IntegrationSource is
        # repaired transactionally inside plan_sync_run and therefore cannot
        # carry this config's planner tag when the request is built. Leaving
        # source_ids unset lets the planner consume that verified account
        # source; applying repository-style tag scoping here produces a
        # permanent zero-unit run.
        and str(getattr(config, "provider", "")).lower() != "pagerduty"
    ):
        request = dataclasses.replace(
            request, source_ids=_planner_scoped_source_ids(session, config)
        )
    return request


def stamp_sync_run_canonical_config(
    session: Session,
    sync_run: Any,
    *,
    completed_at: datetime | None = None,
    success: bool,
    error: str | None,
    stats: dict[str, Any] | None = None,
) -> None:
    config = canonical_sync_config_for_sync_run(session, sync_run)
    if config is None:
        return

    config.last_sync_at = completed_at or datetime.now(timezone.utc)
    config.last_sync_success = success
    config.last_sync_error = error
    if stats is not None:
        config.last_sync_stats = stats


def canonical_sync_config_for_sync_run(session: Session, sync_run: Any) -> Any | None:
    import uuid

    from dev_health_ops.models.settings import SyncConfiguration

    integration_id = getattr(sync_run, "integration_id", None)
    org_id = getattr(sync_run, "org_id", None)
    if integration_id is None or org_id is None:
        return None

    integration_uuid = uuid.UUID(str(integration_id))
    configs = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.org_id == str(org_id),
            SyncConfiguration.integration_id == integration_uuid,
            SyncConfiguration.parent_id.is_(None),
        )
        .order_by(SyncConfiguration.created_at.asc(), SyncConfiguration.id.asc())
        .limit(2)
        .all()
    )
    if not configs:
        return None
    if len(configs) > 1:
        logger.warning(
            "Multiple parent SyncConfigurations found for integration %s in org %s; using %s as canonical",
            integration_uuid,
            org_id,
            configs[0].id,
        )
    return configs[0]


def inactive_child_configs_for_sync_run(session: Session, sync_run: Any) -> list[Any]:
    import uuid

    from dev_health_ops.models.integrations import SyncRunUnit
    from dev_health_ops.models.settings import SyncConfiguration

    org_id = getattr(sync_run, "org_id", None)
    sync_run_id = getattr(sync_run, "id", None)
    if org_id is None or sync_run_id is None:
        return []

    run_uuid = uuid.UUID(str(sync_run_id))
    source_ids = [
        source_id
        for (source_id,) in session.query(SyncRunUnit.source_id)
        .filter(SyncRunUnit.sync_run_id == run_uuid)
        .distinct()
        .all()
        if source_id is not None
    ]
    if not source_ids:
        return []

    return (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.org_id == str(org_id),
            SyncConfiguration.parent_id.is_not(None),
            SyncConfiguration.source_id.in_(source_ids),
            SyncConfiguration.is_active.is_(False),
        )
        .order_by(SyncConfiguration.created_at.asc(), SyncConfiguration.id.asc())
        .all()
    )


def mark_sync_run_failed(session: Session, sync_run_id: str, error: str) -> None:
    """Mark a committed-but-undispatched SyncRun FAILED so it is not stranded.

    Both trigger surfaces (admin "Sync Now" and the scheduled beat) commit the
    planned SyncRun + units before enqueueing ``dispatch_sync_run`` so a separate
    worker session can see them. If that enqueue then fails, the run is committed
    as PLANNED. The outbox and reconciler relay will recover and re-drive the run.
    This helper remains as a legacy, best-effort fallback for the non-outbox path
    to flip the run to FAILED rather than leave it looking perpetually in-flight.

    The update is a CONDITIONAL compare-and-set: it only fires while the run (and
    its units) are still PLANNED. Under an *ambiguous* enqueue failure (the broker
    actually accepted the message before ``apply_async`` raised) the real
    dispatcher may concurrently advance the run to DISPATCHING/SUCCESS; in that
    case this no-ops rather than overwriting a live/finished run. It also flips the
    run's still-PLANNED units to FAILED in the same transaction so a late
    dispatcher (whose ``_claim_units`` only claims PLANNED units) finds nothing to
    claim -- preventing a duplicate provider sync alongside the caller's legacy
    fallback.

    Best-effort: any error is logged and rolled back so the caller can proceed
    with its own fallback / error response.
    """
    import uuid
    from datetime import datetime, timezone

    from dev_health_ops.models.integrations import (
        SyncRun,
        SyncRunStatus,
        SyncRunUnit,
        SyncRunUnitStatus,
    )

    try:
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        # Only act on a still-PLANNED run -- never overwrite one the real
        # dispatcher already advanced past PLANNED.
        if run is None or run.status != SyncRunStatus.PLANNED.value:
            session.rollback()
            return
        now = datetime.now(timezone.utc)
        run.status = SyncRunStatus.FAILED.value
        run.error = error
        run.completed_at = now
        session.query(SyncRunUnit).filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
        ).update(
            {
                SyncRunUnit.status: SyncRunUnitStatus.FAILED.value,
                SyncRunUnit.error: error,
                SyncRunUnit.updated_at: now,
            },
            synchronize_session=False,
        )
        stamp_sync_run_canonical_config(
            session,
            run,
            completed_at=now,
            success=False,
            error=error,
            stats={"error": error, "phase": "dispatch_enqueue"},
        )
        session.commit()
    except Exception:
        logger.exception("Failed to mark stranded sync_run %s as failed", sync_run_id)
        session.rollback()


__all__ = [
    "canonical_sync_config_for_sync_run",
    "map_sync_mode",
    "mark_sync_run_failed",
    "planner_request_for_config_if_routed",
    "plan_request_for_config",
    "stamp_sync_run_canonical_config",
]
