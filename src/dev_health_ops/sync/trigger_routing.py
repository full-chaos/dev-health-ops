"""Migrated-config trigger routing (CHAOS-2516).

The config migration (``config_migration.py``) links each legacy
``SyncConfiguration`` to integration-era records:

* a *parent* config gets ``migrated_integration_id`` set to its Integration;
* a *child* config gets both ``migrated_integration_id`` (the parent's
  Integration) and ``migrated_source_id`` (its own IntegrationSource).

The migration also writes a per-org feature flag Setting
(``sync.migrated_trigger_routing_enabled``). When that flag is enabled, the
legacy trigger surfaces — the admin "Sync Now" endpoint and the scheduled-sync
beat — must route a *migrated* config through the fan-out planner
(``plan_sync_run`` + ``dispatch_sync_run``) instead of the old per-config path.

This module is the single source of truth for that routing decision so the API
(async session, via ``run_sync``) and the beat (sync session) share identical
semantics. Rollback is purely the flag: when it is off, callers fall back to the
legacy path and this module's :func:`plan_request_for_config` is never reached.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.sync.planner import SyncPlanRequest

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dev_health_ops.models.settings import SyncConfiguration

MIGRATED_TRIGGER_ROUTING_SETTING_KEY = "sync.migrated_trigger_routing_enabled"

_TRUTHY = {"1", "true", "yes", "on"}

logger = logging.getLogger(__name__)


def is_migrated_trigger_routing_enabled(session: Session, org_id: str) -> bool:
    """Return True when the integration planner is active for ``org_id``.

    Synchronous read of the ``sync.migrated_trigger_routing_enabled`` Setting
    row written by the CHAOS-2516 migration helper. Missing row => disabled,
    which keeps un-migrated orgs on the legacy path.

    A read failure (``OperationalError``, e.g. the Setting table is absent or a
    transient DB error) also returns ``False``: the flag is opt-in, so "cannot
    determine" maps to the legacy path -- the rollback-safe default. This is
    strictly safer than raising, which would make the beat skip the config
    entirely (no sync at all) instead of falling back to the legacy per-config
    path. The session is rolled back before returning so the caller can keep
    using it: on PostgreSQL a failed statement aborts the transaction until
    rollback, which would otherwise break the very legacy path we fall back to.
    """
    from sqlalchemy.exc import OperationalError

    from dev_health_ops.models.settings import Setting, SettingCategory

    try:
        row = (
            session.query(Setting)
            .filter(
                Setting.org_id == org_id,
                Setting.category == SettingCategory.SYNC.value,
                Setting.key == MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
            )
            .one_or_none()
        )
    except OperationalError:
        # Clear the aborted transaction so the caller's legacy-path DB work
        # (PENDING JobRun create, dispatch bookkeeping) runs on a clean session.
        try:
            session.rollback()
        except Exception as rollback_err:
            logger.debug(
                "Rollback failed after OperationalError while reading migrated trigger routing flag; proceeding with legacy-path fallback.",
                exc_info=rollback_err,
            )
        return False
    if row is None:
        return False
    return str(getattr(row, "value", "") or "").strip().lower() in _TRUTHY


def _dataset_keys_for_config(config: SyncConfiguration) -> tuple[str, ...]:
    """Map a config's legacy ``sync_targets`` to integration dataset keys.

    Mirrors ``config_migration._dataset_keys_for_config`` so a migrated child's
    trigger replays exactly the datasets the legacy child used to cover.
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
    """Build a :class:`SyncPlanRequest` for a migrated config, else ``None``.

    Returns ``None`` when the config was never migrated (no
    ``migrated_integration_id``), signalling the caller to use the legacy path.

    Routing semantics:

    * **Parent config** (no ``migrated_source_id``): plan the *whole*
      integration — all enabled sources and datasets — matching the admin
      integration ``/sync`` endpoint's default fan-out.
    * **Child config** (``migrated_source_id`` set): scope the run to that one
      source, and to the dataset keys derived from the child's legacy targets so
      the migrated trigger covers exactly what the child used to.
    """
    integration_id = getattr(config, "migrated_integration_id", None)
    if integration_id is None:
        return None

    source_id = getattr(config, "migrated_source_id", None)
    source_ids: tuple[str, ...] | None = None
    dataset_keys: tuple[str, ...] | None = None
    if source_id is not None:
        source_ids = (str(source_id),)
        child_dataset_keys = _dataset_keys_for_config(config)
        # Empty => fall back to all enabled datasets rather than an empty run.
        dataset_keys = child_dataset_keys or None

    return SyncPlanRequest(
        integration_id=str(integration_id),
        org_id=str(config.org_id),
        mode=mode,
        triggered_by=triggered_by,
        source_ids=source_ids,
        dataset_keys=dataset_keys,
    )


def should_route_config_to_planner(session: Session, config: SyncConfiguration) -> bool:
    integration_id = getattr(config, "migrated_integration_id", None)
    if integration_id is None:
        return False

    if bool(getattr(config, "planner_managed", False)):
        return True

    return is_migrated_trigger_routing_enabled(session, str(config.org_id))


def planner_request_for_config_if_routed(
    session: Session,
    config: SyncConfiguration,
    *,
    triggered_by: str,
    mode: str = "incremental",
) -> SyncPlanRequest | None:
    if not should_route_config_to_planner(session, config):
        return None
    return plan_request_for_config(config, triggered_by=triggered_by, mode=mode)


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
            SyncConfiguration.migrated_integration_id == integration_uuid,
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
            SyncConfiguration.migrated_source_id.in_(source_ids),
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
    as PLANNED with no queued dispatcher and there is no periodic reconciler for
    such runs -- so flip it to FAILED rather than leave it looking perpetually
    in-flight.

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
    "MIGRATED_TRIGGER_ROUTING_SETTING_KEY",
    "canonical_sync_config_for_sync_run",
    "is_migrated_trigger_routing_enabled",
    "mark_sync_run_failed",
    "planner_request_for_config_if_routed",
    "plan_request_for_config",
    "should_route_config_to_planner",
    "stamp_sync_run_canonical_config",
]
