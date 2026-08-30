from __future__ import annotations

import asyncio
import hashlib
import math
import os
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.models.settings import (
    SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED,
    SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    ScheduledSyncOccurrence,
    SyncConfiguration,
    SyncManualTrigger,
)
from dev_health_ops.sync.canonical_incident_gate import (
    require_canonical_incident_feature_for_update_sync,
    require_canonical_incident_feature_sync,
    sync_targets_require_canonical_incident_feature,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.planner import BackfillSelector, SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION = "sync_scheduler_occurrence_v1"

# Default bound for await_sync_execution_trigger_materialized (CHAOS-4602
# fork 2): generous relative to the Go scheduler's default ~1s reconciler
# tick (internal/scheduler/sync/loop.go's PollInterval), short enough that a
# Sync Now / Backfill click cannot hang the request indefinitely. Env
# override lets ops widen it if the coordinator is ever meaningfully
# backlogged without a code change.
_DEFAULT_MANUAL_TRIGGER_AWAIT_SECONDS = 10.0


def _go_manual_backfill_planner_enabled() -> bool:
    """CHAOS-4602 rollout flag, read at call time (ops/tests can flip it
    live) -- matches the established PROVIDER_SYNC_QUEUES_ENABLED shape
    (workers/queues.py). Default OFF: every planner-managed config keeps
    calling plan_sync_run in-process, byte-for-byte unchanged, until this
    is explicitly turned on. Non-planner-managed and child configs are
    UNCHANGED regardless of this flag (fork 1) -- the gate below also
    checks config.planner_managed.
    """
    return os.getenv(
        "SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "false"
    ).strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _manual_trigger_await_seconds() -> float:
    raw = os.getenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS")
    if raw is not None:
        try:
            value = float(raw)
            # Codex review (gate round 9, P2): float("inf")/"nan" both parse
            # successfully and "inf" > 0 is True, so the old check alone
            # accepted them -- an infinite deadline breaks the "bounded
            # await" contract fork 2 requires (a deadline that never
            # elapses means await_sync_execution_trigger_materialized's
            # poll loop never returns "pending", hanging the admin
            # request's coroutine/connection indefinitely on an occurrence
            # that never completes).
            if value > 0 and math.isfinite(value):
                return value
        except ValueError:
            pass
    return _DEFAULT_MANUAL_TRIGGER_AWAIT_SECONDS


@dataclass(frozen=True)
class SyncExecutionTriggerResult:
    sync_run_id: str
    job_run_id: str
    total_units: int
    dispatch_required: bool = True
    terminal_reason: str = ""
    # CHAOS-4602: set (non-None) only on the Go hand-off path. occurrence_id
    # is the scheduled_sync_occurrences row a caller can poll directly.
    # awaiting_materialization=True means Go has not yet reached a terminal
    # reconcile_status (a caller MUST present this as "pending", never as an
    # error). quarantined=True means Go rejected or exhausted the occurrence
    # (client-visible failure, never silently reported as pending).
    occurrence_id: str | None = None
    awaiting_materialization: bool = False
    quarantined: bool = False


class ScheduledSyncOccurrenceConflictError(RuntimeError):
    """A stable occurrence identity resolved to different scheduling inputs."""


class ScheduledSyncOccurrenceIneligibleError(RuntimeError):
    """The locked scheduled configuration cannot produce an authorized plan."""


def scheduled_sync_occurrence_identity(
    config_id: str | uuid.UUID,
    scheduled_for: datetime,
) -> str:
    """Return the byte-identical occurrence identity used by the Go scheduler."""
    scheduled_for = _as_aware_utc(scheduled_for)
    fields = (
        ("identity_version", SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION),
        ("config_id", str(config_id)),
        ("scheduled_for", scheduled_for.strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"),
    )
    digest = hashlib.sha256()
    for name, value in fields:
        name_bytes = name.encode()
        value_bytes = value.encode()
        digest.update(str(len(name_bytes)).encode())
        digest.update(b":")
        digest.update(name_bytes)
        digest.update(str(len(value_bytes)).encode())
        digest.update(b":")
        digest.update(value_bytes)
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def create_scheduled_sync_execution_trigger(
    session: Session,
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
    *,
    scheduled_for: datetime,
    triggered_by: str = "schedule",
    mode: str = "incremental",
) -> SyncExecutionTriggerResult:
    """Idempotently materialize one scheduled occurrence in the caller transaction."""
    scheduled_for = _as_aware_utc(scheduled_for)
    locked_config = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.id == uuid.UUID(str(config.id)),
            SyncConfiguration.org_id == org_id,
        )
        .populate_existing()
        .with_for_update()
        .one_or_none()
    )
    if locked_config is None:
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration does not exist for organization"
        )
    _require_locked_schedule_contract(locked_config, job, org_id)
    occurrence_id = scheduled_sync_occurrence_identity(locked_config.id, scheduled_for)
    _require_locked_scheduled_eligibility(session, locked_config, org_id)

    occurrence = (
        session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == occurrence_id)
        .with_for_update()
        .one_or_none()
    )
    if occurrence is None:
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=occurrence_id,
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=org_id,
            sync_config_id=uuid.UUID(str(locked_config.id)),
            scheduled_job_id=uuid.UUID(str(job.id)),
            scheduled_for=scheduled_for,
        )
        session.add(occurrence)
        session.flush()
    else:
        _verify_scheduled_occurrence(
            occurrence, locked_config, job, org_id, scheduled_for
        )
        if occurrence.job_run_id is not None and occurrence.sync_run_id is not None:
            occurrence.reconcile_attempt_count = 0
            occurrence.reconcile_next_attempt_at = None
            occurrence.reconcile_error_code = None
            occurrence.reconcile_error_at = None
            occurrence.reconcile_status = SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED
            session.flush()
            return _existing_scheduled_trigger_result(session, occurrence)

    trigger = create_sync_execution_trigger(
        session,
        locked_config,
        org_id,
        triggered_by=triggered_by,
        mode=mode,
    )
    if trigger is None:
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration has no planner route"
        )
    occurrence.job_run_id = uuid.UUID(trigger.job_run_id)
    occurrence.sync_run_id = uuid.UUID(trigger.sync_run_id)
    occurrence.reconcile_attempt_count = 0
    occurrence.reconcile_next_attempt_at = None
    occurrence.reconcile_error_code = None
    occurrence.reconcile_error_at = None
    occurrence.reconcile_status = SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED
    session.flush()
    return trigger


def _ensure_scheduled_job_for_config(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
) -> ScheduledJob:
    """Find or create the ``ScheduledJob`` marker row for ``config``.

    Extracted from ``ensure_pending_sync_job_run`` (CHAOS-4602) so the new
    Go hand-off path -- which needs a ``scheduled_job_id`` for
    ``ScheduledSyncOccurrence``'s FK but must NOT create a ``JobRun`` row
    itself (Go derives job_run_id/sync_run_id deterministically from the
    occurrence_id; see ``scheduled_sync_occurrence_identity`` and Go's own
    ``deterministicMaterializationIDs``) -- can reuse this lookup-or-create
    logic verbatim instead of duplicating it.
    """
    config_uuid = uuid.UUID(str(config.id))
    job = (
        session.query(ScheduledJob)
        .filter(
            ScheduledJob.org_id == org_id,
            ScheduledJob.sync_config_id == config_uuid,
            ScheduledJob.job_type == "sync",
        )
        .one_or_none()
    )
    if job is None:
        sync_options = dict(config.sync_options or {})
        provider = str(config.provider or "")
        explicit_cron = sync_options.get("schedule_cron")
        job = ScheduledJob(
            name=f"sync-config-{config_uuid}",
            job_type="sync",
            schedule_cron=str(explicit_cron or "0 * * * *"),
            org_id=org_id,
            provider=provider,
            job_config={
                "provider": provider,
                "sync_config_id": str(config_uuid),
            },
            sync_config_id=config_uuid,
            tz=str(sync_options.get("timezone") or "UTC"),
            status=(
                JobStatus.ACTIVE.value
                if bool(config.is_active) and explicit_cron
                else JobStatus.PAUSED.value
            ),
        )
        session.add(job)
        session.flush()
    return job


def ensure_pending_sync_job_run(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
    triggered_by: str,
    result: dict[str, Any] | None = None,
) -> str:
    job = _ensure_scheduled_job_for_config(session, config, org_id)
    run = JobRun(
        job_id=uuid.UUID(str(job.id)),
        triggered_by=triggered_by,
        status=JobRunStatus.PENDING.value,
    )
    run.result = result
    session.add(run)
    session.flush()
    return str(run.id)


def merge_job_run_result(
    session: Session, run_id: str, result: dict[str, Any] | None = None
) -> None:
    if result is None:
        return
    run = (
        session.query(JobRun).filter(JobRun.id == uuid.UUID(str(run_id))).one_or_none()
    )
    if run is None:
        return
    current = run.result if isinstance(run.result, dict) else {}
    run.result = {**current, **result}
    session.flush()


def mark_job_run_failed(
    session: Session, run_id: str, error: BaseException | str
) -> None:
    """Terminalize a ``JobRun`` as failed.

    ``error`` is sanitized here, at the sink, rather than trusting every
    caller to have already redacted it (CHAOS-2766 codex review finding):
    a Celery/broker enqueue-failure exception can embed the configured
    broker/result-backend URL, including its credentials, and this column
    surfaces verbatim through admin job-history responses. Accepting
    ``BaseException | str`` (not just ``str``) means a caller that still
    pre-formats a message (e.g. ``f"dispatch enqueue failed: {exc}"``) stays
    covered too -- ``sanitize_error_text`` redacts credential-shaped
    substrings in plain text the same way it does in an exception's message.
    """
    completed_at = datetime.now(timezone.utc)
    run = (
        session.query(JobRun).filter(JobRun.id == uuid.UUID(str(run_id))).one_or_none()
    )
    if run is None:
        return
    run.status = JobRunStatus.FAILED.value
    run.completed_at = completed_at
    run.error = sanitize_error_text(error)
    started_at = getattr(run, "started_at", None)
    if started_at is not None:
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=completed_at.tzinfo)
        run.duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    session.flush()


def create_sync_execution_trigger(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
    *,
    triggered_by: str,
    mode: str,
    since: datetime | None = None,
    before: datetime | None = None,
    backfill_selector: BackfillSelector | None = None,
    initial_job_result: dict[str, Any] | None = None,
) -> SyncExecutionTriggerResult | None:
    sync_targets = [str(target) for target in (config.sync_targets or [])]
    if sync_targets_require_canonical_incident_feature(sync_targets):
        require_canonical_incident_feature_sync(session, org_id)
    request = planner_request_for_config_if_routed(
        session, config, triggered_by=triggered_by, mode=mode
    )
    if request is None:
        return None
    if backfill_selector is not None:
        request = replace(
            request,
            backfill_selector=backfill_selector,
            source_ids=None,
            dataset_keys=None,
            since=None,
            before=None,
        )
    elif since is not None or before is not None:
        request = replace(request, since=since, before=before)

    # CHAOS-4602 fork 1: Go pickup only for planner-managed configs, only
    # when the rollout flag is on, AND only for a genuine manual/backfill
    # trigger (codex review, gate round 9, P1): this function is also the
    # ordinary scheduled-cron path's call target
    # (create_scheduled_sync_execution_trigger passes triggered_by="schedule"
    # straight through from sync_scheduler.py). Without this check, an
    # eligible cron occurrence on a flag-enabled planner-managed config
    # would ALSO route into _create_go_manual_sync_execution_trigger, which
    # writes triggered_by verbatim into SyncManualTrigger -- a value the
    # ck_sync_manual_triggers_triggered_by CHECK constraint (settings.py,
    # 'manual'/'backfill' only) rejects outright, failing every regular
    # scheduled sync for that config the moment the flag is turned on.
    # Every non-planner-managed/child config, every planner-managed config
    # while the flag is off, and every ordinary scheduled tick (regardless
    # of the flag) falls through UNCHANGED to the pre-existing in-process
    # plan_sync_run call below.
    if (
        bool(getattr(config, "planner_managed", False))
        and _go_manual_backfill_planner_enabled()
        and triggered_by in ("manual", "backfill")
    ):
        return _create_go_manual_sync_execution_trigger(
            session, config, org_id, request
        )

    job_run_id = ensure_pending_sync_job_run(
        session,
        config,
        org_id,
        triggered_by,
        initial_job_result,
    )
    plan = plan_sync_run(session, request)
    if not plan.dispatch_required:
        merge_job_run_result(
            session,
            job_run_id,
            {
                "sync_run_id": plan.sync_run_id,
                "terminal_status": "pagerduty_sync_disabled",
                "reason": plan.terminal_reason,
                "total_units": plan.total_units,
            },
        )
        mark_job_run_failed(session, job_run_id, plan.terminal_reason)
    else:
        merge_job_run_result(session, job_run_id, {"sync_run_id": plan.sync_run_id})
    return SyncExecutionTriggerResult(
        sync_run_id=plan.sync_run_id,
        job_run_id=job_run_id,
        total_units=plan.total_units,
        dispatch_required=plan.dispatch_required,
        terminal_reason=plan.terminal_reason,
    )


def _resolve_manual_trigger_selector(
    request: SyncPlanRequest,
) -> tuple[
    datetime | None, datetime | None, tuple[str, ...] | None, tuple[str, ...] | None
]:
    """Return the FINAL (since, before, source_ids, dataset_keys) this
    request resolved to -- the structured backfill_selector when present
    (the only source of truth _validate_backfill_selector_compatibility
    allows to coexist with it is nothing at all), otherwise the flat
    fields plan_sync_run itself would read.
    """
    selector = request.backfill_selector
    if selector is not None:
        return (
            selector.since,
            selector.before,
            selector.source_ids,
            selector.dataset_keys,
        )
    return request.since, request.before, request.source_ids, request.dataset_keys


def _create_go_manual_sync_execution_trigger(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
    request: SyncPlanRequest,
) -> SyncExecutionTriggerResult:
    """CHAOS-4602 Go hand-off: mint a scheduled_sync_occurrences row (the
    SAME identity space and pickup path a cron tick uses -- Go's
    dueOccurrenceKeysSQL/lockPendingOccurrenceSQL is producer-agnostic) plus
    its sync_manual_triggers payload, instead of calling plan_sync_run
    in-process. Deliberately does NOT create a JobRun row: Go derives
    job_run_id/sync_run_id deterministically from occurrence_id (see this
    module's scheduled_sync_occurrence_identity and Go's own
    deterministicMaterializationIDs, the SAME namespace UUID on both sides),
    so there is nothing here for a pre-created JobRun id to match.

    Returns immediately with awaiting_materialization=True; the caller (the
    admin router, after committing this insert) is responsible for the
    bounded await via await_sync_execution_trigger_materialized.
    """
    since, before, source_ids, dataset_keys = _resolve_manual_trigger_selector(request)
    scheduled_for = datetime.now(timezone.utc)
    occurrence_id = scheduled_sync_occurrence_identity(config.id, scheduled_for)
    job = _ensure_scheduled_job_for_config(session, config, org_id)
    occurrence = ScheduledSyncOccurrence(
        occurrence_id=occurrence_id,
        identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
        org_id=org_id,
        sync_config_id=uuid.UUID(str(config.id)),
        scheduled_job_id=uuid.UUID(str(job.id)),
        scheduled_for=scheduled_for,
    )
    session.add(occurrence)
    session.add(
        SyncManualTrigger(
            occurrence_id=occurrence_id,
            mode=request.mode,
            since=since,
            before=before,
            source_ids=list(source_ids) if source_ids is not None else None,
            dataset_keys=list(dataset_keys) if dataset_keys is not None else None,
            triggered_by=request.triggered_by,
        )
    )
    session.flush()
    return SyncExecutionTriggerResult(
        sync_run_id="",
        job_run_id="",
        total_units=0,
        occurrence_id=occurrence_id,
        awaiting_materialization=True,
    )


def _read_occurrence_reconcile_state(
    session: Session, occurrence_id: str
) -> tuple[str, str | None, str | None, str | None] | None:
    occurrence = (
        session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == occurrence_id)
        .one_or_none()
    )
    if occurrence is None:
        return None
    return (
        str(occurrence.reconcile_status),
        str(occurrence.job_run_id) if occurrence.job_run_id is not None else None,
        str(occurrence.sync_run_id) if occurrence.sync_run_id is not None else None,
        occurrence.reconcile_error_code,
    )


def _materialized_trigger_result(
    session: Session,
    occurrence_id: str,
    job_run_id: str | None,
    sync_run_id: str | None,
) -> SyncExecutionTriggerResult:
    from dev_health_ops.models import SyncRun, SyncRunStatus

    if sync_run_id is None:
        # Shouldn't happen: Go's persistCoordinatorGraph links job_run_id/
        # sync_run_id and flips reconcile_status='completed' in the SAME
        # transaction it commits (occurrence_reconciler.go's reconcileOne).
        # Treat as still-pending rather than raising into the request.
        return SyncExecutionTriggerResult(
            sync_run_id="",
            job_run_id="",
            total_units=0,
            occurrence_id=occurrence_id,
            awaiting_materialization=True,
        )
    sync_run = (
        session.query(SyncRun)
        .filter(SyncRun.id == uuid.UUID(sync_run_id))
        .one_or_none()
    )
    if sync_run is None:
        return SyncExecutionTriggerResult(
            sync_run_id="",
            job_run_id="",
            total_units=0,
            occurrence_id=occurrence_id,
            awaiting_materialization=True,
        )
    result = sync_run.result if isinstance(sync_run.result, dict) else {}
    terminal = (
        sync_run.status == SyncRunStatus.FAILED.value
        and result.get("error_category") == "pagerduty_sync_disabled"
    )
    return SyncExecutionTriggerResult(
        sync_run_id=str(sync_run.id),
        job_run_id=job_run_id or "",
        total_units=int(sync_run.total_units or 0),
        dispatch_required=not terminal,
        terminal_reason=str(sync_run.error or "") if terminal else "",
        occurrence_id=occurrence_id,
    )


async def await_sync_execution_trigger_materialized(
    session: AsyncSession,
    occurrence_id: str,
    *,
    poll_interval: float = 0.25,
) -> SyncExecutionTriggerResult:
    """Bounded, async-native poll for a Go-owned scheduled_sync_occurrences
    row to reach a terminal reconcile_status -- the same typed-outcome shape
    ``await_reference_discovery_terminal`` (CHAOS-4498) established, adapted
    for this call site's constraint: THIS function is called from inside
    the admin router's async request handler, where a blocking
    ``time.sleep`` (that function's own mechanism) would stall the whole
    event loop for every concurrent request, not just this one --
    SQLAlchemy's async/greenlet bridge only yields to the loop on real I/O,
    never on a plain sleep. ``asyncio.sleep`` here actually yields.

    Each poll runs its read in its own committed transaction (the ``commit``
    after every ``run_sync`` below), both to release the held connection
    between sleeps and so the NEXT read starts a fresh READ COMMITTED
    transaction that can see Go's own, separately-connected commit.

    Outcomes:
      * materialized -- reconcile_status='completed'; returns the exact
        SyncExecutionTriggerResult shape the in-process path already
        returns (occurrence_id set, awaiting_materialization=False).
      * quarantined=True -- reconcile_status='quarantined': Go rejected the
        occurrence's identity or exhausted its retry budget. Client-visible
        failure, per CHAOS-4602 fork 2 -- the caller must never fold this
        into a silent "pending".
      * awaiting_materialization=True (unchanged) -- the deadline
        (``SYNC_MANUAL_TRIGGER_AWAIT_SECONDS``, default 10s) elapsed with
        neither of the above. Never an error: the caller presents this as
        "pending" and the occurrence keeps reconciling in the background.
    """
    started = time.monotonic()

    def _record(outcome: str) -> None:
        # CHAOS-4602 fork 2: "telemetry: counter + histogram on await
        # outcome/latency" -- both labeled by the SAME outcome so they can
        # be joined (e.g. p99 latency for quarantined vs materialized).
        from dev_health_ops.metrics.prometheus import (
            SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS,
            SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL,
        )

        SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL.labels(outcome=outcome).inc()
        SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS.labels(outcome=outcome).observe(
            time.monotonic() - started
        )

    deadline = started + _manual_trigger_await_seconds()
    while True:
        state = await session.run_sync(
            lambda sync_session: _read_occurrence_reconcile_state(
                sync_session, occurrence_id
            )
        )
        if state is not None:
            status, job_run_id, sync_run_id, error_code = state
            if status == SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED:
                result = await session.run_sync(
                    lambda sync_session: _materialized_trigger_result(
                        sync_session, occurrence_id, job_run_id, sync_run_id
                    )
                )
                await session.commit()
                _record("materialized")
                return result
            if status == SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED:
                await session.commit()
                _record("quarantined")
                return SyncExecutionTriggerResult(
                    sync_run_id="",
                    job_run_id="",
                    total_units=0,
                    dispatch_required=False,
                    terminal_reason=(
                        f"scheduled sync occurrence quarantined: {error_code or 'unknown'}"
                    ),
                    occurrence_id=occurrence_id,
                    quarantined=True,
                )
        await session.commit()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _record("pending")
            return SyncExecutionTriggerResult(
                sync_run_id="",
                job_run_id="",
                total_units=0,
                occurrence_id=occurrence_id,
                awaiting_materialization=True,
            )
        # Codex review (gate round 9, P2): sleeping the full poll_interval
        # unconditionally overshoots a configured deadline shorter than it
        # (e.g. SYNC_MANUAL_TRIGGER_AWAIT_SECONDS=0.05 with the default
        # 0.25s poll_interval measured returning ~5x late) -- cap the sleep
        # at whatever time actually remains, so the next deadline check
        # fires close to the promised bound instead of after one more full
        # poll cycle.
        await asyncio.sleep(min(poll_interval, remaining))


def _require_locked_scheduled_eligibility(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
) -> None:
    from dev_health_ops.models.users import Organization
    from dev_health_ops.workers.org_guard import organization_exists_sync

    if not organization_exists_sync(session, org_id):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync organization does not exist"
        )
    try:
        org_uuid = uuid.UUID(str(org_id))
    except ValueError:
        org_uuid = None
    if org_uuid is not None and org_id != "default":
        organization = (
            session.query(Organization.id)
            .filter(Organization.id == org_uuid)
            .with_for_update(key_share=True)
            .one_or_none()
        )
        if organization is None:
            raise ScheduledSyncOccurrenceIneligibleError(
                "scheduled sync organization does not exist"
            )

    sync_targets = [str(target) for target in (config.sync_targets or [])]
    if sync_targets_require_canonical_incident_feature(sync_targets):
        if org_uuid is None:
            # Preserve the legacy non-UUID/default compatibility path. The
            # unlocked gate remains authoritative there because no UUID-scoped
            # feature row exists to lock.
            require_canonical_incident_feature_sync(session, org_id)
        else:
            require_canonical_incident_feature_for_update_sync(session, org_id)


def _require_locked_schedule_contract(
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
) -> None:
    if not bool(config.is_active):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration is inactive"
        )
    if not str((config.sync_options or {}).get("schedule_cron") or ""):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration is manual-only"
        )
    if (
        str(config.org_id) != org_id
        or str(job.org_id) != org_id
        or job.sync_config_id != uuid.UUID(str(config.id))
        or str(job.job_type) != "sync"
        or int(job.status) != JobStatus.ACTIVE.value
    ):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync marker does not match the locked configuration"
        )


def _verify_scheduled_occurrence(
    occurrence: ScheduledSyncOccurrence,
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
    scheduled_for: datetime,
) -> None:
    persisted_for = _as_aware_utc(occurrence.scheduled_for)
    if (
        occurrence.identity_version != SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION
        or occurrence.org_id != org_id
        or occurrence.sync_config_id != uuid.UUID(str(config.id))
        or occurrence.scheduled_job_id != uuid.UUID(str(job.id))
        or persisted_for != scheduled_for
    ):
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence identity conflicts with persisted inputs"
        )
    if (occurrence.job_run_id is None) != (occurrence.sync_run_id is None):
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence has incomplete plan links"
        )


def _existing_scheduled_trigger_result(
    session: Session,
    occurrence: ScheduledSyncOccurrence,
) -> SyncExecutionTriggerResult:
    from dev_health_ops.models import SyncRun, SyncRunStatus

    job_run = (
        session.query(JobRun).filter(JobRun.id == occurrence.job_run_id).one_or_none()
    )
    sync_run = (
        session.query(SyncRun)
        .filter(SyncRun.id == occurrence.sync_run_id)
        .one_or_none()
    )
    if job_run is None or sync_run is None:
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence plan links do not resolve"
        )
    result = sync_run.result if isinstance(sync_run.result, dict) else {}
    terminal = (
        sync_run.status == SyncRunStatus.FAILED.value
        and result.get("error_category") == "pagerduty_sync_disabled"
    )
    return SyncExecutionTriggerResult(
        sync_run_id=str(sync_run.id),
        job_run_id=str(job_run.id),
        total_units=int(sync_run.total_units or 0),
        dispatch_required=not terminal,
        terminal_reason=str(sync_run.error or "") if terminal else "",
    )


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
