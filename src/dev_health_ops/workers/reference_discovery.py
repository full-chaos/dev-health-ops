from __future__ import annotations

import logging
import os
import random
import threading
import time
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy import update

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models import (
    Integration,
    IntegrationCredential,
    IntegrationSource,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    upsert_outbox_wakeup,
)
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import (
    _credential_mapping,
    _get_db_url,
    _resolve_env_credentials,
)
from dev_health_ops.workers.team_autoimport import run_team_autoimport_strict

logger = logging.getLogger(__name__)

DISCOVERY_STATUS_PLANNED = "planned"
DISCOVERY_STATUS_RUNNING = "running"
DISCOVERY_STATUS_RETRYING = "retrying"
DISCOVERY_STATUS_SUCCESS = "success"
DISCOVERY_STATUS_FAILED = "failed"
REFERENCE_DISCOVERY_ERROR_CATEGORY = "reference_discovery_failed"


@celery_app.task(
    queue="sync", name="dev_health_ops.workers.tasks.run_sync_reference_discovery"
)
def run_sync_reference_discovery(sync_run_id: str) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session_sync

    run_uuid = uuid.UUID(str(sync_run_id))
    started_at = datetime.now(timezone.utc)
    lease_owner = str(uuid.uuid4())
    deadline = started_at + timedelta(seconds=_max_discovery_lifetime_seconds())
    lease_expires_at = min(
        started_at + timedelta(seconds=_discovery_lease_seconds()), deadline
    )
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    try:
        with get_postgres_session_sync() as session:
            ledger = _ensure_reference_discovery(session, run_uuid, now=started_at)
            claim_result = session.execute(
                update(SyncRunReferenceDiscovery)
                .where(
                    SyncRunReferenceDiscovery.id == ledger.id,
                    SyncRunReferenceDiscovery.status.in_(
                        [DISCOVERY_STATUS_PLANNED, DISCOVERY_STATUS_RETRYING]
                    )
                    | (
                        (SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_RUNNING)
                        & SyncRunReferenceDiscovery.lease_expires_at.is_not(None)
                        & (SyncRunReferenceDiscovery.lease_expires_at <= started_at)
                    ),
                    SyncRunReferenceDiscovery.available_at <= started_at,
                )
                .values(
                    status=DISCOVERY_STATUS_RUNNING,
                    attempts=SyncRunReferenceDiscovery.attempts + 1,
                    lease_owner=lease_owner,
                    lease_expires_at=lease_expires_at,
                    last_heartbeat_at=started_at,
                    error=None,
                    updated_at=started_at,
                )
                .execution_options(synchronize_session=False)
            )
            session.flush()
            if _rowcount(claim_result) == 0:
                return {
                    "status": "skipped",
                    "sync_run_id": sync_run_id,
                    "reason": "not_claimed",
                }

        heartbeat_stop, heartbeat_thread = _start_reference_discovery_heartbeat(
            sync_run_id, lease_owner, deadline
        )
        context = _load_discovery_context(run_uuid)
        summary = run_team_autoimport_strict(
            provider=context["provider"],
            org_id=context["org_id"],
            credentials=context["credentials"],
            scope=context["scope"],
            analytics_db_url=context["analytics_db_url"],
        )
        _verify_reference_readback(
            org_id=context["org_id"],
            provider=context["provider"],
            summary=summary,
            analytics_db_url=context["analytics_db_url"],
        )
        completed_at = datetime.now(timezone.utc)
        with get_postgres_session_sync() as session:
            stamp_result = session.execute(
                update(SyncRunReferenceDiscovery)
                .where(
                    SyncRunReferenceDiscovery.sync_run_id == run_uuid,
                    SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_RUNNING,
                    SyncRunReferenceDiscovery.lease_owner == lease_owner,
                    SyncRunReferenceDiscovery.lease_expires_at.is_not(None),
                    SyncRunReferenceDiscovery.lease_expires_at > completed_at,
                )
                .values(
                    status=DISCOVERY_STATUS_SUCCESS,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    completed_at=completed_at,
                    error=None,
                    result=summary,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if _rowcount(stamp_result) == 0:
                return {
                    "status": "skipped",
                    "sync_run_id": sync_run_id,
                    "reason": "lease_lost",
                }
            upsert_outbox_wakeup(
                session,
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=completed_at,
                now=completed_at,
            )
            session.flush()
        return {"status": "success", "sync_run_id": sync_run_id, "result": summary}
    except Exception as exc:
        if _handle_reference_discovery_failure(run_uuid, lease_owner, exc):
            return {
                "status": "retrying"
                if _is_retryable_discovery_error(exc)
                else "failed",
                "sync_run_id": sync_run_id,
                "error": str(exc),
            }
        return {
            "status": "skipped",
            "sync_run_id": sync_run_id,
            "reason": "lease_lost",
            "error": str(exc),
        }
    finally:
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=2)


def _load_discovery_context(run_uuid: uuid.UUID) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            raise ValueError(f"sync run not found: {run_uuid}")
        integration = (
            session.query(Integration)
            .filter(
                Integration.id == run.integration_id, Integration.org_id == run.org_id
            )
            .one_or_none()
        )
        if integration is None:
            raise ValueError(f"integration not found for sync run: {run_uuid}")
        if integration.credential_id is None:
            credentials: dict[str, Any] = dict(
                _resolve_env_credentials(integration.provider)
            )
        else:
            credential = (
                session.query(IntegrationCredential)
                .filter(
                    IntegrationCredential.id == integration.credential_id,
                    IntegrationCredential.org_id == run.org_id,
                )
                .one_or_none()
            )
            if credential is None:
                raise ValueError(f"credential not found for sync run: {run_uuid}")
            credentials = _credential_mapping(credential)
        units = (
            session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == run_uuid)
            .order_by(SyncRunUnit.id)
            .all()
        )
        source_ids = {unit.source_id for unit in units}
        sources = (
            session.query(IntegrationSource)
            .filter(IntegrationSource.id.in_(source_ids))
            .all()
            if source_ids
            else []
        )
        source_external_ids = {
            source.id: str(source.external_id)
            for source in sources
            if source.external_id is not None and str(source.external_id).strip()
        }
        unresolved_source_ids = sorted(
            str(source_id)
            for source_id in source_ids
            if source_id not in source_external_ids
        )
        if unresolved_source_ids:
            raise ValueError(
                "reference discovery source inventory incomplete: "
                f"unresolved_source_ids={unresolved_source_ids}"
            )
        scope = {
            "mode": "sync_reference_discovery",
            "sync_run_id": str(run_uuid),
            "triggered_by": str(run.triggered_by),
            "sync_targets": sorted({str(unit.dataset_key) for unit in units}),
            "source_external_ids": sorted(
                source_external_ids[source_id] for source_id in source_ids
            ),
            "sync_options": dict(integration.config or {}),
        }
        return {
            "provider": str(integration.provider).strip().lower(),
            "org_id": str(run.org_id),
            "credentials": credentials,
            "scope": scope,
            "analytics_db_url": _get_db_url(),
        }


def _verify_reference_readback(
    *, org_id: str, provider: str, summary: Mapping[str, Any], analytics_db_url: str
) -> None:
    expected_team_keys = _strings(summary.get("reference_team_keys"))
    expected_sprint_ids = _strings(summary.get("reference_sprint_ids"))
    deadline = time.monotonic() + _readback_timeout_seconds()
    sink = ClickHouseMetricsSink(dsn=analytics_db_url)
    try:
        while True:
            missing_teams = _missing_team_keys(
                sink, org_id=org_id, provider=provider, expected_keys=expected_team_keys
            )
            missing_sprints = _missing_sprint_ids(
                sink,
                org_id=org_id,
                provider=provider,
                expected_ids=expected_sprint_ids,
            )
            if not missing_teams and not missing_sprints:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "reference discovery readback failed: "
                    f"missing_teams={missing_teams} missing_sprints={missing_sprints}"
                )
            time.sleep(0.25)
    finally:
        sink.close()


def _missing_team_keys(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    provider: str,
    expected_keys: set[str],
) -> set[str]:
    if not expected_keys:
        return set()
    rows = sink.query_dicts(
        "SELECT native_team_key FROM ("
        "SELECT org_id, provider, native_team_key, "
        "argMax(id, updated_at) AS id "
        "FROM teams "
        "WHERE org_id = {org_id:String} AND provider = {provider:String} "
        "AND native_team_key IN {keys:Array(String)} "
        "GROUP BY org_id, provider, native_team_key"
        ")",
        {"org_id": org_id, "provider": provider, "keys": sorted(expected_keys)},
    )
    visible = {str(row.get("native_team_key")) for row in rows}
    return expected_keys - visible


def _missing_sprint_ids(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    provider: str,
    expected_ids: set[str],
) -> set[str]:
    if not expected_ids:
        return set()
    rows = sink.query_dicts(
        "SELECT sprint_id FROM ("
        "SELECT org_id, provider, sprint_id, "
        "argMax(name, last_synced) AS name "
        "FROM sprints "
        "WHERE org_id = {org_id:String} AND provider = {provider:String} "
        "AND sprint_id IN {ids:Array(String)} "
        "GROUP BY org_id, provider, sprint_id"
        ")",
        {"org_id": org_id, "provider": provider, "ids": sorted(expected_ids)},
    )
    visible = {str(row.get("sprint_id")) for row in rows}
    return expected_ids - visible


def _handle_reference_discovery_failure(
    run_uuid: uuid.UUID, lease_owner: str, exc: Exception
) -> bool:
    from dev_health_ops.db import get_postgres_session_sync

    now = datetime.now(timezone.utc)
    retryable = _is_retryable_discovery_error(exc)
    with get_postgres_session_sync() as session:
        ledger = (
            session.query(SyncRunReferenceDiscovery)
            .filter(SyncRunReferenceDiscovery.sync_run_id == run_uuid)
            .one_or_none()
        )
        if ledger is None or not _ledger_lease_is_owned_and_live(
            ledger, lease_owner, now
        ):
            return False
        if retryable and int(ledger.attempts or 0) < _max_attempts():
            available_at = now + timedelta(
                seconds=_reference_discovery_backoff_seconds(int(ledger.attempts or 1))
            )
            result = session.execute(
                update(SyncRunReferenceDiscovery)
                .where(
                    SyncRunReferenceDiscovery.id == ledger.id,
                    SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_RUNNING,
                    SyncRunReferenceDiscovery.lease_owner == lease_owner,
                    SyncRunReferenceDiscovery.lease_expires_at > now,
                )
                .values(
                    status=DISCOVERY_STATUS_RETRYING,
                    available_at=available_at,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=now,
                    error=str(exc),
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if _rowcount(result) == 0:
                return False
            upsert_outbox_wakeup(
                session,
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_DISCOVERY,
                available_at=available_at,
                now=now,
            )
            session.flush()
            return True
        result = session.execute(
            update(SyncRunReferenceDiscovery)
            .where(
                SyncRunReferenceDiscovery.id == ledger.id,
                SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_RUNNING,
                SyncRunReferenceDiscovery.lease_owner == lease_owner,
                SyncRunReferenceDiscovery.lease_expires_at > now,
            )
            .values(
                status=DISCOVERY_STATUS_FAILED,
                lease_owner=None,
                lease_expires_at=None,
                last_heartbeat_at=now,
                completed_at=now,
                error=str(exc),
                result={
                    "error_category": REFERENCE_DISCOVERY_ERROR_CATEGORY,
                    "retryable": retryable,
                    "attempts": int(ledger.attempts or 0),
                },
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        if _rowcount(result) == 0:
            return False
        _fail_nonterminal_units(session, run_uuid, now=now, error=str(exc))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is not None:
            run.error = f"Reference discovery failed: {exc}"
        upsert_outbox_wakeup(
            session,
            sync_run_id=run_uuid,
            kind=OUTBOX_KIND_FINALIZE,
            available_at=now,
            now=now,
        )
        session.flush()
        return True


def _fail_nonterminal_units(
    session: Any, run_uuid: uuid.UUID, *, now: datetime, error: str
) -> None:
    session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.not_in(
                [SyncRunUnitStatus.SUCCESS.value, SyncRunUnitStatus.FAILED.value]
            ),
        )
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            error=error,
            result={"error_category": REFERENCE_DISCOVERY_ERROR_CATEGORY},
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )


def _ensure_reference_discovery(
    session: Any, run_uuid: uuid.UUID, *, now: datetime
) -> SyncRunReferenceDiscovery:
    ledger = (
        session.query(SyncRunReferenceDiscovery)
        .filter(SyncRunReferenceDiscovery.sync_run_id == run_uuid)
        .one_or_none()
    )
    if ledger is not None:
        return ledger
    run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
    if run is None:
        raise ValueError(f"sync run not found: {run_uuid}")
    ledger = SyncRunReferenceDiscovery(
        sync_run_id=run_uuid,
        org_id=str(run.org_id),
        status=DISCOVERY_STATUS_PLANNED,
        attempts=0,
        available_at=now,
    )
    session.add(ledger)
    session.flush()
    return ledger


def reference_discovery_succeeded(session: Any, run_uuid: uuid.UUID) -> bool:
    return (
        session.query(SyncRunReferenceDiscovery.id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == run_uuid,
            SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_SUCCESS,
        )
        .one_or_none()
        is not None
    )


def ensure_reference_discovery_wakeup(
    session: Any, run_uuid: uuid.UUID, *, now: datetime
) -> None:
    ledger = _ensure_reference_discovery(session, run_uuid, now=now)
    available_at = ledger.available_at or now
    upsert_outbox_wakeup(
        session,
        sync_run_id=run_uuid,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=available_at,
        now=now,
    )


def _ledger_lease_is_owned_and_live(
    ledger: SyncRunReferenceDiscovery, lease_owner: str, now: datetime
) -> bool:
    if ledger.status != DISCOVERY_STATUS_RUNNING or ledger.lease_owner != lease_owner:
        return False
    if ledger.lease_expires_at is None:
        return False
    return _as_aware(ledger.lease_expires_at) > now


def _start_reference_discovery_heartbeat(
    sync_run_id: str, lease_owner: str, deadline: datetime
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_reference_discovery,
        args=(sync_run_id, lease_owner, stop_event, deadline),
        name=f"sync-reference-discovery-heartbeat-{sync_run_id}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _heartbeat_reference_discovery(
    sync_run_id: str, lease_owner: str, stop_event: threading.Event, deadline: datetime
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    interval = _heartbeat_interval_seconds()
    lease_seconds = _discovery_lease_seconds()
    run_uuid = uuid.UUID(str(sync_run_id))
    while not stop_event.wait(interval):
        now = datetime.now(timezone.utc)
        if now >= deadline:
            stop_event.set()
            break
        try:
            with get_postgres_session_sync() as session:
                result = session.execute(
                    update(SyncRunReferenceDiscovery)
                    .where(
                        SyncRunReferenceDiscovery.sync_run_id == run_uuid,
                        SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_RUNNING,
                        SyncRunReferenceDiscovery.lease_owner == lease_owner,
                        SyncRunReferenceDiscovery.lease_expires_at > now,
                    )
                    .values(
                        lease_expires_at=min(
                            now + timedelta(seconds=lease_seconds), deadline
                        ),
                        last_heartbeat_at=now,
                        updated_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                if _rowcount(result) == 0:
                    stop_event.set()
        except Exception:
            logger.exception(
                "run_sync_reference_discovery.heartbeat_failed",
                extra={"sync_run_id": sync_run_id},
            )


def _is_retryable_discovery_error(exc: Exception) -> bool:
    if isinstance(exc, (RateLimitException, TimeoutError, SoftTimeLimitExceeded)):
        return True
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    retry_markers = ("timeout", "rate", "429", "temporar", "transient", "too many")
    return any(marker in name or marker in message for marker in retry_markers)


def _reference_discovery_backoff_seconds(attempts: int) -> int:
    base = min(30 * 2 ** min(max(attempts, 1) - 1, 5), 900)
    return base + random.randint(0, max(1, min(base, 30)))


def _max_attempts() -> int:
    try:
        return max(1, int(os.getenv("SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS", "5")))
    except ValueError:
        return 5


def _discovery_lease_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS", "300")))
    except ValueError:
        return 300


def _max_discovery_lifetime_seconds() -> int:
    try:
        return max(
            3600,
            int(os.getenv("SYNC_REFERENCE_DISCOVERY_MAX_LIFETIME_SECONDS", "3720")),
        )
    except ValueError:
        return 3720


def _heartbeat_interval_seconds() -> int:
    return max(1, min(60, _discovery_lease_seconds() // 4))


def _readback_timeout_seconds() -> float:
    try:
        return max(
            0.1, float(os.getenv("SYNC_REFERENCE_DISCOVERY_READBACK_SECONDS", "5"))
        )
    except ValueError:
        return 5.0


def _strings(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value} if value else set()
    if not isinstance(value, Iterable):
        return set()
    return {str(item) for item in value if str(item)}


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _rowcount(result: object) -> int:
    return int(getattr(result, "rowcount", 0) or 0)
