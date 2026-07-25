"""Unit-worker bootstrap + provider runtime reuse contract.

FROZEN CONTRACT (CHAOS-2286 + CHAOS-2291 folded -> CHAOS-2512).

Celery unit payloads carry IDs ONLY. The worker resolves everything from the
DB inside the task:
  * :meth:`SyncTaskBootstrap.load` loads the unit, its source/integration, and
    decrypts credentials IN-PROCESS (never in the Celery payload).
  * :class:`ProviderRuntimeCache` reuses connector/HTTP clients and the
    ClickHouse store across units of the same (org, integration, credential)
    within a worker process, so a run of many small units does not pay
    per-unit construction/decryption cost.

SECURITY: a runtime is NEVER shared across different ``org_id`` or
``credential_id``. The cache key includes a credential fingerprint so rotated
credentials evict the old runtime.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dev_health_ops.credentials.fingerprint import credential_fingerprint

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


class RunAuthFingerprintMismatchError(ValueError):
    """A run's stamped credential fingerprint no longer matches the credential
    resolved at execution time (an in-place secret edit mid-run, CHAOS-2755).

    Raised only under ``SYNC_RUN_AUTH_STRICT``; the default is warn-and-continue.
    Subclasses :class:`ValueError` so it is treated as a non-retryable failure by
    the unit/discovery error handlers (rotation is a legitimate mid-run edit, so
    retrying the old stamp would be wrong).
    """


def _sync_run_auth_strict() -> bool:
    """Hard-fail on a mid-run credential-content change when truthy.

    Default (warn-and-continue) tolerates rotation-to-fix-a-bad-token, the
    common legitimate in-place edit; operators flip this on for a strict rollout.
    """
    return os.getenv("SYNC_RUN_AUTH_STRICT", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _load_credential(session: Session, credential_id: Any, org_id: str) -> Any:
    """Load an ``IntegrationCredential`` by id within an org.

    Deliberately does NOT filter on ``is_active``: ``is_active`` is enforced only
    at plan-time stamping (``sync/planner.py``); a stamped run tolerates its
    credential being deactivated mid-run — that asymmetry is what "freezing" a
    run's auth means.
    """
    from dev_health_ops.models import IntegrationCredential

    return (
        session.query(IntegrationCredential)
        .filter(
            IntegrationCredential.id == credential_id,
            IntegrationCredential.org_id == org_id,
        )
        .one_or_none()
    )


def _require_active_pagerduty_credential(credential: Any) -> None:
    if str(getattr(credential, "provider", "")).lower() != "pagerduty" or not bool(
        getattr(credential, "is_active", False)
    ):
        raise ValueError(
            "PagerDuty sync requires an active organization-scoped credential"
        )


def _resolve_integration_auth(
    session: Session, integration: Any, provider: str, error_label: str
) -> tuple[Any, Any]:
    """Today's mutable-resolution path: read auth from ``Integration.credential_id``.

    Used for NULL-stamped runs (legacy / pre-migration / in-flight at deploy).
    """
    from dev_health_ops.workers.task_utils import (
        _credential_mapping,
        _resolve_env_credentials,
    )

    credential_id = integration.credential_id
    if credential_id is None:
        if provider == "pagerduty":
            raise ValueError(
                "PagerDuty sync requires an active organization-scoped credential"
            )
        return None, _resolve_env_credentials(provider)
    credential = _load_credential(session, credential_id, integration.org_id)
    if credential is None:
        raise ValueError(f"Credential not found for {error_label}")
    if provider == "pagerduty":
        _require_active_pagerduty_credential(credential)
    return credential_id, _credential_mapping(credential)


def _verify_stamped_fingerprint(
    run: Any,
    *,
    decrypted_credentials: Any,
    credential_id: Any,
    integration: Any,
    error_label: str,
) -> None:
    """Detect an in-place secret edit against the run's stamped fingerprint.

    No-op when the run carries no stamped fingerprint. On mismatch: hard-fail
    under ``SYNC_RUN_AUTH_STRICT``, otherwise warn and continue with the newly
    resolved secret. Logs carry no secret material.
    """
    stamped_fingerprint = getattr(run, "credential_fingerprint", None)
    if not stamped_fingerprint:
        return
    current_fingerprint = credential_fingerprint(
        decrypted_credentials,
        credential_id=str(credential_id) if credential_id is not None else None,
        integration_id=str(integration.id),
    )
    if current_fingerprint == stamped_fingerprint:
        return
    if _sync_run_auth_strict():
        raise RunAuthFingerprintMismatchError(
            f"Sync run auth fingerprint mismatch for {error_label}: "
            "stamped credential content changed mid-run"
        )
    logger.warning(
        "sync_run_auth.fingerprint_mismatch",
        extra={
            "error_label": error_label,
            "integration_id": str(integration.id),
            "auth_source": getattr(run, "auth_source", None),
            "strict": False,
        },
    )


def resolve_run_auth(
    session: Session,
    *,
    run: Any,
    integration: Any,
    provider: str,
    error_label: str,
) -> tuple[Any, Any]:
    """Resolve ``(credential_id, decrypted_credentials)`` for a unit/discovery.

    When the run was stamped at plan time (``auth_source`` non-NULL, CHAOS-2755),
    the run-frozen credential is used, so a mid-run edit to
    ``Integration.credential_id`` cannot change this run's auth. NULL-stamped
    runs fall back to the mutable ``Integration.credential_id`` path so runs that
    were already in flight when this change deployed keep working.
    """
    from dev_health_ops.workers.task_utils import (
        _credential_mapping,
        _resolve_env_credentials,
    )

    auth_source = getattr(run, "auth_source", None) if run is not None else None
    if auth_source is None:
        credential_id, decrypted_credentials = _resolve_integration_auth(
            session, integration, provider, error_label
        )
    else:
        stamped_credential_id = getattr(run, "credential_id", None)
        if stamped_credential_id is None:
            if provider == "pagerduty":
                raise ValueError(
                    "PagerDuty sync requires an active organization-scoped credential"
                )
            decrypted_credentials = dict(_resolve_env_credentials(provider))
            credential_id = None
        else:
            credential = _load_credential(
                session, stamped_credential_id, integration.org_id
            )
            if credential is None:
                # Stamped credential deleted mid-run: the intended, honest failure
                # surface (the run was frozen against a now-absent credential).
                raise ValueError(f"Credential not found for {error_label}")
            if provider == "pagerduty":
                _require_active_pagerduty_credential(credential)
            decrypted_credentials = _credential_mapping(credential)
            credential_id = stamped_credential_id
        _verify_stamped_fingerprint(
            run,
            decrypted_credentials=decrypted_credentials,
            credential_id=credential_id,
            integration=integration,
            error_label=error_label,
        )
    if provider == "pagerduty" and isinstance(decrypted_credentials, dict):
        from dev_health_ops.providers.pagerduty.sync_auth import (
            hydrate_pagerduty_credentials,
        )

        decrypted_credentials = hydrate_pagerduty_credentials(
            decrypted_credentials, org_id=integration.org_id
        )
    return credential_id, decrypted_credentials


@dataclass(frozen=True)
class SyncTaskContext:
    """Everything a unit needs to execute, resolved from ``unit_id``.

    ``decrypted_credentials`` stays in-process only and must never be logged or
    re-serialized into a Celery message.
    """

    unit_id: str
    sync_run_id: str
    org_id: str
    integration_id: str
    source_id: str
    source_external_id: str  # watermark identity (stable across migration/rediscovery)
    provider: str
    dataset_key: str
    cost_class: str
    mode: str
    window_start: datetime | None
    window_end: datetime | None
    processor_flags: dict[str, bool]
    credential_id: str | None
    decrypted_credentials: Any
    db_url: str
    source_is_org_wide_placeholder: bool = False
    resume_cursor: datetime | None = None
    dataset_options: dict[str, Any] = field(default_factory=dict)


_NON_GIT_SOURCE_SCOPE_KEYS = ("project_id", "project_key", "team_id", "repo")


def _linear_org_wide_placeholder_source(source: Any, integration: Any) -> bool:
    provider = str(getattr(source, "provider", "") or integration.provider).lower()
    if provider != "linear":
        return False

    metadata = dict(getattr(source, "metadata_", None) or {})
    if metadata.get("org_wide_placeholder") is True:
        return True

    external_id = str(getattr(source, "external_id", "") or "").strip().lower()
    if external_id != provider:
        return False

    if not metadata.get("planner_managed_sync_config_id"):
        return False

    config = dict(getattr(integration, "config", None) or {})
    return not any(
        str(config.get(key) or "").strip() for key in _NON_GIT_SOURCE_SCOPE_KEYS
    )


@dataclass(frozen=True)
class RuntimeCacheKey:
    """Strict scoping key for :class:`ProviderRuntimeCache`."""

    org_id: str
    integration_id: str
    credential_id: str | None
    credential_fingerprint: str
    provider: str
    db_url: str


@dataclass
class ProviderRuntime:
    """Holds a live provider connector/client and a store/sink for reuse."""

    connector: Any = None
    store: Any = None
    extra: dict[str, Any] = field(default_factory=dict)

    def close(self) -> None:
        """Close the connector and store (CHAOS-2592).

        The store is an async context manager entered once at creation time
        (see ``_create_store``); exit it so the underlying client (e.g. the
        ClickHouse connection) is released on cache eviction. The connector
        is closed via its own ``close`` hook.
        """

        from dev_health_ops.workers.async_runner import run_async

        store = self.store
        if store is not None:
            aexit = getattr(store, "__aexit__", None)
            if aexit is not None:
                run_async(aexit(None, None, None))

        connector = self.connector
        close = getattr(connector, "close", None)
        if close is not None:
            result = close()
            if inspect.iscoroutine(result):
                run_async(result)


class SyncTaskBootstrap:
    """Resolve a unit id into a fully-loaded :class:`SyncTaskContext`."""

    @staticmethod
    def load(session: Session, unit_id: str) -> SyncTaskContext:
        """Load + decrypt everything a unit needs. Implemented in CHAOS-2512."""

        from dev_health_ops.models import (
            Integration,
            IntegrationDataset,
            IntegrationSource,
            SyncRun,
            SyncRunUnit,
        )
        from dev_health_ops.workers.task_utils import _get_db_url

        unit_uuid = uuid.UUID(str(unit_id))
        unit = (
            session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_uuid).one_or_none()
        )
        if unit is None:
            raise ValueError(f"Sync run unit not found: {unit_id}")

        integration = (
            session.query(Integration)
            .filter(
                Integration.id == unit.integration_id,
                Integration.org_id == unit.org_id,
            )
            .one_or_none()
        )
        if integration is None:
            raise ValueError(f"Integration not found for unit: {unit_id}")

        source = (
            session.query(IntegrationSource)
            .filter(
                IntegrationSource.id == unit.source_id,
                IntegrationSource.org_id == unit.org_id,
                IntegrationSource.integration_id == integration.id,
            )
            .one_or_none()
        )
        if source is None:
            raise ValueError(f"Integration source not found for unit: {unit_id}")

        dataset = (
            session.query(IntegrationDataset)
            .filter(
                IntegrationDataset.org_id == unit.org_id,
                IntegrationDataset.integration_id == integration.id,
                IntegrationDataset.dataset_key == unit.dataset_key,
            )
            .one_or_none()
        )
        dataset_options = dict(dataset.options or {}) if dataset is not None else {}

        # Prefer the run-stamped credential frozen at plan time (CHAOS-2755). A
        # NULL-stamped (legacy/in-flight) run falls back to the mutable
        # integration.credential_id path inside resolve_run_auth.
        run = (
            session.query(SyncRun)
            .filter(SyncRun.id == unit.sync_run_id, SyncRun.org_id == unit.org_id)
            .one_or_none()
        )
        credential_id, decrypted_credentials = resolve_run_auth(
            session,
            run=run,
            integration=integration,
            provider=str(unit.provider),
            error_label=f"unit: {unit_id}",
        )

        processor_flags = {
            str(key): bool(value)
            for key, value in dict(unit.processor_flags or {}).items()
        }
        resume_cursor = None
        if unit.mode == "incremental":
            from dev_health_ops.sync.watermarks import get_watermark

            resume_cursor = get_watermark(
                session,
                str(unit.org_id),
                str(source.external_id),
                str(unit.dataset_key),
            )
        return SyncTaskContext(
            unit_id=str(unit.id),
            sync_run_id=str(unit.sync_run_id),
            org_id=str(unit.org_id),
            integration_id=str(integration.id),
            source_id=str(source.id),
            source_external_id=str(source.external_id),
            provider=str(unit.provider),
            dataset_key=str(unit.dataset_key),
            cost_class=str(unit.cost_class),
            mode=str(unit.mode),
            window_start=unit.since_at,
            window_end=unit.before_at,
            processor_flags=processor_flags,
            credential_id=str(credential_id) if credential_id is not None else None,
            decrypted_credentials=decrypted_credentials,
            db_url=_get_db_url(),
            source_is_org_wide_placeholder=_linear_org_wide_placeholder_source(
                source, integration
            ),
            resume_cursor=resume_cursor,
            dataset_options=dataset_options,
        )


class ProviderRuntimeCache:
    """Process-local, strictly-scoped runtime cache (TTL/LRU, closes on evict)."""

    def __init__(self, max_size: int = 32) -> None:
        self.max_size = max_size
        self._runtimes: OrderedDict[RuntimeCacheKey, ProviderRuntime] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, context: SyncTaskContext) -> ProviderRuntime:
        """Return a reusable runtime for the context's scope. CHAOS-2512.

        Creation/eviction is serialized (CHAOS-2592): without the lock two
        worker threads missing the same key could both build and enter a
        store, leaking the loser's live client. The lock + re-check guarantees
        exactly one store is entered per key; evicted runtimes are closed
        outside the lock so their async ``__aexit__`` does not run under it.
        """

        key = RuntimeCacheKey(
            org_id=context.org_id,
            integration_id=context.integration_id,
            credential_id=context.credential_id,
            credential_fingerprint=_credential_fingerprint(
                context.decrypted_credentials
            ),
            provider=context.provider,
            db_url=context.db_url,
        )
        evicted_runtimes: list[ProviderRuntime] = []
        with self._lock:
            runtime = self._runtimes.get(key)
            if runtime is not None:
                self._runtimes.move_to_end(key)
                return runtime

            runtime = ProviderRuntime(store=_create_store(context))
            self._runtimes[key] = runtime
            self._runtimes.move_to_end(key)
            while len(self._runtimes) > self.max_size:
                _, evicted = self._runtimes.popitem(last=False)
                evicted_runtimes.append(evicted)
        for evicted in evicted_runtimes:
            evicted.close()
        return runtime


def _credential_fingerprint(credentials: Any) -> str:
    payload = json.dumps(
        credentials or {}, sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _create_store(context: SyncTaskContext) -> Any:
    if not context.db_url:
        return None
    from dev_health_ops.storage import create_store, detect_db_type

    # The runtime cache reuses one store across many units, each run in its own
    # transient run_async() event loop. Only the ClickHouse client is
    # loop-agnostic (a sync client wrapped in asyncio.to_thread); SQLAlchemy
    # stores hold async sessions/connections bound to the loop that opened
    # them, so entering one here and reusing it from later per-unit loops would
    # break (CHAOS-2592). Leave non-ClickHouse stores unset so
    # _run_with_reused_or_new_store falls back to the per-unit run_with_store()
    # lifecycle (enter + exit within the handler's own loop).
    if detect_db_type(context.db_url) != "clickhouse":
        return None

    store = create_store(context.db_url)
    setattr(store, "org_id", context.org_id)
    # Enter the store's async context once so the client is connected before
    # the cached runtime reuses it across units. If __aenter__ opens the client
    # and then fails (e.g. table-ensure raises), best-effort exit so this
    # uncached, failed runtime does not leak a live connection.
    from dev_health_ops.workers.async_runner import run_async

    run_async(_enter_store(store))
    return store


async def _enter_store(store: Any) -> None:
    try:
        await store.__aenter__()
    except BaseException:
        aexit = getattr(store, "__aexit__", None)
        if aexit is not None:
            try:
                await aexit(None, None, None)
            except Exception as exc:
                logger.debug(
                    "Best-effort store __aexit__ failed after __aenter__ error",
                    exc_info=exc,
                )
        raise
