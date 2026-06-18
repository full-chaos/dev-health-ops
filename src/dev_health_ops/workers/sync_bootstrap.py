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
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


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
        """Close the connector and store. Implemented in CHAOS-2512/2513."""

        for resource in (self.connector, self.store):
            close = getattr(resource, "close", None)
            if close is None:
                continue
            result = close()
            if inspect.iscoroutine(result):
                from dev_health_ops.workers.async_runner import run_async

                run_async(result)


class SyncTaskBootstrap:
    """Resolve a unit id into a fully-loaded :class:`SyncTaskContext`."""

    @staticmethod
    def load(session: Session, unit_id: str) -> SyncTaskContext:
        """Load + decrypt everything a unit needs. Implemented in CHAOS-2512."""

        from dev_health_ops.models import (
            Integration,
            IntegrationCredential,
            IntegrationSource,
            SyncRunUnit,
        )
        from dev_health_ops.workers.task_utils import (
            _credential_mapping,
            _get_db_url,
            _resolve_env_credentials,
        )

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

        credential_id = integration.credential_id
        if credential_id is None:
            decrypted_credentials = _resolve_env_credentials(str(unit.provider))
        else:
            credential = (
                session.query(IntegrationCredential)
                .filter(
                    IntegrationCredential.id == credential_id,
                    IntegrationCredential.org_id == unit.org_id,
                )
                .one_or_none()
            )
            if credential is None:
                raise ValueError(f"Credential not found for unit: {unit_id}")
            decrypted_credentials = _credential_mapping(credential)

        processor_flags = {
            str(key): bool(value)
            for key, value in dict(unit.processor_flags or {}).items()
        }
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
        )


class ProviderRuntimeCache:
    """Process-local, strictly-scoped runtime cache (TTL/LRU, closes on evict)."""

    def __init__(self, max_size: int = 32) -> None:
        self.max_size = max_size
        self._runtimes: OrderedDict[RuntimeCacheKey, ProviderRuntime] = OrderedDict()

    def get(self, context: SyncTaskContext) -> ProviderRuntime:
        """Return a reusable runtime for the context's scope. CHAOS-2512."""

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
        runtime = self._runtimes.get(key)
        if runtime is not None:
            self._runtimes.move_to_end(key)
            return runtime

        runtime = ProviderRuntime(store=_create_store(context))
        self._runtimes[key] = runtime
        self._runtimes.move_to_end(key)
        while len(self._runtimes) > self.max_size:
            _, evicted = self._runtimes.popitem(last=False)
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
    from dev_health_ops.storage import create_store

    store = create_store(context.db_url)
    setattr(store, "org_id", context.org_id)
    return store
