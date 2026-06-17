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

        raise NotImplementedError("CHAOS-2512: implement ProviderRuntime.close")


class SyncTaskBootstrap:
    """Resolve a unit id into a fully-loaded :class:`SyncTaskContext`."""

    @staticmethod
    def load(session: Session, unit_id: str) -> SyncTaskContext:
        """Load + decrypt everything a unit needs. Implemented in CHAOS-2512."""

        raise NotImplementedError("CHAOS-2512: implement SyncTaskBootstrap.load")


class ProviderRuntimeCache:
    """Process-local, strictly-scoped runtime cache (TTL/LRU, closes on evict)."""

    def get(self, context: SyncTaskContext) -> ProviderRuntime:
        """Return a reusable runtime for the context's scope. CHAOS-2512."""

        raise NotImplementedError("CHAOS-2512: implement ProviderRuntimeCache.get")
