"""Instrument for the cache-epoch bypass path (CHAOS-4226).

``epoch_cache_key`` folds the per-org cache epoch into the home/explain cache
key. When the epoch is UNREADABLE (a backend error, not an absent key) the
request bypasses the cache -- no read, no write -- rather than guess epoch 0
and risk serving a pre-finalize entry. A bypass is neither a hit nor a
consumed invalidation, so it needs its own signal: this counter, labelled by
cache prefix, plus the ``cache.epoch_unreadable`` structured log line at the
bypass site. A rising series means the API is recomputing every request for
that view and Valkey needs attention.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "CACHE_EPOCH_UNREADABLE_TOTAL",
    "build_cache_epoch_unreadable_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_cache_epoch_unreadable_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of epoch-scoped cache reads that bypassed the cache because
    the org's cache epoch could not be read, by cache prefix."""
    return build_counter(
        "devhealth_cache_epoch_unreadable_total",
        "Epoch-scoped cache reads that bypassed the cache because the org "
        "cache epoch was unreadable, by cache prefix",
        ["prefix"],
        meter=meter,
        prometheus=prometheus,
    )


CACHE_EPOCH_UNREADABLE_TOTAL = build_cache_epoch_unreadable_counter()
