"""Instrument for provider source discovery (CHAOS-4602), both seams.

The native Go materializer (scheduled occurrences,
``internal/scheduler/sync/source_discovery.go``) and :func:`plan_sync_run`
(manual "Sync Now", admin API backfill, and the operator backfill tool --
every trigger path that is NOT a scheduled occurrence) each run their own
source-discovery step immediately before unit planning reads
``integration_sources``. Both seams publish under the exact same metric
name and label set, so one series covers the whole capability regardless
of which plane produced it.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "PROVIDER_SOURCE_DISCOVERY_TOTAL",
    "build_provider_source_discovery_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_provider_source_discovery_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of provider source-discovery outcomes, by provider and outcome.

    ``outcome`` is one of ``created|existing|skipped|error`` -- shares its
    name/labels with the Go-native materializer seam's own counter
    (``source_discovery.go``'s ``provider_source_discovery_total``).
    """
    return build_counter(
        "provider_source_discovery_total",
        "Provider source (repo/project) discovery outcomes, by provider and "
        "outcome (created|existing|skipped|error). Existing rows are never "
        "flipped by discovery (is_enabled/discovered_at untouched); skipped "
        "means the sync config has an explicit single-source scope.",
        ["provider", "outcome"],
        meter=meter,
        prometheus=prometheus,
    )


PROVIDER_SOURCE_DISCOVERY_TOTAL = build_provider_source_discovery_counter()
