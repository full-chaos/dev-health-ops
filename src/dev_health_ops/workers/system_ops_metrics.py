"""Prometheus and OpenTelemetry instruments for the billing-notification
completion fence (CHAOS-3952).

The dual-backend plumbing lives in ``dev_health_ops.telemetry_metrics``; this
module keeps its own meter so the instrument stays attributed to
``system_ops``, the module that owns the fence.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

# outcome labels:
#   sent                 - dispatch performed, fence newly recorded
#   duplicate_suppressed - fence already set; dispatch skipped
BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL = build_counter(
    "devhealth_billing_notification_completion_fence_total",
    "Billing notification durable completion-fence outcomes by result",
    ["outcome"],
    meter=_meter,
    prometheus=_prometheus,
)
