"""Instruments for :mod:`investment_explain_dispatcher` (CHAOS-4977 step
5b) -- a separate counter family from ``go_api_dispatch_telemetry``'s,
because this dispatcher has no document digest and no registry row: its
own module keeps that vocabulary from leaking into the GraphQL
dispatcher's ``fallback_reason`` set.

``fallback_reason`` vocabulary (every branch that returns to Python):
``disabled`` -- ``GO_API_INVESTMENT_EXPLAIN_ENABLED`` is not truthy
    (includes unset, the default-OFF state).
``no_target_url`` -- ``GO_API_QUERY_API_URL`` unset; nothing to forward
    to regardless of the route switch.
``envelope_inputs_missing`` -- tier/licensed_features failed to resolve
    for ``current_user.org_id``.
``envelope_signing_error`` -- ``issue_effective_principal_envelope``
    raised.
``go_timeout`` / ``go_connection_error`` -- the outbound call to
    query-api itself failed before headers arrived.
``go_non_200`` -- query-api answered with a non-200 status (query-api
    has no digest to drift on for this route, so this stays one generic
    reason rather than the GraphQL dispatcher's 404/405 split).
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "INVESTMENT_EXPLAIN_DISPATCH_ATTEMPTED_TOTAL",
    "INVESTMENT_EXPLAIN_DISPATCH_SERVED_GO_TOTAL",
    "INVESTMENT_EXPLAIN_DISPATCH_FALLBACK_TOTAL",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

#: Every call into maybe_dispatch_investment_explain_to_go -- i.e. every
#: request the route handler saw, whether or not it ended up served by
#: Go. Unlike the GraphQL dispatcher there is no catalog-match filter:
#: this route has exactly one operation.
#: `operation` always carries the single literal "investment_explain" --
#: kept as a label rather than dropped (this route has only one operation)
#: so both counters share prometheus_client's normal labeled-Counter shape
#: (an unlabeled Counter rejects `.labels()` entirely, and go_api_dispatch_
#: telemetry's own counters are always called through `.labels()`).
INVESTMENT_EXPLAIN_DISPATCH_ATTEMPTED_TOTAL = build_counter(
    "devhealth_investment_explain_dispatch_attempted_total",
    "POST /api/v1/investment/explain requests seen by the Go dispatcher",
    ["operation"],
    meter=_meter,
    prometheus=_prometheus,
)

INVESTMENT_EXPLAIN_DISPATCH_SERVED_GO_TOTAL = build_counter(
    "devhealth_investment_explain_dispatch_served_go_total",
    "POST /api/v1/investment/explain requests actually served by query-api",
    ["operation"],
    meter=_meter,
    prometheus=_prometheus,
)

INVESTMENT_EXPLAIN_DISPATCH_FALLBACK_TOTAL = build_counter(
    "devhealth_investment_explain_dispatch_fallback_total",
    "POST /api/v1/investment/explain requests that fell back to Python, by reason",
    ["reason"],
    meter=_meter,
    prometheus=_prometheus,
)
