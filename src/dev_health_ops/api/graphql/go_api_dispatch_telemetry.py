"""Instruments for the edge dispatcher itself (CHAOS-4697), distinct from
``go_api_registry_telemetry``'s registry-read counters.

Standing order (CHAOS-4697 brief): telemetry on every branch. A fallback
that is not counted is indistinguishable from Go never being asked --
these counters are what let an operator tell those apart.

``fallback_reason`` vocabulary (every branch that returns to Python):
``no_catalog_match`` -- the request's document digest matched no known
    Go-eligible operation (the overwhelming majority of GraphQL traffic;
    not an error).
``registry_lookup_error`` -- ``lookup_routing_state`` raised.
``no_routing_row`` -- no row for this operation triple (unregistered).
``mode_python`` / ``mode_disabled`` -- explicit safe-default modes.
``mode_shadow_unimplemented`` -- ``mode='shadow'`` requested but the
    shadow executor does not exist (ruled SKIP for local, CHAOS-4697
    brief); the client still gets served, but this must never look like
    ``mode_python`` -- see ``go_api_dispatcher.py``.
``unauthenticated`` -- no authenticated user on the context; never
    forwarded to Go.
``envelope_inputs_missing`` -- ``tier``/``licensed_features`` unresolved
    on the context (best-effort in ``get_context``; can legitimately be
    ``None``).
``envelope_signing_error`` -- ``issue_effective_principal_envelope`` raised
    (e.g. missing/malformed signing key).
``go_timeout`` / ``go_connection_error`` / ``go_5xx`` -- the outbound call
    to query-api itself failed or errored.
``go_404_digest_miss`` -- query-api 404'd DESPITE a local catalog match --
    post-CHAOS-4696 this means digest DRIFT between the edge's catalog and
    the deployed query-api binary, not "unregistered". Alert-worthy.
``go_405_method_not_allowed`` -- defensive; should not occur once the
    dispatcher always forwards as POST (CHAOS-4706), kept as a named
    reason rather than folding into a generic "unexpected status".
``go_unexpected_status`` -- any other non-2xx status from query-api.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    build_histogram,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "GO_API_DISPATCH_ATTEMPTED_TOTAL",
    "GO_API_DISPATCH_SERVED_GO_TOTAL",
    "GO_API_DISPATCH_FALLBACK_TOTAL",
    "GO_API_DISPATCH_DIGEST_MISS_TOTAL",
    "GO_API_DISPATCH_LATENCY_SECONDS",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

#: Every request whose document digest matched a known Go-eligible
#: operation -- i.e. every request for which a routing DECISION was made,
#: whether or not it ended up served by Go. Requests that never matched
#: the catalog at all are not counted here (see `no_catalog_match` above);
#: counting those would make this a proxy for total GraphQL traffic
#: instead of a proxy for "dispatch logic actually ran".
GO_API_DISPATCH_ATTEMPTED_TOTAL = build_counter(
    "devhealth_go_api_dispatch_attempted_total",
    "GraphQL requests whose document digest matched a Go-eligible operation",
    ["operation"],
    meter=_meter,
    prometheus=_prometheus,
)

GO_API_DISPATCH_SERVED_GO_TOTAL = build_counter(
    "devhealth_go_api_dispatch_served_go_total",
    "GraphQL requests actually served by query-api",
    ["operation"],
    meter=_meter,
    prometheus=_prometheus,
)

GO_API_DISPATCH_FALLBACK_TOTAL = build_counter(
    "devhealth_go_api_dispatch_fallback_total",
    "GraphQL requests that fell back to Python, by operation and reason",
    ["operation", "reason"],
    meter=_meter,
    prometheus=_prometheus,
)

#: The specific, alert-worthy digest-miss-on-forward case (query-api 404
#: despite a local catalog match) -- also counted under
#: GO_API_DISPATCH_FALLBACK_TOTAL(reason="go_404_digest_miss"), broken out
#: on its own so an alert can key on this one counter instead of a label
#: filter.
GO_API_DISPATCH_DIGEST_MISS_TOTAL = build_counter(
    "devhealth_go_api_dispatch_digest_miss_total",
    "query-api 404s despite a local operation-catalog match -- digest drift",
    ["operation"],
    meter=_meter,
    prometheus=_prometheus,
)

GO_API_DISPATCH_LATENCY_SECONDS = build_histogram(
    "devhealth_go_api_dispatch_latency_seconds",
    "Dispatch latency by plane actually served and outcome",
    ["plane", "outcome"],
    (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    meter=_meter,
    prometheus=_prometheus,
)
