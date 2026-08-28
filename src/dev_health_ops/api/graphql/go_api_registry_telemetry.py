"""Instruments for the Go API operation rollout registry (CHAOS-4366 Wave 0).

Every registry read/write is a decision that either keeps traffic on Python
or moves it to Go -- a lookup that silently no-ops (e.g. a DB error swallowed
by a broad except) would read as "stay on Python", which is safe by
construction, but invisible: an operator would have no way to tell "nothing
is canaried yet" apart from "the registry stopped answering". These counters
exist so both states have their own signal, the same reasoning as
``investment_coverage_telemetry``'s resolver-fallback counter.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "GO_API_REGISTRY_LOOKUP_TOTAL",
    "GO_API_CANDIDATE_BUILD_REGISTERED_TOTAL",
    "GO_API_PROOF_RUN_RECORDED_TOTAL",
    "build_go_api_registry_lookup_counter",
    "build_go_api_candidate_build_registered_counter",
    "build_go_api_proof_run_recorded_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_go_api_registry_lookup_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of routing-state lookups, by outcome.

    ``result`` is one of ``hit`` (a routing-state row exists for the operation
    triple), ``miss`` (none registered -- the caller must fall back to
    Python), or ``error`` (the lookup itself failed, e.g. DB unavailable --
    distinct from ``miss`` on purpose: an unregistered operation and a broken
    registry both currently resolve to "stay on Python", but only one of them
    is an incident).
    """
    return build_counter(
        "devhealth_go_api_registry_lookup_total",
        "Go API operation rollout registry lookups, by result",
        ["result", "mode"],
        meter=meter,
        prometheus=prometheus,
    )


def build_go_api_candidate_build_registered_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of candidate-build registrations, by whether the row was newly
    inserted or already existed (registering the same build twice is a
    no-op, not an error -- see CandidateBuild's immutability contract)."""
    return build_counter(
        "devhealth_go_api_candidate_build_registered_total",
        "Go API candidate-build registrations, by outcome",
        ["outcome"],
        meter=meter,
        prometheus=prometheus,
    )


def build_go_api_proof_run_recorded_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of proof runs recorded, by stage and terminal_state -- the
    comparator's own outcome telemetry (plan §5's terminal-state vocabulary)."""
    return build_counter(
        "devhealth_go_api_proof_run_recorded_total",
        "Go API proof-gate runs recorded, by stage and terminal state",
        ["stage", "terminal_state"],
        meter=meter,
        prometheus=prometheus,
    )


GO_API_REGISTRY_LOOKUP_TOTAL = build_go_api_registry_lookup_counter()
GO_API_CANDIDATE_BUILD_REGISTERED_TOTAL = (
    build_go_api_candidate_build_registered_counter()
)
GO_API_PROOF_RUN_RECORDED_TOTAL = build_go_api_proof_run_recorded_counter()
