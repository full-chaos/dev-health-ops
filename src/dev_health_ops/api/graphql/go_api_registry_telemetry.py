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
    "GO_API_ROUTING_DIGEST_DRIFT_TOTAL",
    "build_go_api_registry_lookup_counter",
    "build_go_api_candidate_build_registered_counter",
    "build_go_api_proof_run_recorded_counter",
    "build_go_api_routing_digest_drift_counter",
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


def build_go_api_routing_digest_drift_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of routing-table schema-digest drift checks, by result.

    Emitted ONCE per process start (api/_lifespan.py), not per request --
    the condition it reports is a property of the table versus the
    deployed SDL, which cannot change while a process runs.

    ``result`` is one of:

    * ``live``   -- at least one routing row exists at the digest this
      process computes. Dispatch can work.
    * ``stale``  -- rows exist, but NONE at the live digest. Every one of
      them is unreachable and every request silently falls back to
      Python. This is the 2026-09-01 outage's signal, and the reason this
      counter exists: for six days that state produced no metric, no log
      line, and no alert.
    * ``empty``  -- the table has no rows at all. Nothing was ever
      enabled, which is the legitimate default posture, NOT an incident.
      Distinguished from ``stale`` on purpose: the two look identical
      from the outside (no traffic reaches Go) and mean opposite things.
    * ``error``  -- the check itself could not run. Never conflated with
      ``empty``; an unreachable registry must not read as "nothing
      enabled" (the same distinction the lookup counter's ``error`` label
      draws).
    """
    return build_counter(
        "devhealth_go_api_routing_digest_drift_total",
        "Go API routing-state schema-digest drift checks at startup, by result",
        ["result"],
        meter=meter,
        prometheus=prometheus,
    )


GO_API_REGISTRY_LOOKUP_TOTAL = build_go_api_registry_lookup_counter()
GO_API_CANDIDATE_BUILD_REGISTERED_TOTAL = (
    build_go_api_candidate_build_registered_counter()
)
GO_API_PROOF_RUN_RECORDED_TOTAL = build_go_api_proof_run_recorded_counter()
GO_API_ROUTING_DIGEST_DRIFT_TOTAL = build_go_api_routing_digest_drift_counter()
