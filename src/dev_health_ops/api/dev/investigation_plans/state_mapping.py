"""Map each canonical service's own result-state enum onto the closed
``SourceRequirementState`` (CHAOS-3294 base contract) that every plan step
reports through :class:`.steps.StepOutcome`.

Every function here is a total, exhaustive ``match`` over its source enum
(mypy proves exhaustiveness) so a mutation that deletes or swaps one arm
changes an observable branch rather than silently falling through. Two
invariants apply everywhere:

* a *queried* result (the service actually ran) never reports
  ``usable_fact_count > 0`` with anything other than ``"measured_zero"``
  (the contract's own zero-vs-no-data rule, ``DevSourceObservation.
  validate_zero_semantics`` -- the name is the contract's, not chosen here);
  an empty-but-queried result reports ``"no_data"``.
* an *unmeasured* result (never queried, or found unusable) always reports
  ``usable_fact_count == 0``, ``data_semantics == "not_measured"``, and
  requires a ``limitation``.
"""

from __future__ import annotations

from ..contracts import FreshnessState
from ..contracts_v2.base import SourceRequirementState
from ..data_health_service import DataHealthState
from ..metrics.service import MetricDataState
from ..status_change_service import StatusResultState
from ..work_graph_neighbors_service import WorkGraphResultState

__all__ = [
    "queried_semantics",
    "unmeasured_limitation",
    "data_health_state_to_requirement_state",
    "status_result_state_to_requirement_state",
    "metric_data_state_to_requirement_state",
    "work_graph_result_state_to_requirement_state",
]


def queried_semantics(usable_fact_count: int) -> str:
    """The ``data_semantics`` value for any *queried* (non-unmeasured) result."""

    return "measured_zero" if usable_fact_count > 0 else "no_data"


def unmeasured_limitation(state: SourceRequirementState) -> str:
    """A bounded, content-free limitation string for an unmeasured source."""

    return f"source_{state.value}"


def data_health_state_to_requirement_state(
    state: DataHealthState,
) -> SourceRequirementState:
    match state:
        case DataHealthState.COMPLETE:
            return SourceRequirementState.AVAILABLE_CURRENT
        case DataHealthState.STALE:
            return SourceRequirementState.AVAILABLE_STALE
        case DataHealthState.NO_DATA:
            # Queried and configured, genuinely nothing in scope -- distinct
            # from UNCONFIGURED/UNAVAILABLE, which never ran at all.
            return SourceRequirementState.AVAILABLE_CURRENT
        case DataHealthState.UNCONFIGURED:
            return SourceRequirementState.UNCONFIGURED
        case DataHealthState.UNAVAILABLE:
            return SourceRequirementState.UNAVAILABLE
        case DataHealthState.UNAUTHORIZED:
            return SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE


def status_result_state_to_requirement_state(
    state: StatusResultState,
) -> SourceRequirementState:
    match state:
        case StatusResultState.COMPLETE:
            return SourceRequirementState.AVAILABLE_CURRENT
        case StatusResultState.PARTIAL:
            return SourceRequirementState.AVAILABLE_STALE
        case StatusResultState.DEGRADED:
            # At least one contributing source is itself unavailable, but the
            # facts that did return are still real and countable -- an
            # unmeasured state would force usable_fact_count to zero and
            # discard them.
            return SourceRequirementState.AVAILABLE_UNKNOWN
        case StatusResultState.INSUFFICIENT_EVIDENCE:
            return SourceRequirementState.UNAVAILABLE


def metric_data_state_to_requirement_state(
    state: MetricDataState,
) -> SourceRequirementState:
    match state:
        case MetricDataState.VALUE:
            return SourceRequirementState.AVAILABLE_CURRENT
        case MetricDataState.ZERO:
            # A genuinely measured zero value -- one usable fact ("the value
            # is 0"), never conflated with NO_MATCH below.
            return SourceRequirementState.AVAILABLE_CURRENT
        case MetricDataState.NO_MATCH:
            return SourceRequirementState.AVAILABLE_CURRENT
        case MetricDataState.PARTIAL:
            return SourceRequirementState.AVAILABLE_UNKNOWN
        case MetricDataState.INSUFFICIENT_EVIDENCE:
            return SourceRequirementState.AVAILABLE_UNKNOWN
        case MetricDataState.STALE:
            return SourceRequirementState.AVAILABLE_STALE
        case MetricDataState.UNCONFIGURED:
            return SourceRequirementState.UNCONFIGURED
        case MetricDataState.UNAVAILABLE:
            return SourceRequirementState.UNAVAILABLE


def work_graph_result_state_to_requirement_state(
    state: WorkGraphResultState,
) -> SourceRequirementState:
    match state:
        case WorkGraphResultState.COMPLETE:
            return SourceRequirementState.AVAILABLE_CURRENT
        case WorkGraphResultState.EMPTY:
            return SourceRequirementState.AVAILABLE_CURRENT
        case WorkGraphResultState.PARTIAL:
            # The graph query truncated at MAX_NEIGHBORS -- the edges that
            # did come back are still real and countable, so this is a
            # queried/stale state, not TRUNCATED (which the contract reserves
            # for a result with zero usable facts).
            return SourceRequirementState.AVAILABLE_STALE


def freshness_state_to_requirement_state(
    state: FreshnessState,
) -> SourceRequirementState:
    match state:
        case FreshnessState.FRESH:
            return SourceRequirementState.AVAILABLE_CURRENT
        case FreshnessState.STALE:
            return SourceRequirementState.AVAILABLE_STALE
        case FreshnessState.UNKNOWN:
            return SourceRequirementState.AVAILABLE_UNKNOWN
        case FreshnessState.UNAVAILABLE:
            return SourceRequirementState.UNAVAILABLE
