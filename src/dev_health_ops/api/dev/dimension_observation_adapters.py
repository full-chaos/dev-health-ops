"""Typed ``dimension_observation.v1`` adapters (CHAOS-3303).

Pure functions that turn an already-computed canonical service result
(:class:`~.data_health_service.DataHealthResult`,
:class:`~.status_change_service.StatusSnapshotResult`,
:class:`~.metrics.service.MetricQueryResult`) into a
:class:`~.contracts_v2.health_rules.DimensionObservation` for exactly one
:class:`~.contracts_v2.health_rules.HealthRuleDefinition`. No adapter here
computes a NEW metric, ratio, or threshold -- each one only re-expresses a
value the canonical service already produced in the shape
``evaluate_rule`` requires. A rule whose ``comparison_unit`` has no
canonical, scope-safe source yet (see ``health_profile_synthesis``'s
``UNBOUND_RULE_LIMITATIONS``) is never approximated by a different unit's
value here -- it is reported honestly via :func:`unavailable_observation`
instead, matching "No missing data as zero" and "No model-created
dimension, severity, percentage, or finding".

Reuses ``investigation_plans.state_mapping`` for every canonical
service-state -> ``SourceRequirementState`` mapping rather than
re-deriving it (CHAOS-3295's own module, already exhaustive/mypy-proven).
``DimensionObservation.validate_zero_semantics`` enforces one invariant
this module leans on throughout: whenever ``current_value`` is not
``None``, ``data_semantics`` must be ``"measured_zero"`` -- *regardless* of
whether the value itself happens to be numerically zero. ``"measured_zero"``
therefore means "this dimension carries a real, queried value", not "the
value is literally 0"; :func:`_value_semantics` below is the one place that
distinction is applied so every adapter agrees on it.
"""

from __future__ import annotations

from datetime import datetime

from .contracts_v2.base import SourceRequirementState
from .contracts_v2.health_rules import (
    DataSemantics,
    DimensionObservation,
    RuleApplicability,
)
from .data_health_service import DataHealthResult
from .investigation_plans.state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
    data_health_state_to_requirement_state,
    metric_data_state_to_requirement_state,
    status_result_state_to_requirement_state,
)
from .metrics.service import MetricDataState, MetricQueryResult
from .status_change_service import StatusSnapshotResult

__all__ = [
    "change_failure_rate_observation",
    "data_trust_observation",
    "incident_load_observation",
    "unavailable_observation",
]

#: Severity order (least to most available) for picking the worst mapped
#: state across several required data-health sources -- mirrors
#: ``investigation_plans.builtin_steps._data_health_outcome``'s own table
#: (that helper is module-private, so this is a deliberate, small,
#: independently-testable re-derivation over the same public
#: ``state_mapping`` output, not a duplicate of its business logic).
_STATE_SEVERITY: dict[SourceRequirementState, int] = {
    SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE: 0,
    SourceRequirementState.UNAVAILABLE: 1,
    SourceRequirementState.UNCONFIGURED: 2,
    SourceRequirementState.AVAILABLE_STALE: 3,
    SourceRequirementState.AVAILABLE_UNKNOWN: 4,
    SourceRequirementState.AVAILABLE_CURRENT: 5,
    SourceRequirementState.NOT_APPLICABLE: 6,
    SourceRequirementState.TRUNCATED: 3,
}


def _value_semantics(current_value: float | None) -> DataSemantics:
    """``measured_zero`` iff a real value is present -- see module docstring."""

    return "measured_zero" if current_value is not None else "no_data"


def unavailable_observation(
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
    state: SourceRequirementState = SourceRequirementState.UNAVAILABLE,
) -> DimensionObservation:
    """An honest, structural gap: no canonical source is wired for this rule yet.

    Never a stand-in for a real zero -- ``evaluate_rule`` maps every
    ``not_measured`` observation straight to ``DimensionState.UNKNOWN``
    (never ``healthy``), so a caller cannot mistake this for "measured and
    fine".
    """

    if state not in UNMEASURED_REQUIREMENT_STATES:
        raise ValueError(f"{state!r} is not an unmeasured SourceRequirementState")
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(state,),
        data_semantics="not_measured",
        sample_count=None,
        coverage=0.0,
        current_value=None,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def data_trust_observation(
    result: DataHealthResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.data_trust_broken.v1``: deterministic, from ``complete_eligible``.

    ``current_value`` is ``1.0`` (broken -- triggers the deterministic
    condition) when ``complete_eligible`` is ``False``, else ``0.0``. The
    worst individually-mapped source state across ``result.sources`` decides
    whether the dimension was measured at all: if every required source is
    itself unmeasured (unconfigured/unavailable/unauthorized), the whole
    dimension is honestly unmeasured too, never a fabricated "not broken".
    """

    if not result.sources:
        return DimensionObservation(
            schema_version="dimension_observation.v1",
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
            data_semantics="no_data",
            sample_count=0,
            coverage=0.0,
            current_value=None,
            comparison_value=None,
            denominator_present=False,
            attribution_present=False,
            window_index=window_index,
            observed_at=observed_at,
        )
    mapped = [data_health_state_to_requirement_state(s.state) for s in result.sources]
    worst = min(mapped, key=lambda state: _STATE_SEVERITY[state])
    coverage = sum(s.coverage for s in result.sources) / len(result.sources)
    if worst in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=worst,
        )
    current_value = 0.0 if result.complete_eligible else 1.0
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(worst,),
        data_semantics=_value_semantics(current_value),
        sample_count=len(result.sources),
        coverage=coverage,
        current_value=current_value,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def incident_load_observation(
    snapshot: StatusSnapshotResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.incident_load.v1``: the raw incident count from a status snapshot.

    ``StatusSnapshotResult.incidents`` is a real, already-queried fact list
    (CHAOS-3295's mandatory ``status_snapshot`` step); its length is exactly
    the rule's ``incident_count`` comparison unit -- no derived ratio, no
    invented threshold. A snapshot whose overall state never queried
    anything (``INSUFFICIENT_EVIDENCE`` -> ``UNAVAILABLE``) is reported
    unmeasured rather than a fabricated zero-incident count.
    """

    mapped = status_result_state_to_requirement_state(snapshot.state)
    if mapped in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=mapped,
        )
    current_value = float(len(snapshot.incidents))
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(mapped,),
        data_semantics=_value_semantics(current_value),
        sample_count=len(snapshot.incidents),
        coverage=1.0,
        current_value=current_value,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def change_failure_rate_observation(
    result: MetricQueryResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.change_failure_rate.v1``: the canonical ``CHANGE_FAILURE_RATE`` metric.

    Reuses ``MetricQueryService``'s own ``change_failure_rate`` computation
    verbatim -- this adapter only re-expresses ``MetricQueryResult`` as a
    ``DimensionObservation``, it never recomputes the ratio. A zero
    denominator (``MetricDataState.INSUFFICIENT_EVIDENCE``, the metric
    service's own "no denominator" projection) is reported with
    ``denominator_present=False``, never as a healthy zero rate.
    """

    mapped = metric_data_state_to_requirement_state(result.state)
    if mapped in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=mapped,
        )
    value = result.values[0] if result.values else None
    current_value = value.value if value is not None else None
    comparison_value = value.comparison_value if value is not None else None
    denominator_present = (
        result.state is not MetricDataState.INSUFFICIENT_EVIDENCE
        and current_value is not None
    )
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(mapped,),
        data_semantics=_value_semantics(current_value),
        sample_count=None,
        coverage=result.coverage,
        current_value=current_value,
        comparison_value=comparison_value,
        denominator_present=denominator_present,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )
