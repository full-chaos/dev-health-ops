"""Clause-level tests for CHAOS-3303's ``dimension_observation_adapters``.

Mirrors the mutation discipline already used by ``contracts_v2.validators``
and ``health_rule_registry``: each test names the exact zero-vs-no-data or
stale/unavailable boundary it proves, not just "the happy path returns
something".
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import FreshnessState, MetricID
from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability
from dev_health_ops.api.dev.data_health_service import (
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from dev_health_ops.api.dev.dimension_observation_adapters import (
    change_failure_rate_observation,
    data_trust_observation,
    incident_load_observation,
    unavailable_observation,
)
from dev_health_ops.api.dev.metrics.definitions import get_metric
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryResult,
    MetricQueryValue,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    IncidentFact,
    StatusResultState,
    StatusSnapshotResult,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_COMMON: dict[str, Any] = dict(
    subject_kind=RuleApplicability.PROJECT,
    subject_id="proj-1",
    cohort_size=None,
    window_index=0,
    observed_at=_NOW,
)


def _data_health_source(
    *, state: DataHealthState, coverage: float = 1.0
) -> DataHealthSource:
    return DataHealthSource(
        source_system="work_items",
        state=state,
        required=True,
        last_successful_at=_NOW,
        watermark=_NOW,
        missing_repository_ids=(),
        missing_entity_ids=(),
        coverage=coverage,
        confidence_impact=None,
        freshness_policy_version="v1",
    )


def _actual_completion(
    state: CompletionState = CompletionState.READY,
) -> ActualCompletion:
    return ActualCompletion(
        state=state,
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=(),
        required_children=(),
        conflicts=(),
        source_ref_ids=(),
        evidence_ref_ids=(),
    )


def _snapshot(
    *, state: StatusResultState, incidents: tuple[IncidentFact, ...] = ()
) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=state,
        scope=None,  # type: ignore[arg-type]  # unread by the adapter
        as_of=_NOW,
        declared=None,
        actual=_actual_completion(),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=incidents,
        source_refs=(),
        warnings=(),
    )


def _incident(entity_id: str) -> IncidentFact:
    return IncidentFact(
        entity_id=entity_id,
        display_label=entity_id,
        status="open",
        active=True,
        blocking=False,
        observed_at=_NOW,
        source_ref_id="ref-1",
        evidence_ref_ids=(),
    )


def _metric_result(
    *,
    state: MetricDataState,
    values: tuple[MetricQueryValue, ...] = (),
    coverage: float = 1.0,
    metadata: tuple[tuple[str, str], ...] = (),
) -> MetricQueryResult:
    return MetricQueryResult(
        definition=get_metric(MetricID.CHANGE_FAILURE_RATE),
        state=state,
        freshness=FreshnessState.FRESH,
        values=values,
        coverage=coverage,
        current_window_start=_NOW,
        current_window_end=_NOW,
        comparison_window_start=_NOW,
        comparison_window_end=_NOW,
        watermark=_NOW,
        source_refs=(),
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# unavailable_observation
# ---------------------------------------------------------------------------


def test_unavailable_observation_rejects_a_queried_state() -> None:
    """Kill site: the unmeasured-state guard dropped from unavailable_observation."""

    with pytest.raises(ValueError, match="not an unmeasured"):
        unavailable_observation(
            **_COMMON, state=SourceRequirementState.AVAILABLE_CURRENT
        )


def test_unavailable_observation_reports_not_measured() -> None:
    observation = unavailable_observation(**_COMMON)
    assert observation.data_semantics == "not_measured"
    assert observation.current_value is None


# ---------------------------------------------------------------------------
# data_trust_observation
# ---------------------------------------------------------------------------


def test_data_trust_complete_eligible_is_measured_healthy_value() -> None:
    """Kill site: complete_eligible=True must map to current_value=0.0, not None/1.0."""

    result = DataHealthResult(
        sources=(_data_health_source(state=DataHealthState.COMPLETE),),
        complete_eligible=True,
    )
    observation = data_trust_observation(result, **_COMMON)
    assert observation.current_value == 0.0
    assert observation.data_semantics == "measured_zero"
    assert observation.observed_states == (SourceRequirementState.AVAILABLE_CURRENT,)


def test_data_trust_not_complete_eligible_is_measured_broken_value() -> None:
    """Kill site: complete_eligible=False must map to current_value=1.0 (triggers)."""

    result = DataHealthResult(
        sources=(_data_health_source(state=DataHealthState.COMPLETE),),
        complete_eligible=False,
    )
    observation = data_trust_observation(result, **_COMMON)
    assert observation.current_value == 1.0


def test_data_trust_stale_source_reports_available_stale() -> None:
    result = DataHealthResult(
        sources=(_data_health_source(state=DataHealthState.STALE),),
        complete_eligible=False,
    )
    observation = data_trust_observation(result, **_COMMON)
    assert observation.observed_states == (SourceRequirementState.AVAILABLE_STALE,)
    assert observation.current_value == 1.0


def test_data_trust_unavailable_source_is_honestly_unmeasured() -> None:
    """Kill site: an unmeasured worst source must never be reported as a measured value."""

    result = DataHealthResult(
        sources=(_data_health_source(state=DataHealthState.UNAVAILABLE),),
        complete_eligible=False,
    )
    observation = data_trust_observation(result, **_COMMON)
    assert observation.data_semantics == "not_measured"
    assert observation.current_value is None


def test_data_trust_no_sources_is_no_data_not_measured_zero() -> None:
    result = DataHealthResult(sources=(), complete_eligible=True)
    observation = data_trust_observation(result, **_COMMON)
    assert observation.data_semantics == "no_data"
    assert observation.current_value is None


def test_data_trust_worst_source_wins_across_mixed_states() -> None:
    """Kill site: severity selection must pick UNAVAILABLE over COMPLETE, not the first row."""

    result = DataHealthResult(
        sources=(
            _data_health_source(state=DataHealthState.COMPLETE),
            _data_health_source(state=DataHealthState.UNAVAILABLE),
        ),
        complete_eligible=False,
    )
    observation = data_trust_observation(result, **_COMMON)
    assert observation.data_semantics == "not_measured"


# ---------------------------------------------------------------------------
# incident_load_observation
# ---------------------------------------------------------------------------


def test_incident_load_zero_incidents_is_a_measured_zero_not_no_data() -> None:
    """Kill site: a genuinely queried zero-incident count is measured_zero, not no_data."""

    snapshot = _snapshot(state=StatusResultState.COMPLETE, incidents=())
    observation = incident_load_observation(snapshot, **_COMMON)
    assert observation.current_value == 0.0
    assert observation.data_semantics == "measured_zero"


def test_incident_load_counts_real_incidents() -> None:
    snapshot = _snapshot(
        state=StatusResultState.COMPLETE,
        incidents=(_incident("inc-1"), _incident("inc-2")),
    )
    observation = incident_load_observation(snapshot, **_COMMON)
    assert observation.current_value == 2.0
    assert observation.sample_count == 2


def test_incident_load_insufficient_evidence_is_unmeasured() -> None:
    """Kill site: INSUFFICIENT_EVIDENCE maps to UNAVAILABLE, must stay not_measured."""

    snapshot = _snapshot(
        state=StatusResultState.INSUFFICIENT_EVIDENCE,
        incidents=(_incident("inc-1"),),
    )
    observation = incident_load_observation(snapshot, **_COMMON)
    assert observation.data_semantics == "not_measured"
    assert observation.current_value is None


def test_incident_load_degraded_reports_available_unknown() -> None:
    snapshot = _snapshot(
        state=StatusResultState.DEGRADED, incidents=(_incident("inc-1"),)
    )
    observation = incident_load_observation(snapshot, **_COMMON)
    assert observation.observed_states == (SourceRequirementState.AVAILABLE_UNKNOWN,)
    assert observation.current_value == 1.0


# ---------------------------------------------------------------------------
# change_failure_rate_observation
# ---------------------------------------------------------------------------


def test_change_failure_rate_measured_value_passes_through() -> None:
    result = _metric_result(
        state=MetricDataState.VALUE,
        values=(
            MetricQueryValue(dimensions=(), value=0.2, comparison_value=0.1, series=()),
        ),
        coverage=0.9,
    )
    observation = change_failure_rate_observation(result, **_COMMON)
    assert observation.current_value == 0.2
    assert observation.comparison_value == 0.1
    assert observation.coverage == 0.9
    assert observation.denominator_present is True


def test_change_failure_rate_zero_denominator_reports_denominator_absent() -> None:
    """Kill site: INSUFFICIENT_EVIDENCE (zero-denominator) must not claim a denominator."""

    result = _metric_result(
        state=MetricDataState.INSUFFICIENT_EVIDENCE,
        values=(),
        metadata=(("empty_reason", "zero_denominator"),),
    )
    observation = change_failure_rate_observation(result, **_COMMON)
    assert observation.denominator_present is False
    assert observation.current_value is None


def test_change_failure_rate_unconfigured_is_unmeasured() -> None:
    result = _metric_result(state=MetricDataState.UNCONFIGURED, values=())
    observation = change_failure_rate_observation(result, **_COMMON)
    assert observation.data_semantics == "not_measured"


def test_change_failure_rate_stale_state_reports_available_stale() -> None:
    result = _metric_result(
        state=MetricDataState.STALE,
        values=(
            MetricQueryValue(
                dimensions=(), value=0.0, comparison_value=None, series=()
            ),
        ),
    )
    observation = change_failure_rate_observation(result, **_COMMON)
    assert observation.observed_states == (SourceRequirementState.AVAILABLE_STALE,)
    assert observation.current_value == 0.0
    assert observation.data_semantics == "measured_zero"
