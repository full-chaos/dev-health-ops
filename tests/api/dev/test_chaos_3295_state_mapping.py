"""Exhaustive, clause-level coverage of CHAOS-3295's state-mapping functions.

Each mapping function is a total ``match`` over a closed source enum;
these tests assert every arm individually so a mutation that swaps or
deletes one arm changes an observable assertion, not just "some" test.
"""

from __future__ import annotations

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.data_health_service import DataHealthState
from dev_health_ops.api.dev.investigation_plans.state_mapping import (
    data_health_state_to_requirement_state,
    metric_data_state_to_requirement_state,
    queried_semantics,
    status_result_state_to_requirement_state,
    work_graph_result_state_to_requirement_state,
)
from dev_health_ops.api.dev.metrics.service import MetricDataState
from dev_health_ops.api.dev.status_change_service import StatusResultState
from dev_health_ops.api.dev.work_graph_neighbors_service import WorkGraphResultState


def test_queried_semantics_boundary():
    assert queried_semantics(0) == "no_data"
    assert queried_semantics(1) == "measured_zero"
    assert queried_semantics(1_000) == "measured_zero"


def test_data_health_state_mapping_is_exhaustive_and_distinct():
    expected = {
        DataHealthState.COMPLETE: SourceRequirementState.AVAILABLE_CURRENT,
        DataHealthState.STALE: SourceRequirementState.AVAILABLE_STALE,
        DataHealthState.NO_DATA: SourceRequirementState.AVAILABLE_CURRENT,
        DataHealthState.UNCONFIGURED: SourceRequirementState.UNCONFIGURED,
        DataHealthState.UNAVAILABLE: SourceRequirementState.UNAVAILABLE,
        DataHealthState.UNAUTHORIZED: SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
    }
    assert set(expected) == set(DataHealthState)
    for source, target in expected.items():
        assert data_health_state_to_requirement_state(source) == target
    # STALE and NO_DATA both mapping to something *other* than UNCONFIGURED
    # is the load-bearing distinction a mutation could collapse silently.
    assert data_health_state_to_requirement_state(
        DataHealthState.STALE
    ) != data_health_state_to_requirement_state(DataHealthState.UNCONFIGURED)


def test_status_result_state_mapping_is_exhaustive_and_distinct():
    expected = {
        StatusResultState.COMPLETE: SourceRequirementState.AVAILABLE_CURRENT,
        StatusResultState.PARTIAL: SourceRequirementState.AVAILABLE_STALE,
        StatusResultState.DEGRADED: SourceRequirementState.AVAILABLE_UNKNOWN,
        StatusResultState.INSUFFICIENT_EVIDENCE: SourceRequirementState.UNAVAILABLE,
    }
    assert set(expected) == set(StatusResultState)
    for source, target in expected.items():
        assert status_result_state_to_requirement_state(source) == target
    assert len(set(expected.values())) == 4  # every arm distinct


def test_metric_data_state_mapping_is_exhaustive():
    expected = {
        MetricDataState.VALUE: SourceRequirementState.AVAILABLE_CURRENT,
        MetricDataState.ZERO: SourceRequirementState.AVAILABLE_CURRENT,
        MetricDataState.NO_MATCH: SourceRequirementState.AVAILABLE_CURRENT,
        MetricDataState.PARTIAL: SourceRequirementState.AVAILABLE_UNKNOWN,
        MetricDataState.INSUFFICIENT_EVIDENCE: SourceRequirementState.AVAILABLE_UNKNOWN,
        MetricDataState.STALE: SourceRequirementState.AVAILABLE_STALE,
        MetricDataState.UNCONFIGURED: SourceRequirementState.UNCONFIGURED,
        MetricDataState.UNAVAILABLE: SourceRequirementState.UNAVAILABLE,
    }
    assert set(expected) == set(MetricDataState)
    for source, target in expected.items():
        assert metric_data_state_to_requirement_state(source) == target


def test_work_graph_result_state_mapping_never_uses_truncated():
    expected = {
        WorkGraphResultState.COMPLETE: SourceRequirementState.AVAILABLE_CURRENT,
        WorkGraphResultState.EMPTY: SourceRequirementState.AVAILABLE_CURRENT,
        WorkGraphResultState.PARTIAL: SourceRequirementState.AVAILABLE_STALE,
    }
    assert set(expected) == set(WorkGraphResultState)
    for source, target in expected.items():
        result = work_graph_result_state_to_requirement_state(source)
        assert result == target
        # PARTIAL keeps its (nonzero) facts -- never the TRUNCATED state,
        # which the contract reserves for a result with zero usable facts.
        assert result != SourceRequirementState.TRUNCATED
