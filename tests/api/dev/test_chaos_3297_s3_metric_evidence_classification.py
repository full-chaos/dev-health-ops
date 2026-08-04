"""Tests for CHAOS-3297 stack #3's F10 metric half (team-lead ruling,
2026-08-02, option (a)): ``DevMetricRefV2.evidence_classification`` closes
the metric-grounding gap ``validate_frame_grounding``'s own docstring
originally left open.

Covers: the closed vocabulary, the exclusive-OR invariant at both the
per-metric ``DevMetricRefV2`` layer and the frame-level
``validate_frame_grounding`` re-check (plant both defects: classification
set alongside real evidence, and neither present), and that
``wrap_legacy_answer_as_frame`` sets the classification unconditionally on
every v1-sourced metric.
"""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevAnswer
from dev_health_ops.api.dev.contracts_v2.base import PublicOutcome
from dev_health_ops.api.dev.contracts_v2.embedded import (
    DevMetricRefV2,
    DevScopeV2,
    MetricEvidenceClassification,
)
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame
from dev_health_ops.api.dev.contracts_v2.validators import validate_frame_grounding

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _scope_v2() -> DevScopeV2:
    return DevScopeV2.model_validate(
        {
            "schema_version": "dev_scope.v1",
            "organization_id": "org_fullchaos",
            "direct_scope": "repository",
            "repositories": ["repo_dev_health"],
            "entity_refs": [],
            "team_ids": [],
            "time_range": {
                "start": "2026-06-28T00:00:00Z",
                "end": "2026-07-28T00:00:00Z",
                "timezone": "UTC",
            },
            "comparison_range": None,
            "surface_context": None,
        }
    )


def _metric(**overrides: object) -> DevMetricRefV2:
    base: dict[str, object] = dict(
        schema_version="dev_metric_ref.v1",
        metric_ref_id="metric_01",
        metric_id="cycle_time_p50_hours",
        label="Cycle time (p50)",
        definition_version="ask_dev_metrics.v1",
        unit="hours",
        aggregation="p50",
        display_precision=1,
        resolved_scope=_scope_v2(),
        dimensions=(),
        current_window={
            "start": "2026-06-28T00:00:00Z",
            "end": "2026-07-28T00:00:00Z",
            "timezone": "UTC",
        },
        comparison_window=None,
        value=12.5,
        comparison_value=None,
        series=(),
        query_version="ask_dev_queries.v1",
        source_version="work_graph.v1",
        freshness="fresh",
        coverage=1.0,
        evidence_ref_ids=(),
        evidence_classification=None,
    )
    base.update(overrides)
    return DevMetricRefV2(**base)


def test_metric_with_evidence_and_no_classification_is_valid() -> None:
    metric = _metric(evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,))
    assert metric.evidence_ref_ids == (_REAL_EVIDENCE_HANDLE,)
    assert metric.evidence_classification is None


def test_metric_with_classification_and_no_evidence_is_valid() -> None:
    metric = _metric(
        evidence_classification=MetricEvidenceClassification.LEGACY_V1_UNMINTED
    )
    assert metric.evidence_ref_ids == ()
    assert (
        metric.evidence_classification
        is MetricEvidenceClassification.LEGACY_V1_UNMINTED
    )


def test_metric_with_both_evidence_and_classification_is_rejected() -> None:
    """Plant defect 1 (team-lead ruling requirement 2): a plan-minted
    metric with the classification also set must FAIL.
    """

    with pytest.raises(ValidationError, match="must not also carry"):
        _metric(
            evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,),
            evidence_classification=MetricEvidenceClassification.LEGACY_V1_UNMINTED,
        )


def test_metric_with_neither_evidence_nor_classification_is_rejected() -> None:
    """Plant defect 2 (team-lead ruling requirement 2): a metric with
    neither must FAIL.
    """

    with pytest.raises(ValidationError, match="requires either"):
        _metric()


def test_legacy_v1_unminted_is_the_only_member_today() -> None:
    """Regression fence for the closed vocabulary's totality."""

    assert {member.value for member in MetricEvidenceClassification} == {
        "legacy_v1_unminted"
    }


# ---------------------------------------------------------------------------
# Frame-level re-check (validate_frame_grounding), independent of
# DevMetricRefV2's own construction-time validator -- proves the frame-level
# guard is reachable on its own, not merely inheriting the nested model's
# check (defense in depth, per that function's own docstring).
# ---------------------------------------------------------------------------


def _frame_with_metric(metric: DevMetricRefV2) -> DevAnswerFrame:
    from dev_health_ops.api.dev.contracts_v2.embedded import DevCoverageV2
    from dev_health_ops.api.dev.contracts_v2.frame import DevFrameVersions

    return DevAnswerFrame(
        schema_version="dev_answer_frame.v1",
        frame_id="00000000-0000-0000-0000-0000000000f1",
        run_id="00000000-0000-0000-0000-0000000000f2",
        generated_at=_NOW,
        public_outcome=PublicOutcome.ANSWERED,
        direct_answer="Cycle time is 12.5 hours.",
        sections=(
            {
                "section_id": "summary",
                "title": "Summary",
                "fact_ids": (),
            },
        ),
        facts=(),
        metrics=(metric,),
        coverage=DevCoverageV2(
            required_source_count=1,
            available_source_count=1,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=_NOW,
        ),
        versions=DevFrameVersions(
            interpreter_version="intent_interpreter.v1",
            plan_id="metric.comparison.v1",
            plan_version="metric.comparison.v1.0",
            tool_contract_version="ask_dev_tools.v1",
            metric_definition_version="ask_dev_metrics.v1",
            query_version="ask_dev_queries.v1",
        ),
    )


def test_frame_grounding_accepts_a_metric_with_real_evidence() -> None:
    metric = _metric(evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,))
    frame = _frame_with_metric(metric)
    validate_frame_grounding(frame)  # does not raise


def test_frame_grounding_accepts_a_metric_with_classification() -> None:
    metric = _metric(
        evidence_classification=MetricEvidenceClassification.LEGACY_V1_UNMINTED
    )
    frame = _frame_with_metric(metric)
    validate_frame_grounding(frame)  # does not raise


def test_frame_grounding_rejects_a_metric_bypassing_its_own_construction_check() -> (
    None
):
    """Proves ``validate_frame_grounding`` is a REAL second gate, not
    vacuous alongside ``DevMetricRefV2``'s own construction-time
    validator. ``model_copy`` never reruns any validator -- on the metric
    itself (to strip its evidence down to "neither"), and again on the
    whole FRAME (to swap the now-invalid metric in without going through
    ``DevAnswerFrame``'s own constructor, which -- confirmed by
    ``test_metric_with_neither_evidence_nor_classification_is_rejected``'s
    sibling case above -- revalidates nested instances and would otherwise
    reject this before ``validate_frame_grounding`` ever ran).
    """

    valid_frame = _frame_with_metric(_metric(evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,)))
    bypassed_metric = valid_frame.metrics[0].model_copy(
        update={"evidence_ref_ids": ()}
    )  # now neither
    frame = valid_frame.model_copy(update={"metrics": (bypassed_metric,)})

    with pytest.raises(ValueError, match="F10: metric"):
        validate_frame_grounding(frame)


# ---------------------------------------------------------------------------
# wrap_legacy_answer_as_frame: unconditional classification on every
# v1-sourced metric (team-lead ruling requirement 3).
# ---------------------------------------------------------------------------


def _legacy_answer() -> DevAnswer:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    text = __import__("json").dumps(payload, default=str)
    payload = __import__("json").loads(re.sub(r"ev_\d+", _REAL_EVIDENCE_HANDLE, text))
    for metric in payload.get("metrics", []):
        metric["evidence_ref_ids"] = []
    return DevAnswer.model_validate(payload)


def test_wrap_legacy_answer_sets_classification_on_every_metric() -> None:
    answer = _legacy_answer()
    assert answer.metrics, "fixture must carry at least one metric for this test"
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_01")

    assert len(frame.metrics) == len(answer.metrics)
    for metric in frame.metrics:
        assert metric.evidence_ref_ids == ()
        assert metric.evidence_classification is (
            MetricEvidenceClassification.LEGACY_V1_UNMINTED
        )
