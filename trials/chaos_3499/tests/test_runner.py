"""Proof that a crash inside oracle evaluation cannot abort the sweep.

``harness.runner.run_oracle``'s own docstring promises that a measurement
which blows up is recorded, not dropped. Before this suite existed that
promise held only for exceptions raised inside the arm callable -- an
exception raised inside ``oracle.evaluate`` itself (e.g. a tz-naive watermark
comparing against an aware one in ``Oracle._assert_watermark``) propagated
straight out of :func:`harness.runner.run_trial`, silently truncating every
oracle after the one that crashed.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime

from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..harness.contracts import ArmResponse
from ..harness.oracle import Oracle, Verdict
from ..harness.runner import run_trial
from .golden import golden_response

# Deliberately tz-naive: `Oracle._assert_watermark` compares
# `watermark >= self.require_indexed_through_at_or_after`, and comparing a
# naive datetime against ground_truth's timezone-aware constants raises
# TypeError -- inside `oracle.evaluate`, not inside the arm callable.
_NAIVE_WATERMARK = datetime(2026, 7, 30)


def _naive_watermark_arm(oracle: Oracle) -> ArmResponse:
    golden = golden_response(oracle, "naive-watermark-arm")
    return dataclasses.replace(golden, indexed_through=_NAIVE_WATERMARK)


def test_oracle_evaluation_exception_does_not_abort_the_sweep() -> None:
    report = run_trial(ALL_ORACLES, "naive-watermark-arm", _naive_watermark_arm)

    assert len(report.results) == len(ALL_ORACLES), (
        "an exception inside oracle.evaluate() must not drop the remaining "
        "oracles from the sweep -- old code let it propagate out of "
        "run_trial and truncate the report"
    )

    crashing = [
        result
        for result in report.results
        if ORACLES_BY_ID[result.oracle_id].require_indexed_through_at_or_after
        is not None
    ]
    assert crashing, "the naive watermark must actually reach a freshness assertion"
    for result in crashing:
        assert result.verdict.is_failure, (
            f"{result.oracle_id} must fail loudly when its own evaluation "
            "crashes, not silently vanish from the report"
        )
        assert any(
            a.assertion_id == "oracle_evaluation_crashed" for a in result.assertions
        ), (
            f"{result.oracle_id}'s crash must be recorded under its own "
            "assertion id, not folded into an unrelated one"
        )

    # Oracles with no freshness requirement never reach the crashing
    # assertion at all -- they must be unaffected by their neighbours' crash.
    unaffected = [
        result
        for result in report.results
        if ORACLES_BY_ID[result.oracle_id].require_indexed_through_at_or_after is None
    ]
    assert unaffected
    for result in unaffected:
        assert not any(
            a.assertion_id == "oracle_evaluation_crashed" for a in result.assertions
        )


def test_normal_sweep_is_unaffected_by_the_crash_guard() -> None:
    """Control: a well-behaved arm still sweeps clean with no crash records."""

    def _perfect_arm(oracle: Oracle) -> ArmResponse:
        return golden_response(oracle, "perfect")

    report = run_trial(ALL_ORACLES, "perfect", _perfect_arm)
    assert len(report.results) == len(ALL_ORACLES)
    assert all(result.verdict is Verdict.PASS for result in report.results)
