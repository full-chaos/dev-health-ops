"""Offline tests for run_measured_sweep.py's per-oracle logging.

Authoring-round item 1: the #1603 review round flagged that neither
measured sweep logged which specific class-(c) oracle passed in which
run -- only the aggregate `ComparisonReport`. These tests pin the
rendering functions directly, with hand-built results, so they need no
live model and no real sweep at all.
"""

from __future__ import annotations

from ..corpus.oracles import ORACLES_BY_ID
from ..harness.oracle import AssertionResult, OracleResult, Verdict
from ..harness.runner import TrialReport
from ..run_measured_sweep import _oracle_reason, _render_per_oracle_table


def _pass(oracle_id: str, arm: str) -> OracleResult:
    return OracleResult(
        oracle_id=oracle_id,
        arm=arm,
        question_class=ORACLES_BY_ID[oracle_id].question_class,
        assertions=(AssertionResult("arm_outcome", Verdict.PASS, "ok"),),
    )


def _not_measured(oracle_id: str, arm: str, reason: str) -> OracleResult:
    return OracleResult(
        oracle_id=oracle_id,
        arm=arm,
        question_class=ORACLES_BY_ID[oracle_id].question_class,
        assertions=(
            AssertionResult("measurement_happened", Verdict.NOT_MEASURED, reason),
        ),
    )


def test_oracle_reason_extracts_the_not_run_detail_verbatim() -> None:
    reason = (
        "arm reported NOT_RUN (measurement_not_run:"
        "not_authorable_for_extraction_arm:x); an unmeasured oracle is "
        "never a pass"
    )
    result = _not_measured("O3_supersession", "extraction_llm", reason)
    assert _oracle_reason(result) == reason


def test_oracle_reason_is_empty_for_a_measured_result() -> None:
    result = _pass("O3_supersession", "extraction_llm")
    assert _oracle_reason(result) == ""


def test_per_oracle_table_includes_every_oracle_arm_and_timestamp() -> None:
    """The core pin: a reader must be able to find, for ANY oracle x arm
    pair, its verdict AND when it was measured, in one table -- the exact
    gap #1603's review flagged (class (c)'s "which oracle passed when").
    """
    oracles = (ORACLES_BY_ID["O3_supersession"], ORACLES_BY_ID["O5_conflicts"])
    not_authorable_reason = (
        "arm reported NOT_RUN (measurement_not_run:"
        "no_source_material_authored_for_this_oracle_yet); an unmeasured "
        "oracle is never a pass"
    )
    reports = {
        "native": TrialReport(
            arm="native",
            results=(
                _pass("O3_supersession", "native"),
                _pass("O5_conflicts", "native"),
            ),
        ),
        "extraction_llm": TrialReport(
            arm="extraction_llm",
            results=(
                _pass("O3_supersession", "extraction_llm"),
                _not_measured("O5_conflicts", "extraction_llm", not_authorable_reason),
            ),
        ),
    }
    call_timestamps = {
        ("O3_supersession", "native"): "2026-08-08T00:00:00+00:00",
        ("O3_supersession", "extraction_llm"): "2026-08-08T00:00:01+00:00",
        ("O5_conflicts", "native"): "2026-08-08T00:00:00+00:00",
        # extraction_llm never actually called O5_conflicts (NOT_RUN before
        # any call) -- deliberately absent from call_timestamps.
    }

    table = _render_per_oracle_table(oracles, reports, call_timestamps)

    assert "O3_supersession" in table
    assert "O5_conflicts" in table
    assert "native" in table
    assert "extraction_llm" in table
    assert "`pass` @ `2026-08-08T00:00:00+00:00`" in table
    assert "`pass` @ `2026-08-08T00:00:01+00:00`" in table
    assert "`not_measured` @ `n/a`" in table, (
        "an oracle never actually called (NOT_RUN before any provider "
        "attempt) must render its missing timestamp honestly as n/a, not "
        "a fabricated instant"
    )
    assert "no_source_material_authored_for_this_oracle_yet" in table, (
        "the NOT_RUN reason must be visible in the table itself, not just "
        "the aggregate -- this is what lets a reader tell 'not authored "
        "yet' apart from 'not authorable at all' apart from 'provider "
        "down' without cross-referencing a second document"
    )


def test_per_oracle_table_distinguishes_not_authorable_from_not_yet_authored() -> None:
    """The other half of the #1603 gap: NOT_RUN has more than one honest
    reason, and the table must keep them visibly distinct, not collapse
    them into one generic NOT_RUN cell.
    """
    oracles = (ORACLES_BY_ID["O1_ci_prior_attempts"],)
    reports = {
        "extraction_llm": TrialReport(
            arm="extraction_llm",
            results=(
                _not_measured(
                    "O1_ci_prior_attempts",
                    "extraction_llm",
                    "arm reported NOT_RUN (measurement_not_run:"
                    "not_authorable_for_extraction_arm:structured_episode_"
                    "data_has_no_natural_prose_form); an unmeasured oracle "
                    "is never a pass",
                ),
            ),
        ),
    }
    table = _render_per_oracle_table(oracles, reports, call_timestamps={})
    assert "not_authorable_for_extraction_arm" in table
    assert "structured_episode_data_has_no_natural_prose_form" in table
