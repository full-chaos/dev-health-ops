"""Unit coverage for ``scripts.acceptance.corpus.run_report`` (CHAOS-3462 B1).

The second half of the B1 fix: even with the arming variable surviving the
scrub, a corpus run can still report ``exit 0`` while having executed
nothing (every case skipped for some OTHER reason, a collection filter that
matched no ids, a ``-k`` typo). The arming guard cannot see that -- it runs
once, at session start, and has no idea how many cases eventually ran.

So the launcher asserts it from the outside, off the run's own JUnit XML:
an ARMED run must have executed a non-zero number of cases. This module
holds the pure, unit-testable half of that assertion.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.acceptance.corpus.run_report import (
    ArmedRunNotExecutedError,
    RunSummary,
    UnmeasurableRunError,
    assert_armed_run_executed,
    parse_junit_xml,
    read_junit_report,
)


def _junit(*, tests: int, failures: int = 0, errors: int = 0, skipped: int = 0) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<testsuites>"
        f'<testsuite name="pytest" errors="{errors}" failures="{failures}" '
        f'skipped="{skipped}" tests="{tests}" time="1.0"></testsuite>'
        "</testsuites>"
    )


class TestParseJunitXml:
    def test_reads_the_four_counters(self) -> None:
        summary = parse_junit_xml(_junit(tests=144, failures=2, errors=1, skipped=10))
        assert summary == RunSummary(tests=144, failures=2, errors=1, skipped=10)

    def test_sums_multiple_suites(self) -> None:
        xml = (
            "<testsuites>"
            '<testsuite errors="0" failures="1" skipped="2" tests="10"/>'
            '<testsuite errors="1" failures="0" skipped="3" tests="20"/>'
            "</testsuites>"
        )
        assert parse_junit_xml(xml) == RunSummary(
            tests=30, failures=1, errors=1, skipped=5
        )

    def test_accepts_a_bare_testsuite_root(self) -> None:
        xml = '<testsuite errors="0" failures="0" skipped="1" tests="1"/>'
        assert parse_junit_xml(xml) == RunSummary(
            tests=1, failures=0, errors=0, skipped=1
        )

    def test_executed_excludes_skips(self) -> None:
        assert RunSummary(tests=144, failures=0, errors=0, skipped=144).executed == 0
        assert RunSummary(tests=144, failures=0, errors=0, skipped=1).executed == 143

    @pytest.mark.parametrize(
        "payload",
        ["", "not xml at all", "<testsuites>", "<other/>"],
        ids=["empty", "garbage", "truncated", "wrong-root"],
    )
    def test_unparseable_input_fails_loud(self, payload: str) -> None:
        """Rule 4: a measurement that did not happen must FAIL, loudly. A
        parser that returned a zeroed summary here would let a crashed
        pytest (which writes no usable XML) read as "0 failures"."""

        with pytest.raises(UnmeasurableRunError):
            parse_junit_xml(payload)

    def test_missing_counter_attributes_fail_loud(self) -> None:
        with pytest.raises(UnmeasurableRunError):
            parse_junit_xml('<testsuite tests="5"/>')


class TestReadJunitReport:
    def test_reads_a_real_file(self, tmp_path: Path) -> None:
        path = tmp_path / "report.xml"
        path.write_text(_junit(tests=3, skipped=1), encoding="utf-8")
        assert read_junit_report(path).executed == 2

    def test_absent_file_fails_loud(self, tmp_path: Path) -> None:
        """The single most likely real-world shape of "the measurement did
        not happen": pytest died before writing the report at all."""

        with pytest.raises(UnmeasurableRunError, match="does not exist"):
            read_junit_report(tmp_path / "never-written.xml")

    def test_empty_file_fails_loud(self, tmp_path: Path) -> None:
        path = tmp_path / "report.xml"
        path.write_text("", encoding="utf-8")
        with pytest.raises(UnmeasurableRunError):
            read_junit_report(path)


class TestAssertArmedRunExecuted:
    def test_a_run_that_executed_cases_passes(self) -> None:
        assert_armed_run_executed(
            RunSummary(tests=144, failures=0, errors=0, skipped=0)
        )

    def test_the_exit_evidence_shape_fails(self) -> None:
        """The literal Phase 2 exit run: 144 collected, 144 skipped, exit 0."""

        with pytest.raises(ArmedRunNotExecutedError, match="144"):
            assert_armed_run_executed(
                RunSummary(tests=144, failures=0, errors=0, skipped=144)
            )

    def test_zero_collected_fails(self) -> None:
        with pytest.raises(ArmedRunNotExecutedError):
            assert_armed_run_executed(
                RunSummary(tests=0, failures=0, errors=0, skipped=0)
            )

    def test_a_single_skipped_case_is_not_enough_to_hide_the_rest(self) -> None:
        """Partial skipping is not this check's business -- it only draws
        the "did ANYTHING execute" line. A run with 143 executed and 1
        skipped is a legitimate, separately-reviewable result."""

        assert_armed_run_executed(
            RunSummary(tests=144, failures=0, errors=0, skipped=1)
        )

    def test_a_failing_run_still_counts_as_executed(self) -> None:
        """This check is about execution, not about passing -- a red run is
        already loud on its own, and re-flagging it here would blur the two
        signals."""

        assert_armed_run_executed(
            RunSummary(tests=10, failures=10, errors=0, skipped=0)
        )

    def test_errors_count_as_executed(self) -> None:
        assert_armed_run_executed(
            RunSummary(tests=10, failures=0, errors=10, skipped=0)
        )

    def test_min_executed_floor_is_enforceable(self) -> None:
        with pytest.raises(ArmedRunNotExecutedError, match="93"):
            assert_armed_run_executed(
                RunSummary(tests=94, failures=0, errors=0, skipped=50),
                min_executed=93,
            )

    def test_the_message_names_what_to_check(self) -> None:
        with pytest.raises(ArmedRunNotExecutedError, match="DEV_HEALTH_TEST_ENV_ALLOW"):
            assert_armed_run_executed(
                RunSummary(tests=144, failures=0, errors=0, skipped=144)
            )
