"""CHAOS-3618: the guard harness must not credit a kill it did not earn.

``scripts/verify_chaos_3618_baseline_honesty_guards.py`` is the artefact
every "N guards observed failing" claim in this epic rests on. It is
therefore the artefact most worth attacking, and the independent verifier
did: it demonstrated an UNEARNED KILL being credited, because
``expected_failure`` was matched against the whole pytest output and pytest
echoes source. A ``GUARD`` token could be credited from a passing assertion
above the real failure, from a docstring, or from a comment — in the harness
whose entire purpose is refusing exactly that.

These tests pin the fix. They are deliberately about the MEASURING
instrument rather than about the code it measures: a harness that cannot be
trusted turns every green run downstream of it into an unsupported claim.
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType

import pytest

_HARNESS_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "scripts"
    / "verify_chaos_3618_baseline_honesty_guards.py"
)


def _harness() -> ModuleType:
    spec = importlib.util.spec_from_file_location("chaos_3618_guards", _HARNESS_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


#: One realistic pytest failure, with the guard token appearing THREE times:
#: in an echoed docstring, in an echoed passing assertion, and finally in the
#: assertion that actually failed. Only the last one is evidence.
_PYTEST_OUTPUT = '''
=================================== FAILURES ===================================
______________________________ test_something ________________________________

    def test_something() -> None:
        """GUARD token_in_a_docstring is what this test is about."""

        assert True, "GUARD token_in_a_passing_assertion"
>       assert False, "GUARD token_that_actually_failed"
E       AssertionError: GUARD token_that_actually_failed
E       assert False

tests/example.py:7: AssertionError
=========================== short test summary info ============================
FAILED tests/example.py::test_something - AssertionError: GUARD token_that_act
'''


def test_an_echoed_docstring_is_not_evidence_of_a_kill() -> None:
    """The exact shape the verifier exploited.

    Proven end-to-end against the real harness too: with
    ``expected_failure`` set to a phrase that appears only in the failing
    test's DOCSTRING, matching the whole output CREDITED the case and
    matching the failure region REJECTED it.
    """

    region = _harness().failure_region(_PYTEST_OUTPUT)
    assert "GUARD token_in_a_docstring" in _PYTEST_OUTPUT
    assert "GUARD token_in_a_docstring" not in region, (
        "GUARD echoed_source_is_not_a_failure"
    )


def test_an_echoed_passing_assertion_is_not_evidence_of_a_kill() -> None:
    """Sharper than the docstring case, and likelier in practice.

    A test with several guard-marked assertions echoes the ones that PASSED
    alongside the one that failed. Crediting those means a case can be
    marked load-bearing while the assertion it names is still green.
    """

    region = _harness().failure_region(_PYTEST_OUTPUT)
    assert "GUARD token_in_a_passing_assertion" in _PYTEST_OUTPUT
    assert "GUARD token_in_a_passing_assertion" not in region, (
        "GUARD an_echoed_passing_assertion_is_not_a_failure"
    )


def test_the_assertion_that_actually_failed_is_evidence() -> None:
    """The positive control, without which the two tests above are satisfied
    by a ``failure_region`` that returns the empty string."""

    region = _harness().failure_region(_PYTEST_OUTPUT)
    assert "GUARD token_that_actually_failed" in region, (
        "GUARD the_real_failure_is_still_credited"
    )


def test_the_short_summary_line_counts_as_the_failure_region() -> None:
    """Some failures state their reason only on the summary line."""

    region = _harness().failure_region(
        "FAILED tests/example.py::test_x - RuntimeError: the arm blew up\n"
    )
    assert "RuntimeError: the arm blew up" in region


def test_a_run_with_no_failure_has_no_failure_region() -> None:
    """An empty region must not silently satisfy a token check.

    ``"" in anything`` is True, so a region check over an empty string would
    credit every case whose expected token happened to be empty. Asserting
    the region is empty here keeps that from ever reading as a pass.
    """

    assert _harness().failure_region("2 passed in 0.10s\n") == ""


@pytest.mark.parametrize(
    "case_field", ["expected_failure", "forbidden_failure", "plant", "test"]
)
def test_every_case_declares_the_fields_the_verdict_depends_on(
    case_field: str,
) -> None:
    """No case may be missing a field the verdict is computed from.

    ``forbidden_failure`` may be empty — that is a judgement, not an
    omission — but the attribute must exist on every case, because the
    harness reads it unconditionally.
    """

    harness = _harness()
    for case in harness.CASES:
        assert hasattr(case, case_field), f"{case.case_id} lacks {case_field}"
    assert len(harness.CASES) == len({case.case_id for case in harness.CASES}), (
        "case ids must be unique or --only silently runs the wrong one"
    )
