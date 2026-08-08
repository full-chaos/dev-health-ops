#!/usr/bin/env python3
"""CHAOS-3567 fault-injection proof: reproducible, runnable evidence that the
``SourceClass.TEMPORAL_CONTEXT`` flag-off stub is genuinely inert, and that
``tests/api/dev/test_chaos_3567_temporal_context_source_class_stub.py``
actually detects the two ways it could stop being inert.

Run with::

    .venv/bin/python scripts/verify_chaos_3567_temporal_context_inertness.py

Committed in-repo (not a one-off manual proof that only lives in a PR
description) so the claim it makes stays auditable from the tree and
re-runnable by anyone, including future reviewers of the post-ADR
implementation issue.

Every plant below runs the guard test module in a FRESH subprocess, because
the totality checks it exercises (``wave_3_1_plans.py``'s CHAOS-3337 guard,
``relationship_matrix.py``'s two completeness checks) are module-level code
that only runs ONCE per interpreter, on first import -- exactly the "cold
process start" shape a real CI run or server boot has. In-process monkeypatch
after those modules are already imported would silently miss the exact class
of defect these guards exist to catch. No on-disk file is ever mutated by
this script; every plant is applied via a small in-process patch inside the
subprocess's own interpreter and is gone the moment that subprocess exits.

Four runs:

1. Baseline (no plant) -- every guard test must pass.
2. Plant 1 (single-file): widen ``data_health_service.NATIVE_EVIDENCE_
   SOURCES`` to include ``"temporal_context"``. Expected: the guard suite's
   ``test_native_evidence_sources_is_exactly_pinned_to_eight_members`` fails
   on its own merits (a clean, isolated ``AssertionError``); nothing else in
   the module is affected.
3. Plant 2a (single-file, the review's "does this alone reach the named
   assertion?" question): widen ONE real registered plan's
   ``source_requirements`` to include ``SourceClass.TEMPORAL_CONTEXT``,
   leaving ``persistence.service._SOURCE_CLASSES`` untouched. Expected:
   the guard module never reaches collection at all -- a DIFFERENT,
   pre-existing guard (``wave_3_1_plans.py``'s own CHAOS-3337 import-time
   totality check) raises first, naming the exact injected class. This is a
   real, valid, even-earlier RED signal (defense in depth), and it is
   NOT the guard suite's own
   ``test_temporal_context_is_not_referenced_by_any_registered_plan``
   failing on its own assertion -- this script proves that distinction
   rather than asserting it in prose.
4. Plant 2b (two-file, the actual reproduction of the named assertion
   failing): widen BOTH the plan's ``source_requirements`` AND
   ``persistence.service._SOURCE_CLASSES`` together. Expected: the
   CHAOS-3337 guard no longer fires (the allowlist now accepts the class),
   the module imports cleanly, and
   ``test_temporal_context_is_not_referenced_by_any_registered_plan`` THEN
   fails on its own assertion.

Exit code is 0 only if every run's actual outcome matches its expectation.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD_TEST_PATH = "tests/api/dev/test_chaos_3567_temporal_context_source_class_stub.py"

_RUN_GUARD_SUITE = f"""
import pytest
raise SystemExit(pytest.main(["{GUARD_TEST_PATH}", "-q"]))
"""

_PLANT_NATIVE_EVIDENCE_SOURCES = f"""
import dev_health_ops.api.dev.data_health_service as dhs
dhs.NATIVE_EVIDENCE_SOURCES = dhs.NATIVE_EVIDENCE_SOURCES + ("temporal_context",)
{_RUN_GUARD_SUITE}
"""

_WIDEN_PLAN_SOURCE_REQUIREMENTS = """
import dev_health_ops.api.dev.investigation_plans.plan_documents as pd
from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID, SourceClass
from dev_health_ops.api.dev.contracts_v2.plan import DevSourceRequirement

_poisoned_requirement = DevSourceRequirement(
    schema_version="dev_source_requirement.v1",
    source_class=SourceClass.TEMPORAL_CONTEXT,
    adapter_id="chaos_3567_fault_injection.scratch.v1",
    requirement_level="optional",
    freshness_policy="unversioned",
    minimum_usable_facts=0,
)
_trust_plan = pd.CORE_PLANS_BY_INTENT[QuestionIntentID.DATA_TRUST]
pd.CORE_PLANS_BY_INTENT[QuestionIntentID.DATA_TRUST] = _trust_plan.model_copy(
    update={
        "source_requirements": _trust_plan.source_requirements
        + (_poisoned_requirement,)
    }
)
"""

_PLANT_PLAN_ONLY = f"""
{_WIDEN_PLAN_SOURCE_REQUIREMENTS}
# First import of wave_3_1_plans triggers its own CHAOS-3337 totality check
# against the now-poisoned CORE_PLANS_BY_INTENT -- the persistence allowlist
# was NOT widened, so this import-time RuntimeError is expected to fire
# before pytest even collects the guard suite.
import dev_health_ops.api.dev.investigation_plans.wave_3_1_plans  # noqa: F401
{_RUN_GUARD_SUITE}
"""

_PLANT_PLAN_AND_ALLOWLIST = f"""
import dev_health_ops.api.dev.persistence.service as persistence_service
persistence_service._SOURCE_CLASSES = persistence_service._SOURCE_CLASSES | {{
    "temporal_context"
}}
{_WIDEN_PLAN_SOURCE_REQUIREMENTS}
# With both tables widened together, this import must succeed cleanly.
import dev_health_ops.api.dev.investigation_plans.wave_3_1_plans  # noqa: F401
{_RUN_GUARD_SUITE}
"""

# (label, snippet, expect_pytest_success, expect_runtime_error_before_pytest,
#  expected_failed_test, expected_marker)
#
# ``expected_failed_test`` is the bare test function name that must appear in
# a ``FAILED <path>::<name>`` line in stdout (None when pytest is expected to
# either fully pass or never run at all). ``expected_marker`` is an
# additional required substring: the exact pytest summary line fragment
# (e.g. "5 passed" / "1 failed, 4 passed") for the two pytest-executed
# shapes, or the named guard's own error text for the import-error shape.
_RUNS: tuple[tuple[str, str, bool, bool, str | None, str], ...] = (
    ("baseline (no plant)", _RUN_GUARD_SUITE, True, False, None, "5 passed"),
    (
        "plant 1: NATIVE_EVIDENCE_SOURCES widened alone",
        _PLANT_NATIVE_EVIDENCE_SOURCES,
        False,
        False,
        "test_native_evidence_sources_is_exactly_pinned_to_eight_members",
        "1 failed, 4 passed",
    ),
    (
        "plant 2a: plan source_requirements widened alone "
        "(persistence allowlist untouched)",
        _PLANT_PLAN_ONLY,
        False,
        True,
        None,
        "CHAOS-3337",
    ),
    (
        "plant 2b: plan source_requirements + persistence allowlist widened together",
        _PLANT_PLAN_AND_ALLOWLIST,
        False,
        False,
        "test_temporal_context_is_not_referenced_by_any_registered_plan",
        "1 failed, 4 passed",
    ),
)


def _run(
    label: str,
    snippet: str,
    expect_pytest_success: bool,
    expect_import_error: bool,
    expected_failed_test: str | None,
    expected_marker: str,
) -> bool:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    pytest_ran = "short test summary" in result.stdout or " passed" in result.stdout
    import_error_fired = "RuntimeError" in result.stderr and not pytest_ran
    outcome_ok = (result.returncode == 0) == expect_pytest_success
    shape_ok = import_error_fired == expect_import_error

    # N1 fix (independent fix-verify review, 2026-08-07): the checks above
    # are exit-code/shape only. A plant whose body dies before pytest ever
    # runs -- e.g. a bare ``raise SystemExit(<n>)`` -- can still produce an
    # exit code and "no RuntimeError in stderr" shape that accidentally
    # match a failing-pytest expectation, certifying coverage that never
    # actually happened. Every run's *content* must additionally prove
    # pytest reached and failed the SPECIFIC named test (not merely "some"
    # failure), or -- for the collection-error shape -- that the SPECIFIC
    # named guard (CHAOS-3337) fired, not just any RuntimeError anywhere in
    # the import chain.
    if expect_import_error:
        content_ok = (
            expected_marker in result.stderr and "RuntimeError" in result.stderr
        )
    elif expected_failed_test is not None:
        failed_line = f"FAILED {GUARD_TEST_PATH}::{expected_failed_test}"
        content_ok = failed_line in result.stdout and expected_marker in result.stdout
    else:
        content_ok = expected_marker in result.stdout and "FAILED " not in result.stdout

    print(f"=== {label} ===")
    print(
        f"exit={result.returncode} pytest_ran={pytest_ran} "
        f"import_error_before_pytest={import_error_fired} content_ok={content_ok}"
    )
    tail_stdout = result.stdout.strip().splitlines()[-8:]
    for line in tail_stdout:
        print(f"  stdout| {line}")
    if import_error_fired or result.returncode not in (0, 1) or not content_ok:
        tail_stderr = result.stderr.strip().splitlines()[-6:]
        for line in tail_stderr:
            print(f"  stderr| {line}")
    print()

    if not outcome_ok:
        print(
            f"UNEXPECTED: {label} expected pytest success={expect_pytest_success}, "
            f"got exit code {result.returncode}"
        )
    if not shape_ok:
        print(
            f"UNEXPECTED: {label} expected import_error_before_pytest="
            f"{expect_import_error}, got {import_error_fired}"
        )
    if not content_ok:
        print(
            f"UNEXPECTED: {label} expected content marker {expected_marker!r} "
            f"(failed_test={expected_failed_test!r}) not found in captured output "
            "-- exit code/shape matched but the specific evidence did not, which "
            "means this run's coverage claim cannot be certified"
        )
    return outcome_ok and shape_ok and content_ok


def main() -> int:
    ok = True
    for (
        label,
        snippet,
        expect_pytest_success,
        expect_import_error,
        expected_failed_test,
        expected_marker,
    ) in _RUNS:
        ok = (
            _run(
                label,
                snippet,
                expect_pytest_success,
                expect_import_error,
                expected_failed_test,
                expected_marker,
            )
            and ok
        )
    if ok:
        print(
            "ALL RUNS MATCHED EXPECTATIONS -- the guard suite's coverage claims hold."
        )
    else:
        print("AT LEAST ONE RUN DID NOT MATCH -- see UNEXPECTED lines above.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
