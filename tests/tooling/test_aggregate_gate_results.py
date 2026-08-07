"""Contract for the required `test` gate's verdict (CHAOS-3482).

The aggregator in ``.github/workflows/test.yml`` used to read only
``needs.test-matrix.result`` and ``needs.coverage.result`` and accept
``skipped`` from either. On 2026-08-06 (PR #1528, runs during a declared
GitHub Actions outage) the ``changes`` job failed -- and on a rerun, was
cancelled -- in "Set up job". Both test jobs then evaluated their ``if:`` to
false and reported ``skipped``, the ``success|skipped`` arm matched, and the
single REQUIRED check reported SUCCESS on a run in which zero tests ran.
Reproduced 2 of 2 attempts.

Why this needs a table and not one regression test: ``skipped`` is a
legitimate pass in this workflow. ``coverage`` is excluded from every
``pull_request`` by design (CHAOS-2586), and ``test-matrix`` is excluded from
docs-only changes by the path filter. A repair that simply rejects ``skipped``
turns every PR red; one that keeps accepting it leaves the hole open. The
distinguishing information is not in the skipped job's result at all -- it is
in ``changes``: whether the gating decision completed, and what it decided.
The rows below are exactly the pairs that share a literal result string and
must reach opposite verdicts.

These tests execute the REAL ``ci/aggregate_gate_results.sh`` -- the file the
workflow runs, byte for byte -- not a reimplementation of its rules. The
second half parses ``test.yml`` and asserts the ``if:`` conditions that the
script mirrors still say what it models, so the script and the workflow cannot
drift apart while every row here keeps passing.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "aggregate_gate_results.sh"
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "test.yml"
LINT_PATH = ROOT / ".github" / "workflows" / "lint.yml"
TYPECHECK_PATH = ROOT / ".github" / "workflows" / "typecheck.yml"

_PASS = True
_FAIL = False

# (id, event, changes result, changes code, test-matrix result, coverage
# result, expected verdict). "code" is `needs.changes.outputs.code`, which is
# the empty string whenever `changes` did not complete.
_MATRIX: list[tuple[str, str, str, str, str, str, bool]] = [
    # --- the incident, both observed upstream states -------------------------
    ("changes-failure", "pull_request", "failure", "", "skipped", "skipped", _FAIL),
    # The rerun 35 minutes later: `changes` cancelled, both test jobs reporting
    # the same benign-looking `skipped` as a docs-only PR. This is the row the
    # pre-fix rule passed.
    (
        "changes-cancelled-jobs-skipped",
        "pull_request",
        "cancelled",
        "",
        "skipped",
        "skipped",
        _FAIL,
    ),
    # A superseded push, where the cancellation reaches the test jobs too.
    (
        "changes-cancelled-jobs-cancelled",
        "pull_request",
        "cancelled",
        "",
        "cancelled",
        "cancelled",
        _FAIL,
    ),
    # A job that fails at a late step still publishes the outputs its earlier
    # steps set, so `changes: failure` can arrive WITH a usable-looking
    # `code=false`. These two rows exist because a mutation sweep found the
    # `changes`-result guard otherwise unpinned at the verdict level: every
    # other failure row is also caught by the undecided-`code` rule, so
    # deleting the guard entirely changed no verdict. Here it is the only
    # thing standing between a dead gating job and a green gate.
    (
        "changes-failure-with-published-code",
        "pull_request",
        "failure",
        "false",
        "skipped",
        "skipped",
        _FAIL,
    ),
    (
        "changes-cancelled-with-published-code",
        "push",
        "cancelled",
        "false",
        "skipped",
        "skipped",
        _FAIL,
    ),
    # `changes` has no `if:`, so a skip means something stranger still went
    # wrong upstream. Unknown state is not evidence.
    ("changes-skipped", "pull_request", "skipped", "", "skipped", "skipped", _FAIL),
    ("everything-empty", "pull_request", "", "", "", "", _FAIL),
    (
        "changes-unknown-word",
        "pull_request",
        "neutral",
        "",
        "skipped",
        "skipped",
        _FAIL,
    ),
    # --- the legitimate passes that a naive "reject skipped" fix would break --
    ("normal-pr", "pull_request", "success", "true", "success", "skipped", _PASS),
    ("docs-only-pr", "pull_request", "success", "false", "skipped", "skipped", _PASS),
    ("docs-only-push", "push", "success", "false", "skipped", "skipped", _PASS),
    ("main-push", "push", "success", "true", "success", "success", _PASS),
    ("merge-queue", "merge_group", "success", "false", "success", "success", _PASS),
    # A manual run does not run the path filter at all, so `code` is empty and
    # `coverage` is not selected; test-matrix carries the proof.
    ("dispatch", "workflow_dispatch", "success", "", "success", "skipped", _PASS),
    # --- skips that share the literal string but are NOT legitimate ----------
    # Same `test-matrix: skipped` as docs-only-pr, opposite verdict: the path
    # filter selected the job to run.
    (
        "matrix-skipped-but-selected",
        "pull_request",
        "success",
        "true",
        "skipped",
        "skipped",
        _FAIL,
    ),
    # dorny/paths-filter has no base/head diff in the merge queue and can
    # report code=false, so test-matrix runs there unconditionally; a skip
    # means an untested change is entering main.
    (
        "merge-queue-matrix-skipped",
        "merge_group",
        "success",
        "false",
        "skipped",
        "success",
        _FAIL,
    ),
    (
        "merge-queue-coverage-skipped",
        "merge_group",
        "success",
        "true",
        "success",
        "skipped",
        _FAIL,
    ),
    # The merge queue selects coverage through its own clause, independent of
    # `code` -- so a merge-queue coverage skip is illegitimate even when the
    # filter said false. (Added because a mutation that deleted only that
    # clause changed no verdict in an earlier version of this table.)
    (
        "merge-queue-coverage-skipped-code-false",
        "merge_group",
        "success",
        "false",
        "success",
        "skipped",
        _FAIL,
    ),
    # And the same undecided-`code` rule applies to coverage: on a push where
    # the filter published no decision, its skip cannot be attributed to one.
    (
        "push-code-empty-coverage-skipped",
        "push",
        "success",
        "",
        "success",
        "skipped",
        _FAIL,
    ),
    # The hole Codex found in the first version of this fix: a manual
    # `gh workflow run test.yml` whose path filter reported code=false skipped
    # both jobs and the gate went green with zero tests. The filter no longer
    # runs on dispatch (so `code` is empty), and either way a dispatch run that
    # skipped test-matrix is not evidence.
    (
        "dispatch-matrix-skipped",
        "workflow_dispatch",
        "success",
        "",
        "skipped",
        "skipped",
        _FAIL,
    ),
    (
        "dispatch-matrix-skipped-filter-false",
        "workflow_dispatch",
        "success",
        "false",
        "skipped",
        "skipped",
        _FAIL,
    ),
    # Codex round 3: `changes` succeeded but published no usable decision --
    # a renamed output, a filter step made conditional, a filter that produced
    # no key. "Not 'true'" used to read as "docs-only", so an undecided filter
    # bought a green check with nothing run. Only a literal 'false' explains a
    # skip.
    ("pr-code-empty", "pull_request", "success", "", "skipped", "skipped", _FAIL),
    (
        "pr-code-not-a-boolean",
        "pull_request",
        "success",
        "neutral",
        "skipped",
        "skipped",
        _FAIL,
    ),
    ("push-code-empty", "push", "success", "", "skipped", "skipped", _FAIL),
    # ... but an undecided `code` on a run where the jobs DID execute is not a
    # gate failure. Fail-closed must not mean fail-noisy: these two would be
    # red if the rule above were a hard validation of `code` instead of a
    # question about whether a skip can be explained.
    (
        "pr-code-empty-but-matrix-ran",
        "pull_request",
        "success",
        "",
        "success",
        "skipped",
        _PASS,
    ),
    (
        "merge-queue-code-empty",
        "merge_group",
        "success",
        "",
        "success",
        "success",
        _PASS,
    ),
    # Same `coverage: skipped` as normal-pr, opposite verdict: off a PR with
    # gated paths touched, the coverage job was selected.
    (
        "push-coverage-skipped-but-selected",
        "push",
        "success",
        "true",
        "success",
        "skipped",
        _FAIL,
    ),
    # --- downstream failures: current behaviour, must not regress ------------
    ("matrix-failure", "pull_request", "success", "true", "failure", "skipped", _FAIL),
    (
        "matrix-cancelled",
        "pull_request",
        "success",
        "true",
        "cancelled",
        "skipped",
        _FAIL,
    ),
    ("coverage-failure", "push", "success", "true", "success", "failure", _FAIL),
    ("coverage-cancelled", "push", "success", "true", "success", "cancelled", _FAIL),
    (
        "matrix-unknown-word",
        "pull_request",
        "success",
        "true",
        "neutral",
        "skipped",
        _FAIL,
    ),
]


def _run(
    *,
    event: str,
    changes: str,
    code: str,
    matrix: str,
    coverage: str,
    script: Path = SCRIPT,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": "test",
            "EVENT_NAME": event,
            "CHANGES_RESULT": changes,
            "CHANGES_CODE": code,
            "GATED_JOB_1": f"test-matrix|path-filtered|{matrix}",
            "GATED_JOB_2": f"coverage|merge-time-only|{coverage}",
        },
    )


@pytest.mark.parametrize(
    ("event", "changes", "code", "matrix", "coverage", "expected_pass"),
    [pytest.param(*row[1:], id=row[0]) for row in _MATRIX],
)
def test_gate_verdict(
    event: str,
    changes: str,
    code: str,
    matrix: str,
    coverage: str,
    expected_pass: bool,
) -> None:
    # Given one combination of upstream job results the required gate can see
    # When the real aggregation script judges it
    proc = _run(
        event=event, changes=changes, code=code, matrix=matrix, coverage=coverage
    )

    # Then the verdict is the one the check's meaning demands -- asserted on the
    # exit status the required check actually consumes, AND on the printed
    # verdict, so a script that exits 1 from an unrelated `set -e` trip cannot
    # masquerade as this guard firing.
    if expected_pass:
        assert proc.returncode == 0, (
            f"expected the gate to PASS for {event}/{changes}/code={code!r}/"
            f"{matrix}/{coverage}; stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
        assert "test gate passed" in proc.stdout
        assert "test gate failed" not in proc.stderr
    else:
        assert proc.returncode == 1, (
            f"expected the gate to FAIL for {event}/{changes}/code={code!r}/"
            f"{matrix}/{coverage}; stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
        assert "test gate failed" in proc.stderr
        assert "test gate passed" not in proc.stdout


def test_failure_output_names_the_upstream_job_that_caused_it() -> None:
    # Given the incident's exact state
    proc = _run(
        event="pull_request",
        changes="failure",
        code="",
        matrix="skipped",
        coverage="skipped",
    )

    # Then the reason blames `changes`, not the two jobs that merely skipped:
    # a gate that fails for the wrong stated reason sends the next responder to
    # the wrong place.
    assert proc.returncode == 1
    assert "changes reported 'failure'" in proc.stderr


def test_legitimate_skips_are_reported_with_their_reason() -> None:
    # Given a docs-only pull request, where BOTH jobs skip legitimately
    proc = _run(
        event="pull_request",
        changes="success",
        code="false",
        matrix="skipped",
        coverage="skipped",
    )

    # Then the log names why each skip is accepted, so a reader of a green run
    # can check the claim rather than trust it.
    assert proc.returncode == 0
    assert "test-matrix skipped legitimately" in proc.stdout
    assert "coverage skipped legitimately" in proc.stdout


# --------------------------------------------------------------------------
# Single-gated-job gates (lint, typecheck). CHAOS-3513.
#
# These two carried the same `success|skipped` rule as `test` did, and carried
# it while their path filters omitted `.github/workflows/**`. That combination
# is not hypothetical: in merge_group runs 30597898813 / 30597912205 (lint) and
# 30597898822 / 30597912214 (typecheck), on 2026-07-31, the queue's diff held
# exactly one file -- `.github/workflows/governance.yml` -- so the filter
# reported `code=false`, `lint-job` and `typecheck-mypy` skipped, and BOTH
# required checks reported SUCCESS. PR #1349, a change to the CI gates
# themselves, reached main with neither having run.
#
# The rows below are the same shape as the `test` table, for a gate with one
# gated job.
# --------------------------------------------------------------------------

_SINGLE_JOB_MATRIX: list[tuple[str, str, str, str, str, bool]] = [
    # (id, event, changes result, code, job result, expected verdict)
    ("changes-failure", "pull_request", "failure", "", "skipped", _FAIL),
    ("changes-cancelled", "pull_request", "cancelled", "", "skipped", _FAIL),
    ("changes-skipped", "pull_request", "skipped", "", "skipped", _FAIL),
    ("changes-empty", "pull_request", "", "", "", _FAIL),
    # The observed incident, in the shape it actually occurred: a merge_group
    # run whose filter said false. Before CHAOS-3513 this printed
    # "lint gate passed".
    ("merge-queue-skipped", "merge_group", "success", "false", "skipped", _FAIL),
    ("merge-queue-code-empty-skipped", "merge_group", "success", "", "skipped", _FAIL),
    ("merge-queue-ran", "merge_group", "success", "false", "success", _PASS),
    # Legitimate: the filter genuinely decided against it off the queue.
    ("docs-only-pr", "pull_request", "success", "false", "skipped", _PASS),
    ("docs-only-push", "push", "success", "false", "skipped", _PASS),
    ("normal-pr", "pull_request", "success", "true", "success", _PASS),
    ("push-to-main", "push", "success", "true", "success", _PASS),
    # A manual run does not run the filter, so a skip there is never explained.
    ("dispatch-ran", "workflow_dispatch", "success", "", "success", _PASS),
    ("dispatch-skipped", "workflow_dispatch", "success", "", "skipped", _FAIL),
    (
        "dispatch-skipped-filter-false",
        "workflow_dispatch",
        "success",
        "false",
        "skipped",
        _FAIL,
    ),
    # Undecided `code` is not a decision, so it cannot explain a skip.
    ("pr-code-empty-skipped", "pull_request", "success", "", "skipped", _FAIL),
    ("pr-code-garbage-skipped", "pull_request", "success", "neutral", "skipped", _FAIL),
    ("pr-code-empty-but-ran", "pull_request", "success", "", "success", _PASS),
    # Downstream states that were already fatal and must stay fatal.
    ("job-failure", "pull_request", "success", "true", "failure", _FAIL),
    ("job-cancelled", "pull_request", "success", "true", "cancelled", _FAIL),
    ("job-unknown-word", "pull_request", "success", "true", "neutral", _FAIL),
]


def _run_single(
    *, gate: str, event: str, changes: str, code: str, job: str, job_name: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": gate,
            "EVENT_NAME": event,
            "CHANGES_RESULT": changes,
            "CHANGES_CODE": code,
            "GATED_JOB_1": f"{job_name}|path-filtered|{job}",
        },
    )


@pytest.mark.parametrize(
    ("gate", "job_name"), [("lint", "lint-job"), ("typecheck", "typecheck-mypy")]
)
@pytest.mark.parametrize(
    ("event", "changes", "code", "job", "expected_pass"),
    [pytest.param(*row[1:], id=row[0]) for row in _SINGLE_JOB_MATRIX],
)
def test_single_job_gate_verdict(
    gate: str,
    job_name: str,
    event: str,
    changes: str,
    code: str,
    job: str,
    expected_pass: bool,
) -> None:
    proc = _run_single(
        gate=gate, event=event, changes=changes, code=code, job=job, job_name=job_name
    )

    if expected_pass:
        assert proc.returncode == 0, (
            f"expected {gate} to PASS for {event}/{changes}/code={code!r}/{job}; "
            f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
        assert f"{gate} gate passed" in proc.stdout
    else:
        assert proc.returncode == 1, (
            f"expected {gate} to FAIL for {event}/{changes}/code={code!r}/{job}; "
            f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
        assert f"{gate} gate failed" in proc.stderr
        assert f"{gate} gate passed" not in proc.stdout


def test_a_gate_given_nothing_to_judge_fails() -> None:
    # Given a workflow wiring mistake that passes no GATED_JOB_<n> at all
    proc = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": "lint",
            "EVENT_NAME": "pull_request",
            "CHANGES_RESULT": "success",
            "CHANGES_CODE": "true",
        },
    )

    # Then the gate fails rather than reporting a green check for a measurement
    # that never happened -- the failure mode this whole file exists to close,
    # one level further up.
    assert proc.returncode == 1
    assert "asked to judge nothing" in proc.stderr


@pytest.mark.parametrize(
    ("env", "expected_judged", "expected_supplied"),
    [
        pytest.param(
            {
                "GATED_JOB_1": "a|path-filtered|success",
                "GATED_JOB_3": "b|path-filtered|failure",
            },
            1,
            2,
            id="gap-hides-a-failure",
        ),
        pytest.param(
            {
                "GATED_JOB_1": "a|path-filtered|success",
                "GATED_JOB_2": "",
                "GATED_JOB_3": "b|path-filtered|skipped",
            },
            1,
            2,
            id="empty-middle-entry-hides-a-skip",
        ),
    ],
)
def test_a_gap_in_the_job_numbering_fails_the_gate(
    env: dict[str, str], expected_judged: int, expected_supplied: int
) -> None:
    # Given a gate handed non-consecutive GATED_JOB_<n> entries -- one typo in
    # a workflow away, and test.yml already ships two of them
    proc = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": "test",
            "EVENT_NAME": "pull_request",
            "CHANGES_RESULT": "success",
            "CHANGES_CODE": "true",
            **env,
        },
    )

    # Then it refuses. The consecutive loop stops at the first gap, so before
    # this check `GATED_JOB_1=success` + `GATED_JOB_3=failure` printed
    # "gate passed" and exited 0 -- a required check going green while a job's
    # failure was never read (adversarial review 2026-08-06, reproduced). A
    # gate that silently judges a subset is the same defect class as one that
    # judges nothing.
    assert proc.returncode == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert f"judged {expected_judged} of {expected_supplied}" in proc.stderr


def test_an_unrecognized_policy_fails_the_gate() -> None:
    # Given a job whose selection policy this script does not model
    proc = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": "lint",
            "EVENT_NAME": "pull_request",
            "CHANGES_RESULT": "success",
            "CHANGES_CODE": "false",
            "GATED_JOB_1": "lint-job|invented-policy|skipped",
        },
    )

    # Then it refuses rather than guessing. A typo in a policy name must not
    # silently become "this skip was fine".
    assert proc.returncode == 1
    assert "not recognized" in proc.stderr


# --------------------------------------------------------------------------
# The selector-less gate (CHAOS-3219 Phase 5).
#
# .github/workflows/ask-dev-acceptance.yml fires only on `schedule` and
# `workflow_dispatch`, so it has no `changes` job and nothing can explain a
# skip. That is the `unconditional` policy, and it is admitted only together
# with GATE_HAS_SELECTOR=false. The rows below are the pairs that share a
# literal result string and must reach opposite verdicts, plus the two wiring
# mistakes that would let an empty selector result stand in for a decision.
# --------------------------------------------------------------------------


def _run_selectorless(
    *,
    result: str,
    event: str = "schedule",
    policy: str = "unconditional",
    has_selector: str = "false",
    changes_result: str | None = None,
    changes_code: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {
        "PATH": "/usr/bin:/bin:/usr/local/bin",
        "GATE_NAME": "ask-dev-acceptance",
        "EVENT_NAME": event,
        "GATE_HAS_SELECTOR": has_selector,
        "GATED_JOB_1": f"acceptance-live|{policy}|{result}",
    }
    if changes_result is not None:
        env["CHANGES_RESULT"] = changes_result
    if changes_code is not None:
        env["CHANGES_CODE"] = changes_code
    return subprocess.run(
        ["bash", str(SCRIPT)], capture_output=True, text=True, check=False, env=env
    )


@pytest.mark.parametrize(
    ("result", "event", "expected_pass"),
    [
        pytest.param("success", "schedule", _PASS, id="nightly-ran-and-passed"),
        pytest.param(
            "success", "workflow_dispatch", _PASS, id="dispatch-ran-and-passed"
        ),
        # The whole point. On the other three gates a `skipped` job on
        # workflow_dispatch or with an undecided filter can be legitimate; here
        # it never is, because there is no condition that could have deselected
        # it. A nightly that booted nothing is not a nightly that found nothing.
        pytest.param("skipped", "schedule", _FAIL, id="nightly-booted-nothing"),
        pytest.param(
            "skipped", "workflow_dispatch", _FAIL, id="dispatch-booted-nothing"
        ),
        pytest.param("failure", "schedule", _FAIL, id="stack-or-corpus-failed"),
        pytest.param("cancelled", "schedule", _FAIL, id="cancelled-is-unmeasured"),
        pytest.param("", "schedule", _FAIL, id="empty-result-is-not-evidence"),
        pytest.param("neutral", "schedule", _FAIL, id="unknown-result-is-not-evidence"),
    ],
)
def test_selectorless_gate_verdict(
    result: str, event: str, expected_pass: bool
) -> None:
    proc = _run_selectorless(result=result, event=event)

    if expected_pass:
        assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        assert "ask-dev-acceptance gate passed" in proc.stdout
    else:
        assert proc.returncode == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        assert "ask-dev-acceptance gate failed" in proc.stderr
        assert "gate passed" not in proc.stdout
        # And it failed for the RIGHT reason. Asserting only the generic footer
        # let a mutation that deleted the `unconditional` policy arm survive:
        # the row still went red, but through the "unrecognized policy" path,
        # which proves nothing about the policy this gate actually uses.
        expected_reason = (
            "was skipped, but its job condition selected it to run"
            if result == "skipped"
            else f"acceptance-live reported '{result or '<empty>'}'"
        )
        assert expected_reason in proc.stderr


def test_a_selectorless_gate_may_not_also_report_a_selector() -> None:
    # Given a gate that declares it has no `changes` job but passes one's result
    proc = _run_selectorless(
        result="success", changes_result="success", changes_code="true"
    )

    # Then it refuses: one of the two statements is wrong and the script cannot
    # tell which, so it must not act on either.
    assert proc.returncode == 1
    assert "cannot both have no selector job and report one" in proc.stderr


@pytest.mark.parametrize("result", ["success", "skipped", "failure"])
@pytest.mark.parametrize("policy", ["totally-bogus", "uncondit1onal", ""])
def test_an_unrecognized_policy_is_refused_whatever_the_result(
    policy: str, result: str
) -> None:
    # Given a policy name this script does not implement -- a typo, or a name
    # from a workflow that was never taught to this script
    proc = _run_selectorless(result=result, policy=policy)

    # Then it refuses on EVERY result, not only on a skip. Before this,
    # is_selected() ran only on the `skipped` arm, so a mis-wired gate passed
    # green on every run where nothing happened to skip and revealed itself
    # only on the day the verdict mattered. Adversarial review 2026-08-06,
    # reproduced: `acceptance-live|totally-bogus|success` exited 0.
    assert proc.returncode == 1, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "is not recognized" in proc.stderr


def test_the_unconditional_policy_arm_is_load_bearing() -> None:
    # Given the ONE combination this workflow actually ships
    proc = _run_selectorless(result="skipped", policy="unconditional")

    # Then the refusal names the unexplained skip, not an unknown policy.
    # Without this assertion, deleting the `unconditional)` arm outright still
    # turned every skipped row red -- via the "not recognized" arm -- and the
    # rows only checked the generic footer, so the arm was unpinned
    # (adversarial review 2026-08-06, MEDIUM-HIGH, reproduced).
    assert proc.returncode == 1
    assert "was skipped, but its job condition selected it to run" in proc.stderr
    assert "is not recognized" not in proc.stderr


def test_a_filter_policy_without_a_selector_is_refused() -> None:
    # Given a job judged by the path filter, with no filter result to judge by.
    # `path-filtered` reads CHANGES_CODE; unset, it would read as "undecided"
    # and quietly call the job selected -- a verdict reached from an absent
    # measurement rather than a decision.
    proc = _run_selectorless(result="skipped", policy="path-filtered")

    assert proc.returncode == 1
    assert "no filter result to decide it with" in proc.stderr


@pytest.mark.parametrize("value", ["", "yes", "FALSE", "1"])
def test_a_non_literal_selector_declaration_is_refused(value: str) -> None:
    # Given anything but the literal 'true'/'false' -- including the empty
    # string a mistyped `${{ ... }}` expression produces
    proc = _run_selectorless(result="success", has_selector=value)

    # Then the gate fails rather than defaulting to the permissive reading.
    assert proc.returncode == 1
    assert "must be literally 'true' or 'false'" in proc.stderr


def test_the_default_selector_declaration_is_unchanged_for_existing_gates() -> None:
    # Given a caller that predates GATE_HAS_SELECTOR entirely (no such variable
    # in its env), which is every gate in test.yml / lint.yml / typecheck.yml
    proc = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "GATE_NAME": "lint",
            "EVENT_NAME": "pull_request",
            "CHANGES_RESULT": "cancelled",
            "CHANGES_CODE": "",
            "GATED_JOB_1": "lint-job|path-filtered|skipped",
        },
    )

    # Then it still takes the selector path, and the CHAOS-3482 incident row
    # still fails. The new input must not have widened the old gates.
    assert proc.returncode == 1
    assert "the job that decides what must run did not complete" in proc.stderr


# --------------------------------------------------------------------------
# Workflow contract, for all three gates. The script models each job's `if:`
# condition; these assert the workflows still hold up their end.
# --------------------------------------------------------------------------

#: (workflow path, gate job name, step name, ((gated job, policy), ...)).
#: The policies here are the ones the workflows actually pass to the script --
#: asserted against the step's env below, not just described.
_GATES: tuple[tuple[Path, str, str, tuple[tuple[str, str], ...]], ...] = (
    (
        WORKFLOW_PATH,
        "test",
        "Aggregate test results",
        (("test-matrix", "path-filtered"), ("coverage", "merge-time-only")),
    ),
    (LINT_PATH, "lint", "Aggregate lint result", (("lint-job", "path-filtered"),)),
    (
        TYPECHECK_PATH,
        "typecheck",
        "Aggregate typecheck result",
        (("typecheck-mypy", "path-filtered"),),
    ),
)

_GATE_IDS = [gate for _, gate, _, _ in _GATES]

#: Every `path-filtered` job must carry exactly this condition, and the script's
#: model of that policy mirrors it. Drift here means the model is stale: fix
#: ci/aggregate_gate_results.sh, do not just update the string.
_PATH_FILTERED_IF = (
    "github.event_name == 'merge_group' || "
    "github.event_name == 'workflow_dispatch' || "
    "needs.changes.outputs.code == 'true'"
)
_MERGE_TIME_ONLY_IF = (
    "github.event_name != 'pull_request' && "
    "(github.event_name == 'merge_group' || needs.changes.outputs.code == 'true')"
)


def _load(path: Path) -> dict[str, object]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _job_of(path: Path, name: str) -> dict[str, object]:
    jobs = _load(path)["jobs"]
    assert isinstance(jobs, dict)
    job = jobs[name]
    assert isinstance(job, dict)
    return job


def _steps_of(path: Path, name: str) -> list[object]:
    steps = _job_of(path, name)["steps"]
    assert isinstance(steps, list)
    return steps


def _normalize(expression: object) -> str:
    assert isinstance(expression, str)
    return " ".join(expression.split())


def _filter_step(path: Path) -> dict[str, object]:
    step = next(
        candidate
        for candidate in _steps_of(path, "changes")
        if isinstance(candidate, dict) and candidate.get("id") == "filter"
    )
    return step


def _patterns_of(path: Path) -> list[str]:
    with_block = _filter_step(path)["with"]
    assert isinstance(with_block, dict)
    filters = yaml.safe_load(with_block["filters"])
    assert isinstance(filters, dict)
    patterns = filters["code"]
    assert isinstance(patterns, list)
    return [str(pattern) for pattern in patterns]


def _is_covered(path: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if pattern == path:
            return True
        if pattern.endswith("/**") and path.startswith(pattern[: -len("**")]):
            return True
    return False


# Back-compat helpers for the `test`-gate assertions above.
def _job(name: str) -> dict[str, object]:
    return _job_of(WORKFLOW_PATH, name)


def _steps(name: str) -> list[object]:
    return _steps_of(WORKFLOW_PATH, name)


def _code_filter_patterns() -> list[str]:
    return _patterns_of(WORKFLOW_PATH)


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_gate_job_consumes_changes_and_calls_the_shared_script(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    job = _job_of(path, gate)

    # The gate depends on `changes` -- without it every `needs.changes.*`
    # expression resolves to empty and the verdict silently degrades to the
    # pre-CHAOS-3482 rule.
    assert job["needs"] == ["changes", *[name for name, _ in gated]]

    # It runs unconditionally: under `!cancelled()` a cancelled run leaves the
    # REQUIRED check skipped, which branch protection counts as satisfied.
    assert _normalize(job["if"]) == "always()"

    step = next(
        candidate
        for candidate in _steps_of(path, gate)
        if isinstance(candidate, dict) and candidate.get("name") == step_name
    )

    expected_env = {
        "GATE_NAME": gate,
        "EVENT_NAME": "${{ github.event_name }}",
        "CHANGES_RESULT": "${{ needs.changes.result }}",
        "CHANGES_CODE": "${{ needs.changes.outputs.code }}",
    }
    for index, (job_name, policy) in enumerate(gated, start=1):
        expected_env[f"GATED_JOB_{index}"] = (
            f"{job_name}|{policy}|${{{{ needs.{job_name}.result }}}}"
        )
    assert step["env"] == expected_env

    # And it runs the shared script these tests exercise, not an inline copy of
    # the rules that no test would see.
    assert "ci/aggregate_gate_results.sh" in _normalize(step["run"])

    # The script lives in the repo, so the gate has to check it out.
    assert any(
        isinstance(candidate, dict)
        and str(candidate.get("uses", "")).startswith("actions/checkout@")
        for candidate in _steps_of(path, gate)
    )


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_gated_jobs_carry_the_condition_their_policy_models(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    # Given the script decides whether a skip was legitimate from a policy name
    # When each gated job's own `if:` is read
    # Then it matches the condition that policy models. This is the join that
    # keeps the script honest: the policy is not a label, it is a claim about
    # this expression.
    expected = {
        "path-filtered": _PATH_FILTERED_IF,
        "merge-time-only": _MERGE_TIME_ONLY_IF,
    }
    for job_name, policy in gated:
        assert _normalize(_job_of(path, job_name)["if"]) == expected[policy], (
            f"{path.name}:{job_name} no longer matches the '{policy}' policy "
            f"that {gate} passes to ci/aggregate_gate_results.sh"
        )


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_path_filter_is_not_run_on_manual_dispatch(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    # A manual run has no base/head diff worth filtering, and if the filter ran
    # there its verdict would reach `code` -- which is one of the ways a job
    # gets selected. That is how a manual run once skipped every test job and
    # still reported green (CHAOS-3482 Codex round 1).
    assert (
        _normalize(_filter_step(path)["if"])
        == "github.event_name != 'workflow_dispatch'"
    )


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_changes_job_publishes_the_code_output(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    outputs = _job_of(path, "changes")["outputs"]
    assert isinstance(outputs, dict)
    assert outputs["code"] == "${{ steps.filter.outputs.code }}"


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_a_workflow_gates_on_changes_to_itself(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    # CHAOS-3513, the lead defect. A workflow whose own filter does not select
    # on the workflow file can be edited -- weakened, or disabled outright -- by
    # a PR that it then declines to run on, and the required check reports
    # SUCCESS. Observed on 2026-07-31: merge_group runs 30597898813 (lint) and
    # 30597898822 (typecheck) skipped their gated jobs and reported green,
    # because the queue's one-file diff was .github/workflows/governance.yml
    # and neither filter listed `.github/workflows/**`.
    #
    # Derived, not hand-listed: the file asserted on is the workflow being
    # parsed, so a fourth gate added later inherits this guard for free.
    relative = path.relative_to(ROOT).as_posix()
    patterns = _patterns_of(path)
    assert _is_covered(relative, patterns), (
        f"{relative} is not selected by its own `code` filter {patterns}: a PR "
        f"editing only this workflow would skip {gate}'s gated jobs and the "
        f"required '{gate}' check would report success"
    )


@pytest.mark.parametrize(("path", "gate", "step_name", "gated"), _GATES, ids=_GATE_IDS)
def test_paths_filter_covers_every_file_the_gated_jobs_install(
    path: Path, gate: str, step_name: str, gated: tuple[tuple[str, str], ...]
) -> None:
    # A file a gated job installs is part of that job's environment, so a change
    # to it can turn the job red -- and must therefore select the job.
    # (requirements-docs.txt was the entry this found missing in test.yml,
    # CHAOS-3482 Codex round 2.)
    patterns = _patterns_of(path)
    installed: set[str] = set()
    for job_name, _ in gated:
        for step in _steps_of(path, job_name):
            if not isinstance(step, dict):
                continue
            run = step.get("run")
            if not isinstance(run, str):
                continue
            for match in re.finditer(r"pip install -r\s+(\S+)", run):
                installed.add(match.group(1))

    assert installed, (
        f"no `pip install -r` found in {path.name} -- guard measured nothing"
    )
    uncovered = sorted(name for name in installed if not _is_covered(name, patterns))
    assert not uncovered, (
        f"{path.name}'s gated jobs install {uncovered}, but its `code` filter "
        f"does not select on them: a PR touching only those files skips the "
        f"jobs and the required '{gate}' check goes green. Patterns: {patterns}"
    )


def test_paths_filter_covers_the_acceptance_runtime_dependencies() -> None:
    # Given the acceptance suite hashes a fixed set of runtime inputs and
    # asserts on them, a change to any of them can turn the suite red. Only
    # test.yml runs that suite.
    from scripts.acceptance.acceptance_artifact import RUNTIME_DEPENDENCY_PATHS

    patterns = _patterns_of(WORKFLOW_PATH)
    assert RUNTIME_DEPENDENCY_PATHS, "empty inventory -- this guard measured nothing"
    uncovered = sorted(
        name for name in RUNTIME_DEPENDENCY_PATHS if not _is_covered(name, patterns)
    )
    assert not uncovered, (
        f"the acceptance suite depends on {uncovered}, but the `code` path "
        f"filter does not select on them: a PR touching only those files skips "
        f"every test job and the required gate goes green with nothing run."
    )


# --------------------------------------------------------------------------
# Tool scope. CHAOS-3513 Codex round 1.
#
# The filters above decide whether a gate RUNS. These two decide whether a gate
# that ran actually looked at anything -- the same failure dressed differently:
# `ruff check .` over an empty file set exits 0 and reports a green required
# check. Measured on this repo: appending `src/` to .gitignore takes ruff's
# file set from 1045 source files to 0, silently.
# --------------------------------------------------------------------------

#: Config and ignore files that ruff and mypy discover by DEFAULT (from their
#: documented discovery order). None of these exists in the repo today, which
#: is exactly why they are listed: a PR adding one would redefine what the gate
#: checks, and unless the filter selects on it, that PR skips the gate.
_TOOL_SCOPE_INPUTS = (
    ".gitignore",
    ".ignore",
    "ruff.toml",
    ".ruff.toml",
    "mypy.ini",
    ".mypy.ini",
    "setup.cfg",
)


@pytest.mark.parametrize("path", [LINT_PATH, TYPECHECK_PATH], ids=["lint", "typecheck"])
def test_filter_selects_on_the_inputs_that_define_tool_scope(path: Path) -> None:
    patterns = _patterns_of(path)
    uncovered = sorted(
        name for name in _TOOL_SCOPE_INPUTS if not _is_covered(name, patterns)
    )
    assert not uncovered, (
        f"{path.name}'s `code` filter does not select on {uncovered}. These "
        f"change what ruff/mypy look at, so a PR adding or editing one would "
        f"skip this gate and every later run would be measured under the new "
        f"scope. Patterns: {patterns}"
    )


LINT_SCOPE_SCRIPT = ROOT / "ci" / "check_lint_scope.sh"


def _fake_ruff(tmp_path: Path, listed: list[str]) -> Path:
    """A stand-in `ruff` that prints a chosen file set.

    The scope check is exercised through a shim rather than the real binary on
    purpose. ruff is installed by lint.yml alone (`pip install ruff`; it is in
    no requirements file), so a test that needed the real ruff would SKIP in
    the CI job that runs this suite -- silently, while reading as coverage.
    That is the failure this whole ticket is about, and the first version of
    this test committed it (CHAOS-3513 Codex round 2).
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    shim = bin_dir / "ruff"
    body = "\n".join(listed)
    shim.write_text(f"#!/usr/bin/env bash\ncat <<'FILES'\n{body}\nFILES\n")
    shim.chmod(0o755)
    return bin_dir


def _run_scope(
    tmp_path: Path, listed: list[str] | None
) -> subprocess.CompletedProcess[str]:
    env = {"PATH": "/usr/bin:/bin", "LINT_SCOPE_MIN_FILES": "5"}
    if listed is not None:
        env["PATH"] = f"{_fake_ruff(tmp_path, listed)}:{env['PATH']}"
    return subprocess.run(
        ["bash", str(LINT_SCOPE_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


_HEALTHY = [f"/repo/src/mod_{index}.py" for index in range(6)] + [
    f"/repo/tests/test_{index}.py" for index in range(6)
]


def test_lint_scope_check_accepts_a_healthy_tree(tmp_path: Path) -> None:
    proc = _run_scope(tmp_path, _HEALTHY)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "check_lint_scope OK" in proc.stdout


@pytest.mark.parametrize(
    ("case", "listed", "expected"),
    [
        # The measured defect: `src/` in .gitignore. Note the file COUNT stays
        # healthy -- only the src/ membership assertion catches this, which is
        # why the floor alone would not have been enough.
        (
            "src-swallowed",
            [f"/repo/tests/test_{i}.py" for i in range(20)],
            "no files under src/",
        ),
        (
            "tests-swallowed",
            [f"/repo/src/mod_{i}.py" for i in range(20)],
            "no files under tests/",
        ),
        (
            "almost-everything-gone",
            ["/repo/src/a.py", "/repo/tests/b.py"],
            "below the floor",
        ),
    ],
)
def test_lint_scope_check_rejects_a_collapsed_file_set(
    tmp_path: Path, case: str, listed: list[str], expected: str
) -> None:
    proc = _run_scope(tmp_path, listed)
    assert proc.returncode == 1, f"{case}: {proc.stdout + proc.stderr}"
    assert expected in proc.stderr


def test_lint_scope_check_fails_when_ruff_is_absent(tmp_path: Path) -> None:
    # Given ruff cannot be found at all
    proc = _run_scope(tmp_path, None)

    # Then the check FAILS rather than passing or skipping. A scope that was
    # never measured is not a measured scope, and this runs in the job whose
    # exit status is the lint gate.
    assert proc.returncode == 1
    assert "not on PATH" in proc.stderr


def test_lint_workflow_measures_scope_before_it_lints() -> None:
    # Given the authoritative measurement lives in the lint job (that is where
    # ruff exists), this is what makes the shim tests above coverage rather
    # than decoration: it pins the real invocation, unconditionally.
    steps = _steps_of(LINT_PATH, "lint-job")
    names = [str(step.get("name", "")) for step in steps if isinstance(step, dict)]
    runs = {
        str(step.get("name", "")): str(step.get("run", ""))
        for step in steps
        if isinstance(step, dict)
    }

    assert "Check lint scope" in names, (
        "lint.yml no longer measures ruff's file set; a swallowed tree would "
        "make `ruff check .` pass over nothing and the gate report green"
    )
    assert "ci/check_lint_scope.sh" in runs["Check lint scope"]

    # And it runs BEFORE the steps whose exit status is the gate -- after them
    # it would still report, but the gate would already have been decided.
    assert names.index("Check lint scope") < names.index("Run ruff (format check)")
    assert names.index("Check lint scope") < names.index("Run ruff (linting)")


def test_mypy_configuration_comes_from_the_file_the_filter_gates() -> None:
    # Given mypy discovers mypy.ini, .mypy.ini and setup.cfg BEFORE pyproject
    competing = [
        name
        for name in ("mypy.ini", ".mypy.ini", "setup.cfg")
        if (ROOT / name).exists()
    ]

    # Then none of them exists, so the settings the typecheck gate enforces are
    # the ones in pyproject.toml -- which the filter does select on. If this
    # fails, a config file was added that outranks pyproject: either delete it
    # or pin the job with `mypy --config-file pyproject.toml`.
    assert not competing, (
        f"{competing} outrank pyproject.toml in mypy's discovery order, so the "
        f"typecheck gate is no longer enforcing the settings its filter gates"
    )
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[tool.mypy]" in pyproject, "mypy settings are not where this test believes"
