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

These tests execute the REAL ``ci/aggregate_test_results.sh`` -- the file the
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
SCRIPT = ROOT / "ci" / "aggregate_test_results.sh"
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "test.yml"

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
            "EVENT_NAME": event,
            "CHANGES_RESULT": changes,
            "CHANGES_CODE": code,
            "MATRIX_RESULT": matrix,
            "COVERAGE_RESULT": coverage,
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
# Workflow contract. The script models the two jobs' `if:` conditions; these
# assert the workflow still holds up its end.
# --------------------------------------------------------------------------


def _workflow() -> dict[str, object]:
    loaded = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _job(name: str) -> dict[str, object]:
    jobs = _workflow()["jobs"]
    assert isinstance(jobs, dict)
    job = jobs[name]
    assert isinstance(job, dict)
    return job


def _steps(name: str) -> list[object]:
    steps = _job(name)["steps"]
    assert isinstance(steps, list)
    return steps


def _normalize(expression: object) -> str:
    assert isinstance(expression, str)
    return " ".join(expression.split())


def _code_filter_patterns() -> list[str]:
    filter_step = next(
        step
        for step in _steps("changes")
        if isinstance(step, dict) and step.get("id") == "filter"
    )
    with_block = filter_step["with"]
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


def test_paths_filter_covers_every_file_the_test_jobs_install() -> None:
    # Given the aggregator now formally blesses `code == 'false'` skips, the
    # filter's file list is what decides whether a change is untested. A file
    # the test jobs consume but the filter omits is a green required check with
    # zero tests run -- which is what `requirements-docs.txt` was until
    # CHAOS-3482 Codex round 2.
    patterns = _code_filter_patterns()
    installed: set[str] = set()
    for job_name in ("test-matrix", "coverage"):
        for step in _steps(job_name):
            if not isinstance(step, dict):
                continue
            run = step.get("run")
            if not isinstance(run, str):
                continue
            for match in re.finditer(r"pip install -r\s+(\S+)", run):
                installed.add(match.group(1))

    # Then the set is derived from the jobs themselves (not hand-listed here,
    # which would go stale the moment a job installs something new) ...
    assert installed, "no `pip install -r` found -- this guard measured nothing"

    # ... and every one of those files is gated by the filter.
    uncovered = sorted(path for path in installed if not _is_covered(path, patterns))
    assert not uncovered, (
        f"the test jobs install {uncovered}, but the `code` path filter does "
        f"not select on them: a PR touching only those files skips every test "
        f"job and the required gate goes green with nothing run. Filter "
        f"patterns: {patterns}"
    )


def test_aggregator_consumes_the_changes_job() -> None:
    # Given the required gate
    test_job = _job("test")

    # Then it depends on `changes` -- without this the `needs.changes.*`
    # expressions below resolve to empty and every verdict silently degrades to
    # the pre-CHAOS-3482 rule.
    assert test_job["needs"] == ["changes", "test-matrix", "coverage"]

    step = next(
        candidate
        for candidate in _steps("test")
        if isinstance(candidate, dict)
        and candidate.get("name") == "Aggregate test results"
    )
    assert step["env"] == {
        "EVENT_NAME": "${{ github.event_name }}",
        "CHANGES_RESULT": "${{ needs.changes.result }}",
        "CHANGES_CODE": "${{ needs.changes.outputs.code }}",
        "MATRIX_RESULT": "${{ needs.test-matrix.result }}",
        "COVERAGE_RESULT": "${{ needs.coverage.result }}",
    }
    # And it runs the script these tests exercise, rather than an inline copy
    # of the rules that no test would see.
    assert "ci/aggregate_test_results.sh" in _normalize(step["run"])


def test_aggregator_runs_unconditionally() -> None:
    # Given a run that gets cancelled
    # When the gate's own condition is inspected
    # Then it is `always()`: under `!cancelled()` this job would itself be
    # skipped, and branch protection counts a skipped REQUIRED check as
    # satisfied -- relocating the silent pass rather than closing it.
    assert _normalize(_job("test")["if"]) == "always()"


def test_aggregator_checks_out_the_repository() -> None:
    # Given the verdict now lives in a file in the repo
    steps = _steps("test")

    # Then the gate checks the repo out before running it. (A missing checkout
    # fails closed -- the step errors -- but it would fail every run, so this
    # is a contract assertion, not a safety one.)
    assert any(
        isinstance(step, dict)
        and str(step.get("uses", "")).startswith("actions/checkout@")
        for step in steps
    )


def test_script_mirrors_the_test_matrix_selection_condition() -> None:
    # Given the script treats `test-matrix: skipped` as legitimate exactly when
    # the event is not merge_group and code != 'true'
    # Then that is still the job's own condition. If this assertion fails, the
    # script's model is stale: fix ci/aggregate_test_results.sh, do not just
    # update the string.
    assert _normalize(_job("test-matrix")["if"]) == (
        "github.event_name == 'merge_group' || "
        "github.event_name == 'workflow_dispatch' || "
        "needs.changes.outputs.code == 'true'"
    )


def test_path_filter_does_not_run_on_manual_dispatch() -> None:
    # Given a manual run has no base/head diff worth filtering
    filter_step = next(
        step
        for step in _steps("changes")
        if isinstance(step, dict) and step.get("id") == "filter"
    )

    # Then the filter is not consulted there. If it were, its verdict would
    # reach `code`, and `code == 'true'` is one of the ways a job gets
    # selected -- which is how a manual run once skipped both test jobs and
    # still reported a green required check.
    assert _normalize(filter_step["if"]) == "github.event_name != 'workflow_dispatch'"


def test_script_mirrors_the_coverage_selection_condition() -> None:
    # Same contract for coverage, whose pull_request exclusion is the reason
    # `skipped` cannot simply be rejected.
    assert _normalize(_job("coverage")["if"]) == (
        "github.event_name != 'pull_request' && "
        "(github.event_name == 'merge_group' || needs.changes.outputs.code == 'true')"
    )


def test_changes_job_still_publishes_the_code_output() -> None:
    # Given the whole rule keys off `needs.changes.outputs.code`
    changes = _job("changes")
    outputs = changes["outputs"]
    assert isinstance(outputs, dict)

    # Then the job still publishes it. A renamed output would make CHANGES_CODE
    # empty, which the script treats as "not 'false'" -- fail-closed, but this
    # names the cause instead of leaving a confusing red gate.
    assert outputs["code"] == "${{ steps.filter.outputs.code }}"
