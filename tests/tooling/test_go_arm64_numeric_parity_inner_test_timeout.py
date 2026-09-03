"""go.yml's arm64-numeric-parity `go test` invocations must carry their own
`-timeout`, not rely solely on the enclosing job's `timeout-minutes`.

WHY THIS TEST EXISTS
---------------------
CHAOS-4906, #2145 (team-lead ruling, 09-03, from CF's acr-pool measurement):
acr's own self-hosted sibling job has `timeout-minutes: 10` and NO inner
`go test -timeout`. A real run against the pool measured 9m58s-10m03s wall
time -- close enough to that 10-minute job cap that ordinary variance turned
an actually-passing run into a job CANCELLED by its own timeout, which this
contract's fallback leg then reads as `claimed-failure`: a red required
check for a test that was never broken, just slow to report through two
layers of process (checkout, setup-go, the self-hosted pod itself) before
the test binary even starts.

The fix pattern (the "double bound"): an INNER `go test -timeout` shorter
than the OUTER job's `timeout-minutes` fires first, on its own terms, with a
goroutine dump -- an actionable signal distinct from an ambiguous job
cancellation the poll logic has to interpret. `go.yml`'s two arm64-numeric-
parity `go test` invocations (the self-hosted attempt job, and the fallback
job's own real-work path when the switch is off or the attempt is
unclaimed) run the IDENTICAL command by construction (both mirror the same
job before this PR split it into two legs) -- both need the same inner
bound, or one of them silently keeps relying on the outer cap alone.

WHAT THIS ASSERTS
-----------------
Both `go test` invocations in go.yml's arm64-numeric-parity jobs carry an
explicit `-timeout` shorter than EITHER job's own `timeout-minutes` (10 for
the self-hosted attempt, 20 for the fallback) -- proving the inner bound is
load-bearing (fires first) rather than a number picked without checking it
actually sits inside both outer caps.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"

# (job name, step id or step name substring) -- the step id differs from the
# F2 guard's target step ("wait"); this one has no explicit `id:`, so it is
# found by its `name:`, same technique _filter_step-style helpers elsewhere
# in tests/tooling use for a step with no id.
_TARGET_JOBS = [
    "go-arm64-numeric-parity-self-hosted",
    "go-arm64-numeric-parity-fallback",
]
_STEP_NAME = "Run FMA bit-pattern goldens on real arm64"
_GO_TEST_TIMEOUT = re.compile(r"go test\b[^\n]*?-timeout[ =](\S+)")


_ENV_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")


def _strip_line_continuation(line: str) -> str:
    """Drop a trailing shell line-continuation backslash (an ODD number of
    trailing backslashes means the last one escapes the newline; an EVEN
    number means they escape each other and the line ends "clean") so
    `shlex.split` doesn't raise on a real, multi-line "go test ... \\" /
    "-run ... \\" / "| tee ..." invocation -- this repo's own go.yml wraps
    its go test line exactly this way. `shlex.split` in posix mode treats a
    trailing lone backslash as "escape the next character", and there is
    none, which is a ValueError, not a token -- silently returning None for
    every real multi-line invocation would be worse than stripping the
    continuation marker for THIS line's own word-position check (the words
    that matter -- `go`, `test`, an env-assignment prefix -- are never split
    across the continuation)."""
    stripped = line.rstrip()
    trailing_backslashes = len(stripped) - len(stripped.rstrip("\\"))
    if trailing_backslashes % 2 == 1:
        return stripped[:-1]
    return stripped


def _leading_command_words(line: str, count: int) -> list[str] | None:
    """The first `count` REAL command words on `line` (skipping leading
    `KEY=value` env-var assignments), or None if the line does not
    tokenize as shell (a comment, or something the shim doesn't need to
    understand) or has fewer than `count` real words. Shared reasoning
    with the F2 same-run guard's identical helper -- see
    test_go_arm64_runner_fallback_same_run_lookup.py.
    """
    try:
        tokens = shlex.split(_strip_line_continuation(line), comments=True)
    except ValueError:
        return None
    while tokens and _ENV_ASSIGNMENT.match(tokens[0]):
        tokens = tokens[1:]
    if len(tokens) < count:
        return None
    return tokens[:count]


def _the_go_test_line(script: str, job_name: str) -> str:
    """The one physical line that actually INVOKES `go test`.

    codex round 1 (#2145, CHAOS-4906): the ORIGINAL version of this test
    searched the WHOLE step script for `-timeout` after any `go test`
    substring -- which a COMMENT mentioning the required form (e.g. `#
    required form: go test -timeout 8m`) satisfies just as well as the real
    invocation. Reproduced: a script whose only ACTUAL `go test` command has
    no `-timeout` at all, preceded by a comment line quoting the required
    form, made the old regex match the comment and report a value that was
    never really there. Round 1's fix (line stripped of leading whitespace
    must literally start with the text "go test") was ITSELF found wrong in
    round 2: an env-var-prefixed real invocation (`CI_STAGE=arm64 go test
    ...`) does not start with "go test" and was MISSED, while a dead `if
    false; then` branch's own `go test -timeout 8m ...` line (a physical
    line that, on its own, does start with "go test") was WRONGLY accepted
    as "the" real invocation -- the exact bug this helper exists to prevent,
    one level in. Fixed by tokenizing with `shlex.split` and checking
    COMMAND POSITION (word 0 literally `go`, word 1 literally `test`, after
    stripping any leading env-var assignment) instead of a raw-text prefix
    check. This also fixes what happens when BOTH a dead branch's `go test`
    line and a real env-prefixed one are present: both now tokenize as
    candidates, so the existing "assert exactly 1 match" below reports an
    honest ambiguity instead of silently picking the wrong one -- a safe
    loud failure, not a silent misdetection.
    """
    matches = [
        line
        for line in script.splitlines()
        if _leading_command_words(line, 2) == ["go", "test"]
    ]
    assert len(matches) == 1, (
        f"{WORKFLOW_PATH.name}: job {job_name!r} step {_STEP_NAME!r} has "
        f"{len(matches)} lines that are themselves a `go test` invocation "
        "(command-position match, env-assignment prefix stripped), expected "
        "exactly 1 -- this guard scopes its checks to THE line that "
        "actually runs the test; update it (and this test) if the step now "
        "runs more than one, or disambiguate a dead-branch decoy"
    )
    return matches[0]


def _job(name: str) -> dict[str, object]:
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}
    job = (document.get("jobs") or {}).get(name)
    assert job is not None, (
        f"{WORKFLOW_PATH.name}: job {name!r} not found -- this test's target "
        "job was renamed or removed; update _TARGET_JOBS above rather than "
        "letting this assertion go stale"
    )
    return job


def _go_test_step_script(job_name: str) -> str:
    job = _job(job_name)
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list), (
        f"{WORKFLOW_PATH.name}: job {job_name!r}'s `steps` is not a list"
    )
    matches = [
        s for s in steps_raw if isinstance(s, dict) and s.get("name") == _STEP_NAME
    ]
    assert len(matches) == 1, (
        f"{WORKFLOW_PATH.name}: job {job_name!r} must have exactly one step "
        f"named {_STEP_NAME!r} (found {len(matches)}) -- this test's target "
        "step was renamed, duplicated, or removed"
    )
    script = matches[0].get("run")
    assert isinstance(script, str) and "go test" in script, (
        f"{WORKFLOW_PATH.name}: job {job_name!r} step {_STEP_NAME!r} has no "
        "`go test` invocation to inspect"
    )
    return script


def _timeout_minutes(job_name: str) -> int:
    value = _job(job_name).get("timeout-minutes")
    assert isinstance(value, int), (
        f"{WORKFLOW_PATH.name}: job {job_name!r} has no integer "
        "`timeout-minutes` -- this test's outer-bound comparison needs one"
    )
    return value


def _assert_inner_timeout_shorter_than_cap(
    script: str, job_name: str, outer_minutes: int
) -> None:
    """The guard's actual logic, factored out so a regression test can feed
    it a CONSTRUCTED script (the codex round 1 comment-decoy evasion)
    without needing a second copy of go.yml to point it at."""
    go_test_line = _the_go_test_line(script, job_name)
    match = _GO_TEST_TIMEOUT.search(go_test_line)
    assert match, (
        f"job {job_name!r}'s `go test` invocation has no `-timeout` flag -- "
        "relying on `timeout-minutes` alone reproduces acr's near-miss "
        "(CF, 09-03): a job cancelled by its own outer timeout reads as "
        "claimed-failure through this contract's poll logic, "
        "indistinguishable from a real defect"
    )
    raw = match.group(1)
    assert raw.endswith("m") and raw[:-1].isdigit(), (
        f"job {job_name!r}'s go test -timeout value {raw!r} is not a plain "
        "'<N>m' minutes form this test knows how to compare against "
        "timeout-minutes"
    )
    inner_minutes = int(raw[:-1])
    assert inner_minutes < outer_minutes, (
        f"job {job_name!r}'s inner go test -timeout ({inner_minutes}m) is "
        f"not strictly shorter than its own job timeout-minutes "
        f"({outer_minutes}m) -- the whole point of the inner bound is "
        "firing BEFORE the outer one; equal or longer means the outer cap "
        "can still win the race"
    )


def test_go_test_carries_an_inner_timeout_shorter_than_the_job_cap() -> None:
    for job_name in _TARGET_JOBS:
        script = _go_test_step_script(job_name)
        _assert_inner_timeout_shorter_than_cap(
            script, job_name, _timeout_minutes(job_name)
        )


def test_guard_rejects_a_timeout_mentioned_only_in_a_comment() -> None:
    """codex round 1 (#2145, CHAOS-4906), reproduced as a permanent
    regression: a COMMENT quoting the required `-timeout` form, sitting
    above the real `go test` invocation which has none, satisfied the OLD
    whole-script regex search -- a comment can say anything without making
    it true of the code below it. This must now raise, because
    `_the_go_test_line` only ever looks at a line that IS itself an
    executable `go test` invocation."""
    decoy_script = (
        "# required form: go test -timeout 8m\n"
        "go test -mod=readonly -count=1 -json ./internal/jobs/... -run pattern\n"
    )
    with pytest.raises(AssertionError, match="has no `-timeout` flag"):
        _assert_inner_timeout_shorter_than_cap(decoy_script, "fake-job", 10)


def test_guard_rejects_a_timeout_decoy_inside_a_dead_branch() -> None:
    """codex round 2 (#2145, CHAOS-4906), reproduced as a permanent
    regression: round 1's own fix (a stripped-leading-whitespace
    `startswith("go test")` check) was fooled by a dead `if false; then`
    branch whose OWN `go test -timeout 8m ...` line, taken alone, still
    starts with "go test" -- while the REAL invocation, prefixed with an
    env-var assignment (`CI_STAGE=arm64 go test ...`), does NOT start with
    "go test" and was silently missed entirely. `_the_go_test_line` picked
    the dead branch's line as "the" one real invocation and reported ITS
    timeout value, which was never actually enforced (the code never runs).

    The command-position fix does not try to guess which of the two is
    "real" -- it correctly recognizes BOTH as `go test` invocations
    (env-assignment stripped, or none to strip) and therefore raises an
    honest ambiguity error instead of silently picking the wrong one."""
    decoy_script = (
        "if false; then\n"
        "  go test -timeout 8m ./internal/jobs/... -run pattern\n"
        "fi\n"
        "CI_STAGE=arm64 go test -timeout 8m ./internal/jobs/... -run pattern\n"
    )
    with pytest.raises(AssertionError, match="expected exactly 1"):
        _assert_inner_timeout_shorter_than_cap(decoy_script, "fake-job", 10)
