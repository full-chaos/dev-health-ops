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
from pathlib import Path

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


def test_go_test_carries_an_inner_timeout_shorter_than_the_job_cap() -> None:
    for job_name in _TARGET_JOBS:
        script = _go_test_step_script(job_name)
        match = _GO_TEST_TIMEOUT.search(script)
        assert match, (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s `go test` invocation "
            "has no `-timeout` flag -- relying on `timeout-minutes` alone "
            "reproduces acr's near-miss (CF, 09-03): a job cancelled by its "
            "own outer timeout reads as claimed-failure through this "
            "contract's poll logic, indistinguishable from a real defect"
        )
        raw = match.group(1)
        assert raw.endswith("m") and raw[:-1].isdigit(), (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s go test -timeout "
            f"value {raw!r} is not a plain '<N>m' minutes form this test "
            "knows how to compare against timeout-minutes"
        )
        inner_minutes = int(raw[:-1])
        outer_minutes = _timeout_minutes(job_name)
        assert inner_minutes < outer_minutes, (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s inner go test "
            f"-timeout ({inner_minutes}m) is not strictly shorter than its "
            f"own job timeout-minutes ({outer_minutes}m) -- the whole point "
            "of the inner bound is firing BEFORE the outer one; equal or "
            "longer means the outer cap can still win the race"
        )
