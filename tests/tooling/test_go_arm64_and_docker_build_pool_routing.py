"""go.yml's arm64-numeric-parity and docker-images.yml's docker-build
worker/arm64 self-hosted-pool routing (CHAOS-4906, runner contract v1.6).

WHY THIS TEST EXISTS
---------------------
Contract v1.6 (signed 2026-09-03, chris via CF team-lead; see
/Users/chris/projects/full-chaos/dev-health/.remember/lanes/
lane-4904-acr-runners/contract-v1.6-signed.md) retires the v1.5.1
fallback-poller pattern for routed jobs. This PR converts two more pilots
to the v1.6 shape:

* go.yml's go-arm64-numeric-parity -- #2145's original pick-runner/
  self-hosted-attempt/fallback trio, now two jobs sharing one `name:` (no
  aggregator: a single job, not a matrix, same shape as
  go-container-reproducibility elsewhere in this file).
* docker-images.yml's docker-build (worker, linux/arm64) leg -- re-added
  from closed #2180 (which also used the v1.5.1 poller) in v1.6 shape.
  Needs its own `dind-smoke-test` (a working Docker daemon inside the
  ephemeral pod), and go-merge's own `needs:` must tolerate exactly one of
  the two legs being skipped, the same "both skipped is a routing bug"
  shape test_go_storage_and_reproducibility_pilot.py already asserts for
  the storage-integration shards.

This file asserts the structural invariants a future edit could silently
break without any test noticing:
1. Both bundles' hosted and self-hosted legs are gated on COMPLEMENTARY
   conditions over SELF_HOSTED_RUNNERS (+ fork-PR exclusion) -- exactly
   one runs, never both, never neither.
2. Both legs of both bundles share the SAME `name:`.
3. go-arm64-numeric-parity needs NO `dind-smoke-test` dependency (it runs
   bit-pattern golden tests only, never touches a Docker socket) --
   unlike the docker-build legs, which DO need one.
4. Each docker-build self-hosted job's real-work step carries a
   step-level `timeout-minutes:` strictly shorter than its own job's --
   arm64-numeric-parity's own inner-`go test`-timeout equivalent is
   already covered by test_go_arm64_numeric_parity_inner_test_timeout.py
   (updated in this PR for the new job names), not duplicated here.
5. docker-images.yml's `go-build` matrix excludes (worker, linux/arm64)
   so it is never built twice.
6. `go-merge` needs BOTH new arm64 legs, and its own check rejects
   "both skipped" as a routing bug, not a pass.
7. NEITHER new self-hosted job's steps contain a poll/wait step
   (structural confirmation the v1.5.1 poller pattern was actually
   retired here, not left dangling alongside the new gate).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GO_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"
DOCKER_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docker-images.yml"

_ARM64_PARITY_HOSTED = "go-arm64-numeric-parity"
_ARM64_PARITY_SELF_HOSTED = "go-arm64-numeric-parity-self-hosted"

_DIND_JOB = "dind-smoke-test"
_DOCKER_BUILD_HOSTED = "go-build-worker-arm64"
_DOCKER_BUILD_SELF_HOSTED = "go-build-worker-arm64-self-hosted"
_DOCKER_BUILD_RUN_STEP = "Build and push by digest"
_GO_BUILD_MATRIX_JOB = "go-build"
_GO_MERGE_JOB = "go-merge"

_SELF_HOSTED_CONDITION = (
    "vars.SELF_HOSTED_RUNNERS == 'enabled' && "
    "(github.event_name != 'pull_request' || "
    "github.event.pull_request.head.repo.full_name == github.repository)"
)
_HOSTED_CONDITION = (
    "vars.SELF_HOSTED_RUNNERS != 'enabled' || "
    "(github.event_name == 'pull_request' && "
    "github.event.pull_request.head.repo.full_name != github.repository)"
)

_ELIGIBILITY_CONDITION = (
    "github.event_name == 'workflow_dispatch' || "
    "github.event_name == 'release' || "
    "(github.event_name == 'push' && github.ref == 'refs/heads/main') || "
    "needs.changes.outputs.code == 'true'"
)


def _document(path: Path) -> dict[str, object]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _job(path: Path, name: str) -> dict[str, object]:
    jobs = _document(path).get("jobs")
    assert isinstance(jobs, dict), f"{path.name}: top-level 'jobs' is not a mapping"
    job = jobs.get(name)
    assert isinstance(job, dict), (
        f"{path.name}: job {name!r} not found -- renamed or removed; "
        "update this test's job name constants rather than letting this "
        "assertion go stale"
    )
    return job


def _needs(job: dict[str, object]) -> list[str]:
    needs = job.get("needs")
    if needs is None:
        return []
    if isinstance(needs, str):
        return [needs]
    assert isinstance(needs, list)
    return [str(n) for n in needs]


def _dict_field(obj: dict[str, object], key: str) -> dict[str, object]:
    value = obj.get(key)
    if value is None:
        return {}
    assert isinstance(value, dict), f"{key!r} is not a mapping: {value!r}"
    return value


def _step_by_name(job: dict[str, object], name: str) -> dict[str, object]:
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    matches = [s for s in steps_raw if isinstance(s, dict) and s.get("name") == name]
    assert len(matches) == 1, (
        f"must have exactly one step named {name!r} (found {len(matches)})"
    )
    return matches[0]


# ---------------------------------------------------------------------------
# go.yml: go-arm64-numeric-parity
# ---------------------------------------------------------------------------


def test_arm64_parity_legs_are_gated_on_complementary_conditions() -> None:
    hosted_if = str(_job(GO_WORKFLOW_PATH, _ARM64_PARITY_HOSTED).get("if", ""))
    self_hosted_if = str(
        _job(GO_WORKFLOW_PATH, _ARM64_PARITY_SELF_HOSTED).get("if", "")
    )
    assert hosted_if == _HOSTED_CONDITION, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_HOSTED!r}'s `if:` "
        f"({hosted_if!r}) does not match the expected hosted-fallback "
        "condition"
    )
    assert self_hosted_if == _SELF_HOSTED_CONDITION, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_SELF_HOSTED!r}'s `if:` "
        f"({self_hosted_if!r}) does not match the expected "
        "self-hosted-eligible condition"
    )


def test_arm64_parity_legs_share_one_stable_check_name() -> None:
    hosted_name = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_HOSTED).get("name")
    self_hosted_name = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_SELF_HOSTED).get("name")
    assert hosted_name == self_hosted_name == "go-arm64-numeric-parity", (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_HOSTED!r} and "
        f"{_ARM64_PARITY_SELF_HOSTED!r} must share the same templated "
        f"`name:` -- got {hosted_name!r} / {self_hosted_name!r}"
    )


def test_arm64_parity_self_hosted_leg_needs_no_dind() -> None:
    # Unlike the storage-integration shards, container-reproducibility, and
    # the new docker-build legs below, this job never touches a Docker
    # socket -- it runs `go test` bit-pattern goldens. A `dind-smoke-test`
    # dependency here would be a wasted job on every self-hosted run, and
    # its absence is worth pinning so nobody "completes the pattern" by
    # adding one out of habit.
    needs = _needs(_job(GO_WORKFLOW_PATH, _ARM64_PARITY_SELF_HOSTED))
    assert _DIND_JOB not in needs, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_SELF_HOSTED!r} should not "
        f"`needs:` {_DIND_JOB!r} -- this job never uses Docker"
    )


def test_arm64_parity_legs_have_the_pre_poller_outer_timeout() -> None:
    # The inner-vs-outer comparison itself (and its comment-immune `go
    # test` line detection -- a naive whole-script regex search would
    # match this job's own explanatory comment, which literally contains
    # the substring "-timeout 8m", instead of the real invocation; caught
    # by red-proofing this exact assertion against a mutated real
    # invocation while leaving the comment untouched) is already covered,
    # correctly, by test_go_arm64_numeric_parity_inner_test_timeout.py's
    # test_go_test_carries_an_inner_timeout_shorter_than_the_job_cap for
    # these same two job names -- not duplicated here. This only pins the
    # OUTER value itself, which that file's comparison takes as a given
    # rather than asserting.
    for job_name in (_ARM64_PARITY_HOSTED, _ARM64_PARITY_SELF_HOSTED):
        job_timeout = _job(GO_WORKFLOW_PATH, job_name).get("timeout-minutes")
        assert job_timeout == 10, (
            f"{GO_WORKFLOW_PATH.name}: {job_name!r}'s timeout-minutes "
            f"({job_timeout!r}) does not match the job's pre-poller "
            "baseline (10m) -- v1.6 has no fallback-poll budget to size for"
        )


def test_arm64_parity_self_hosted_has_no_poll_step() -> None:
    job = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_SELF_HOSTED)
    needs = _needs(job)
    assert "pick-runner" not in needs, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_SELF_HOSTED!r} still "
        "`needs:` pick-runner -- v1.6 gates directly on "
        "vars.SELF_HOSTED_RUNNERS"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    for step in steps_raw:
        assert isinstance(step, dict)
        run_text = str(step.get("run", ""))
        assert "job_status()" not in run_text, (
            f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_SELF_HOSTED!r} still "
            "has a poll script -- v1.6 retires the fallback poller"
        )
        assert "run_here" not in run_text, (
            f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_SELF_HOSTED!r} still "
            "references steps.own.outputs.run_here -- v1.6 retires the "
            "fallback poller's ownership-decision step"
        )


# ---------------------------------------------------------------------------
# docker-images.yml: go-build-worker-arm64
# ---------------------------------------------------------------------------


def test_docker_build_legs_are_gated_on_complementary_conditions_and_eligibility() -> (
    None
):
    hosted_if = str(_job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_HOSTED).get("if", ""))
    self_hosted_if = str(
        _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_SELF_HOSTED).get("if", "")
    )
    # Unlike the arm64-parity/storage-shard bundles, docker-build ALSO
    # carries go-build's own eligibility condition (CHAOS-4921's
    # push-to-main-always-builds / paths-filter logic), ANDed with the
    # v1.6 routing condition -- this leg replaces exactly the matrix
    # combination excluded from go-build, so it must behave identically to
    # that combination when the switch is off, not build unconditionally.
    expected_hosted = f"({_HOSTED_CONDITION}) && ({_ELIGIBILITY_CONDITION})"
    expected_self_hosted = f"{_SELF_HOSTED_CONDITION} && ({_ELIGIBILITY_CONDITION})"
    assert hosted_if == expected_hosted, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_HOSTED!r}'s `if:` "
        f"({hosted_if!r}) does not match the expected AND of the v1.6 "
        f"hosted-fallback condition and go-build's own eligibility "
        f"condition ({expected_hosted!r})"
    )
    assert self_hosted_if == expected_self_hosted, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_SELF_HOSTED!r}'s "
        f"`if:` ({self_hosted_if!r}) does not match the expected AND of "
        f"the v1.6 self-hosted-eligible condition and go-build's own "
        f"eligibility condition ({expected_self_hosted!r})"
    )


def test_docker_build_legs_share_one_stable_check_name() -> None:
    hosted_name = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_HOSTED).get("name")
    self_hosted_name = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_SELF_HOSTED).get("name")
    assert hosted_name == self_hosted_name == "go-build-worker-arm64", (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_HOSTED!r} and "
        f"{_DOCKER_BUILD_SELF_HOSTED!r} must share the same templated "
        f"`name:` -- got {hosted_name!r} / {self_hosted_name!r}"
    )


def test_docker_build_self_hosted_leg_depends_on_dind() -> None:
    needs = _needs(_job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_SELF_HOSTED))
    assert _DIND_JOB in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_SELF_HOSTED!r} does "
        f"not `needs:` {_DIND_JOB!r} -- a real job dependency, not just an "
        "`if:`, since this leg needs a working Docker daemon to build+push"
    )


def test_docker_build_legs_have_a_shorter_inner_timeout() -> None:
    cases = [
        (_DOCKER_BUILD_HOSTED, 15),
        (_DOCKER_BUILD_SELF_HOSTED, 10),
    ]
    for job_name, expected_outer in cases:
        job = _job(DOCKER_WORKFLOW_PATH, job_name)
        job_timeout = job.get("timeout-minutes")
        assert job_timeout == expected_outer, (
            f"{DOCKER_WORKFLOW_PATH.name}: {job_name!r}'s timeout-minutes "
            f"({job_timeout!r}) does not match the expected sizing "
            f"({expected_outer}m)"
        )
        step = _step_by_name(job, _DOCKER_BUILD_RUN_STEP)
        # This step is a `uses:` action, so the double bound is native
        # per-step `timeout-minutes:`, not a shell `timeout` wrapper.
        step_timeout = step.get("timeout-minutes")
        assert isinstance(step_timeout, int) and isinstance(job_timeout, int), (
            f"{DOCKER_WORKFLOW_PATH.name}: {job_name!r}'s {_DOCKER_BUILD_RUN_STEP!r} "
            "step needs an integer `timeout-minutes:`"
        )
        assert step_timeout < job_timeout, (
            f"{DOCKER_WORKFLOW_PATH.name}: {job_name!r}'s step-level "
            f"timeout ({step_timeout}m) is not strictly shorter than its "
            f"own job timeout-minutes ({job_timeout}m)"
        )


def test_docker_build_self_hosted_has_no_poll_step() -> None:
    job = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_SELF_HOSTED)
    needs = _needs(job)
    assert "pick-runner" not in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_SELF_HOSTED!r} still "
        "`needs:` pick-runner"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    for step in steps_raw:
        assert isinstance(step, dict)
        run_text = str(step.get("run", ""))
        assert "job_status()" not in run_text, (
            f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_SELF_HOSTED!r} "
            "still has a poll script"
        )
        assert "run_here" not in run_text, (
            f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_SELF_HOSTED!r} "
            "still references steps.own.outputs.run_here"
        )


def test_go_build_matrix_excludes_worker_arm64() -> None:
    matrix = _dict_field(
        _dict_field(_job(DOCKER_WORKFLOW_PATH, _GO_BUILD_MATRIX_JOB), "strategy"),
        "matrix",
    )
    excludes = matrix.get("exclude")
    assert (
        isinstance(excludes, list)
        and {
            "target": "worker",
            "platform": "linux/arm64",
        }
        in excludes
    ), (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_BUILD_MATRIX_JOB!r}'s matrix "
        "must exclude (worker, linux/arm64) -- otherwise it is built "
        "twice, once by the matrix and once by the dedicated pilot legs"
    )


def test_go_merge_needs_both_docker_build_legs_and_rejects_both_skipped() -> None:
    needs = _needs(_job(DOCKER_WORKFLOW_PATH, _GO_MERGE_JOB))
    assert _DOCKER_BUILD_HOSTED in needs and _DOCKER_BUILD_SELF_HOSTED in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r} must `needs:` "
        f"both {_DOCKER_BUILD_HOSTED!r} and {_DOCKER_BUILD_SELF_HOSTED!r}, "
        f"got {needs!r}"
    )
    job_if = str(_job(DOCKER_WORKFLOW_PATH, _GO_MERGE_JOB).get("if", ""))
    assert "always()" in job_if, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r}'s `if:` must use "
        "always() -- otherwise GitHub's implicit needs-success check "
        "treats whichever arm64 leg is skipped as a failure and skips "
        "go-merge entirely on every eligible run"
    )
    assert (
        f"needs.{_DOCKER_BUILD_HOSTED}.result == 'skipped'" in job_if
        and f"needs.{_DOCKER_BUILD_SELF_HOSTED}.result == 'skipped'" in job_if
    ), (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r}'s `if:` must "
        "explicitly reject the both-legs-skipped case as a routing bug, "
        f"not a pass: {job_if!r}"
    )
