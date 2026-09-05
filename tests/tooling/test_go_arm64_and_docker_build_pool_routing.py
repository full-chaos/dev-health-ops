"""go.yml's go-arm64-numeric-parity and docker-images.yml's
go-build-worker-arm64 routing (CHAOS-4906, runner contract v1.6, amended
2026-09-04 -- twice, the two jobs ending up with DIFFERENT shapes).

WHY THIS TEST EXISTS
---------------------
Contract v1.6 (signed 2026-09-03, chris via CF team-lead; see
/Users/chris/projects/full-chaos/dev-health/.remember/lanes/
lane-4904-acr-runners/contract-v1.6-signed.md) originally gave both of these
jobs a hosted `ubuntu-26.04-arm` leg alongside a self-hosted `oci-arc-runners`
leg, gated on complementary conditions over the same SELF_HOSTED_RUNNERS
variable.

AMENDED 2026-09-04, first pass (chris, 16:59): the enterprise's GitHub-hosted
runner label list is exactly ubuntu-latest/ubuntu-22.04/ubuntu-24.04 (+
windows/macos) -- ubuntu-26.04-arm was never a real hosted label there, so
the "hosted" leg of both pairs could never actually have run. Both hosted
legs were dropped; each self-hosted leg took over its pair's original job id
(dropping the `-self-hosted` suffix), so it is now the sole, direct producer
of the shared check name / `needs:` reference.

AMENDED 2026-09-04, second pass (chris, follow-up ruling on go-build-worker-
arm64 specifically): the two jobs' hosted-fallback story DIVERGES here.
go-arm64-numeric-parity keeps NO fallback -- its whole purpose is proving
real-hardware FMA behavior, an x64 run would prove nothing, and it is not a
required context, so a coverage gap when SELF_HOSTED_RUNNERS is disabled or
on a fork PR is accepted. go-build-worker-arm64 is different: it feeds the
PUBLISHED `worker` image's linux/arm64 layer, so losing that fallback
entirely would silently drop arm64 support from a real artifact -- it now
carries the SAME `runs-on:`-level routing expression as the build/go-build
matrix arm64 legs (ARC when eligible, else ubuntu-latest with QEMU
cross-building `platforms: linux/arm64`), moving the routing decision OUT of
`if:` and leaving only go-build's own eligibility condition there (plus
tolerating dind-smoke-test's `skipped` result, since that job only runs when
this leg actually lands on ARC).

WHAT THIS ASSERTS
-----------------
1. go-arm64-numeric-parity's `if:` is exactly the v1.6 self-hosted-eligible
   condition (no fallback); go-build-worker-arm64's `if:` is go-build's own
   eligibility condition plus dind-tolerance (no SELF_HOSTED_RUNNERS/fork-PR
   clause -- that moved to `runs-on:`).
2. go-arm64-numeric-parity's `runs-on` is the bare ARC pool. go-build-
   worker-arm64's `runs-on` is the ARC-or-x64-fallback routing expression,
   with a "Set up QEMU" step for the fallback leg.
3. go-arm64-numeric-parity needs NO `dind-smoke-test` dependency (it runs
   bit-pattern golden tests only, never touches a Docker socket) --
   go-build-worker-arm64 DOES need one (it builds+pushes a real image).
4. go-build-worker-arm64's real-work step carries a step-level
   `timeout-minutes:` strictly shorter than its own job's -- arm64-numeric-
   parity's own inner-`go test`-timeout equivalent is already covered by
   test_go_arm64_numeric_parity_inner_test_timeout.py, not duplicated here.
5. docker-images.yml's `go-build` matrix excludes (worker, linux/arm64) so
   it is never built twice.
6. `go-merge` needs go-build-worker-arm64 (single id, post-amendment) and
   its own check tolerates that leg being skipped (by the eligibility
   condition, not by runner routing) without treating it as a
   pass-through failure.
7. Neither job's steps contain a poll/wait step (structural confirmation the
   v1.5.1 poller pattern stays retired, not left dangling alongside the new
   gate).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GO_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"
DOCKER_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docker-images.yml"

_ARM64_PARITY_JOB = "go-arm64-numeric-parity"

_DIND_JOB = "dind-smoke-test"
_DOCKER_BUILD_JOB = "go-build-worker-arm64"
_DOCKER_BUILD_RUN_STEP = "Build and push by digest"
_GO_BUILD_MATRIX_JOB = "go-build"
_GO_MERGE_JOB = "go-merge"

_SELF_HOSTED_CONDITION = (
    "vars.SELF_HOSTED_RUNNERS == 'enabled' && "
    "(github.event_name != 'pull_request' || "
    "github.event.pull_request.head.repo.full_name == github.repository)"
)

_ELIGIBILITY_CONDITION = (
    "github.event_name == 'workflow_dispatch' || "
    "github.event_name == 'release' || "
    "(github.event_name == 'push' && github.ref == 'refs/heads/main') || "
    "needs.changes.outputs.code == 'true'"
)

# The single-expression `runs-on:` form used by go-build-worker-arm64 and
# the build/go-build matrix arm64 legs: ARC when SELF_HOSTED_RUNNERS is
# enabled and this isn't a fork PR, else ubuntu-latest (x64, QEMU-assisted).
_ROUTING_EXPRESSION = (
    "${{ (vars.SELF_HOSTED_RUNNERS == 'enabled' && "
    "(github.event_name != 'pull_request' || "
    "github.event.pull_request.head.repo.full_name == github.repository)) "
    '&& fromJSON(\'["self-hosted","oci-arc-runners"]\') '
    "|| 'ubuntu-latest' }}"
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


def _is_self_hosted_arc(runs_on: object) -> bool:
    if isinstance(runs_on, list):
        return set(runs_on) >= {"self-hosted", "oci-arc-runners"}
    if isinstance(runs_on, str):
        return "self-hosted" in runs_on and "oci-arc-runners" in runs_on
    return False


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


def test_arm64_parity_is_gated_on_the_self_hosted_condition() -> None:
    job_if = str(_job(GO_WORKFLOW_PATH, _ARM64_PARITY_JOB).get("if", ""))
    assert job_if == _SELF_HOSTED_CONDITION, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r}'s `if:` ({job_if!r}) "
        f"does not match the expected self-hosted-eligible condition -- this "
        "job has no hosted fallback since 2026-09-04 (ubuntu-26.04-arm is "
        "not a real enterprise hosted label), so it must not run outside "
        "that condition"
    )


def test_arm64_parity_runs_on_the_arc_pool() -> None:
    runs_on = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_JOB).get("runs-on")
    assert _is_self_hosted_arc(runs_on), (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r}'s runs-on "
        f"({runs_on!r}) must be the self-hosted oci-arc-runners pool -- it "
        "has no valid hosted fallback"
    )


def test_arm64_parity_needs_no_dind() -> None:
    # Unlike go-build-worker-arm64 below, this job never touches a Docker
    # socket -- it runs `go test` bit-pattern goldens. A `dind-smoke-test`
    # dependency here would be a wasted job on every run, and its absence is
    # worth pinning so nobody "completes the pattern" by adding one out of
    # habit.
    needs = _needs(_job(GO_WORKFLOW_PATH, _ARM64_PARITY_JOB))
    assert _DIND_JOB not in needs, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r} should not `needs:` "
        f"{_DIND_JOB!r} -- this job never uses Docker"
    )


def test_arm64_parity_has_the_pre_poller_outer_timeout() -> None:
    # The inner-vs-outer comparison itself is already covered, correctly, by
    # test_go_arm64_numeric_parity_inner_test_timeout.py -- not duplicated
    # here. This only pins the OUTER value.
    job_timeout = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_JOB).get("timeout-minutes")
    assert job_timeout == 10, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r}'s timeout-minutes "
        f"({job_timeout!r}) does not match the job's pre-poller baseline "
        "(10m)"
    )


def test_arm64_parity_has_no_poll_step() -> None:
    job = _job(GO_WORKFLOW_PATH, _ARM64_PARITY_JOB)
    needs = _needs(job)
    assert "pick-runner" not in needs, (
        f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r} still `needs:` "
        "pick-runner -- v1.6 gates directly on vars.SELF_HOSTED_RUNNERS"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    for step in steps_raw:
        assert isinstance(step, dict)
        run_text = str(step.get("run", ""))
        assert "job_status()" not in run_text, (
            f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r} still has a "
            "poll script -- v1.6 retires the fallback poller"
        )
        assert "run_here" not in run_text, (
            f"{GO_WORKFLOW_PATH.name}: {_ARM64_PARITY_JOB!r} still "
            "references steps.own.outputs.run_here -- v1.6 retires the "
            "fallback poller's ownership-decision step"
        )


# ---------------------------------------------------------------------------
# docker-images.yml: go-build-worker-arm64
# ---------------------------------------------------------------------------


def test_docker_build_is_gated_on_eligibility_not_runner_choice() -> None:
    # Amended 2026-09-04 (chris, follow-up ruling): unlike the other ARC-
    # routed jobs in this file, go-build-worker-arm64 feeds the PUBLISHED
    # `worker` image's linux/arm64 layer, so it must not skip just because
    # SELF_HOSTED_RUNNERS is disabled or the PR is from a fork -- that
    # routing decision moved into `runs-on` (see
    # test_docker_build_routes_to_arc_or_falls_back_to_x64_qemu below)
    # instead of gating the whole job. What remains in `if:` is: go-build's
    # own eligibility condition, plus tolerating dind-smoke-test's `skipped`
    # result (it only runs when this leg lands on ARC) while still
    # rejecting a real dind failure/cancellation.
    job_if = str(_job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_JOB).get("if", ""))
    expected = (
        "always() && "
        "needs.changes.result == 'success' && "
        f"needs.{_DIND_JOB}.result != 'failure' && "
        f"needs.{_DIND_JOB}.result != 'cancelled' && "
        f"({_ELIGIBILITY_CONDITION})"
    )
    assert job_if == expected, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r}'s `if:` "
        f"({job_if!r}) does not match the expected eligibility + "
        f"dind-tolerance condition ({expected!r})"
    )


def test_docker_build_routes_to_arc_or_falls_back_to_x64_qemu() -> None:
    # Unlike go-arm64-numeric-parity, this job has a real x64 fallback
    # (chris's follow-up ruling: a published image losing its arm64 layer
    # is product-facing, not just a coverage gap) -- so its `runs-on` is the
    # SAME routing expression as the build/go-build matrix arm64 legs, not
    # a bare self-hosted label.
    job = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_JOB)
    runs_on = str(job.get("runs-on", ""))
    assert runs_on == _ROUTING_EXPRESSION, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r}'s runs-on "
        f"({runs_on!r}) does not match the expected ARC-or-x64-fallback "
        f"routing expression ({_ROUTING_EXPRESSION!r})"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    step_names = [s.get("name") for s in steps_raw if isinstance(s, dict)]
    assert "Set up QEMU" in step_names, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r} must set up "
        "QEMU -- its x64 fallback needs to cross-build platforms: "
        "linux/arm64, which it never had to do while this leg only ever "
        "ran on real arm64 hardware"
    )


def test_docker_build_depends_on_dind() -> None:
    needs = _needs(_job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_JOB))
    assert _DIND_JOB in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r} does not "
        f"`needs:` {_DIND_JOB!r} -- a real job dependency, not just an "
        "`if:`, since this leg needs a working Docker daemon to build+push"
    )


def test_docker_build_has_a_shorter_inner_timeout() -> None:
    job = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_JOB)
    job_timeout = job.get("timeout-minutes")
    assert job_timeout == 10, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r}'s "
        f"timeout-minutes ({job_timeout!r}) does not match the expected "
        "sizing (10m)"
    )
    step = _step_by_name(job, _DOCKER_BUILD_RUN_STEP)
    # This step is a `uses:` action, so the double bound is native per-step
    # `timeout-minutes:`, not a shell `timeout` wrapper.
    step_timeout = step.get("timeout-minutes")
    assert isinstance(step_timeout, int) and isinstance(job_timeout, int), (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r}'s "
        f"{_DOCKER_BUILD_RUN_STEP!r} step needs an integer `timeout-minutes:`"
    )
    assert step_timeout < job_timeout, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r}'s step-level "
        f"timeout ({step_timeout}m) is not strictly shorter than its own "
        f"job timeout-minutes ({job_timeout}m)"
    )


def test_docker_build_has_no_poll_step() -> None:
    job = _job(DOCKER_WORKFLOW_PATH, _DOCKER_BUILD_JOB)
    needs = _needs(job)
    assert "pick-runner" not in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r} still `needs:` pick-runner"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    for step in steps_raw:
        assert isinstance(step, dict)
        run_text = str(step.get("run", ""))
        assert "job_status()" not in run_text, (
            f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r} still has a "
            "poll script"
        )
        assert "run_here" not in run_text, (
            f"{DOCKER_WORKFLOW_PATH.name}: {_DOCKER_BUILD_JOB!r} still "
            "references steps.own.outputs.run_here"
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
        "twice, once by the matrix and once by the dedicated pilot leg"
    )


def test_go_merge_needs_docker_build_leg_and_tolerates_it_skipping() -> None:
    needs = _needs(_job(DOCKER_WORKFLOW_PATH, _GO_MERGE_JOB))
    assert _DOCKER_BUILD_JOB in needs, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r} must `needs:` "
        f"{_DOCKER_BUILD_JOB!r}, got {needs!r}"
    )
    job_if = str(_job(DOCKER_WORKFLOW_PATH, _GO_MERGE_JOB).get("if", ""))
    assert "always()" in job_if, (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r}'s `if:` must use "
        "always() -- otherwise GitHub's implicit needs-success check treats "
        "a skipped arm64 leg (SELF_HOSTED_RUNNERS disabled, or a fork PR) "
        "as a failure and skips go-merge entirely on every eligible run"
    )
    assert (
        f"needs.{_DOCKER_BUILD_JOB}.result != 'failure'" in job_if
        and f"needs.{_DOCKER_BUILD_JOB}.result != 'cancelled'" in job_if
    ), (
        f"{DOCKER_WORKFLOW_PATH.name}: {_GO_MERGE_JOB!r}'s `if:` must "
        f"explicitly tolerate {_DOCKER_BUILD_JOB!r} being skipped while "
        f"still rejecting a real failure or cancellation: {job_if!r}"
    )
