"""docker-images.yml's (worker, linux/arm64) self-hosted-pool pilot.

WHY THIS TEST EXISTS
---------------------
CHAOS-4906: the second self-hosted-pool pilot (after go.yml's go-test-only
go-arm64-numeric-parity), and the first one that actually needs a working
Docker daemon inside the ephemeral oci-arc-runners pod -- unlike a `go test`
invocation, a `docker/build-push-action` step needs buildx, a daemon, and
registry push credentials. team-lead's ruling (09-03) required a PRECONDITION
inside the same PR: a dind smoke-test job that runs first and fails loud if
the pod cannot actually build and run a container, so a broken daemon is
diagnosed in isolation rather than discovered as an ambiguous failure inside
the real build step.

This file asserts the structural invariants a future edit could silently
break without any test noticing:
1. The self-hosted attempt job cannot run unless the dind smoke test job is
   also satisfied (a job `needs:` dependency, not just an `if:` check that
   could be edited to skip it).
2. The (worker, linux/arm64) combination is excluded from go-build's own
   matrix, so it is never built twice.
3. BOTH legs' build step (the self-hosted attempt AND the fallback's own
   copy -- either can be the one that actually does the real work) has its
   own `timeout-minutes` set and strictly shorter than its job's own
   `timeout-minutes` -- the double-bound lesson from go.yml's own pilot
   (CF, 09-03, acr's near-miss: a job cancelled by its own outer timeout
   reads as claimed-failure through the shared poll contract). The
   fallback's copy was missing this in the first cut of this PR -- caught
   and fixed before any round ran, not by one.
4. go-merge's `needs:` includes the fallback leg (not just go-build), same
   "needs ONLY the fallback, never the attempt directly" shape as go.yml's
   own aggregator -- a self-hosted attempt stuck permanently `queued` never
   reaches a terminal state, and a `needs:` on it directly would block
   go-merge for up to GitHub's 24h self-hosted backstop instead of the
   fallback's own bounded poll.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docker-images.yml"

_SELF_HOSTED_JOB = "go-build-worker-arm64-self-hosted"
_FALLBACK_JOB = "go-build-worker-arm64-fallback"
_DIND_JOB = "dind-smoke-test"
_PICK_RUNNER_JOB = "pick-runner"
_BUILD_STEP_NAME = "Build and push by digest"


def _document() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}


def _job(name: str) -> dict[str, object]:
    jobs = _document().get("jobs")
    assert isinstance(jobs, dict), (
        f"{WORKFLOW_PATH.name}: top-level 'jobs' is not a mapping"
    )
    job = jobs.get(name)
    assert isinstance(job, dict), (
        f"{WORKFLOW_PATH.name}: job {name!r} not found -- renamed or "
        "removed; update this test's job name constants rather than "
        "letting this assertion go stale"
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


def test_self_hosted_attempt_depends_on_the_dind_smoke_test() -> None:
    needs = _needs(_job(_SELF_HOSTED_JOB))
    assert _DIND_JOB in needs, (
        f"{WORKFLOW_PATH.name}: job {_SELF_HOSTED_JOB!r} does not `needs:` "
        f"{_DIND_JOB!r} -- the precondition must be a real job dependency "
        "(GitHub Actions blocks a job whose dependency failed), not an "
        "`if:` condition someone could edit or remove independently"
    )
    assert _PICK_RUNNER_JOB in needs, (
        f"{WORKFLOW_PATH.name}: job {_SELF_HOSTED_JOB!r} does not `needs:` "
        f"{_PICK_RUNNER_JOB!r}"
    )


def test_worker_arm64_is_excluded_from_the_go_build_matrix() -> None:
    strategy = _job("go-build").get("strategy")
    assert isinstance(strategy, dict), f"{WORKFLOW_PATH.name}: go-build has no strategy"
    matrix = strategy.get("matrix")
    assert isinstance(matrix, dict), (
        f"{WORKFLOW_PATH.name}: go-build's strategy has no matrix"
    )
    exclude = matrix.get("exclude") or []
    assert isinstance(exclude, list)
    assert {"target": "worker", "platform": "linux/arm64"} in exclude, (
        f"{WORKFLOW_PATH.name}: go-build's matrix no longer excludes "
        "(worker, linux/arm64) -- without this, the pilot jobs below AND "
        "the original matrix leg would both build and push the same "
        "image, racing each other for the same digest artifact name"
    )


def test_both_legs_build_step_has_an_inner_timeout_shorter_than_the_job_cap() -> None:
    # Both legs: the self-hosted attempt's step and the fallback's own copy
    # of the same step both need the inner bound -- either one can be the
    # leg that actually does the real work (see FALLBACK_JOB's own comment
    # on `steps.own.outputs.run_here`).
    for job_name in (_SELF_HOSTED_JOB, _FALLBACK_JOB):
        job = _job(job_name)
        job_timeout = job.get("timeout-minutes")
        assert isinstance(job_timeout, int), (
            f"{WORKFLOW_PATH.name}: job {job_name!r} has no integer timeout-minutes"
        )
        steps_raw = job.get("steps") or []
        assert isinstance(steps_raw, list)
        matches = [
            s
            for s in steps_raw
            if isinstance(s, dict) and s.get("name") == _BUILD_STEP_NAME
        ]
        assert len(matches) == 1, (
            f"{WORKFLOW_PATH.name}: job {job_name!r} must have exactly one "
            f"step named {_BUILD_STEP_NAME!r} (found {len(matches)})"
        )
        step_timeout = matches[0].get("timeout-minutes")
        assert isinstance(step_timeout, int), (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s {_BUILD_STEP_NAME!r} "
            "step has no `timeout-minutes` -- relying on the job's own cap "
            "alone reproduces acr's near-miss (CF, 09-03): a job cancelled "
            "by its own outer timeout reads as claimed-failure through the "
            "shared poll contract, indistinguishable from a real defect. "
            "This is a `uses:` action step, so the double bound is a "
            "native per-step `timeout-minutes:`, not a shell `timeout` "
            "wrapper (there is no command line to wrap)"
        )
        assert step_timeout < job_timeout, (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s {_BUILD_STEP_NAME!r} "
            f"step timeout ({step_timeout}m) is not strictly shorter than "
            f"the job's own timeout-minutes ({job_timeout}m) -- the whole "
            "point of the inner bound is firing BEFORE the outer one"
        )


def test_go_merge_needs_the_fallback_leg_not_the_attempt() -> None:
    needs = _needs(_job("go-merge"))
    assert _FALLBACK_JOB in needs, (
        f"{WORKFLOW_PATH.name}: go-merge does not `needs:` {_FALLBACK_JOB!r} "
        "-- without this, go-merge could run before the pilot leg has "
        "uploaded its digest artifact"
    )
    assert _SELF_HOSTED_JOB not in needs, (
        f"{WORKFLOW_PATH.name}: go-merge `needs:` {_SELF_HOSTED_JOB!r} "
        "directly -- the self-hosted attempt can end up permanently "
        "`queued` with no terminal state of its own (pool down, or "
        "already at capacity), and a `needs:` on it directly would block "
        "go-merge for up to GitHub's 24h self-hosted backstop instead of "
        "the fallback leg's own bounded poll. go-merge must depend ONLY "
        "on the fallback, which is built to be the sole source of truth."
    )


def test_digest_artifact_names_match_the_wildcard_go_merge_downloads() -> None:
    # go-merge downloads `digests-go-${{ matrix.target }}-*` (a wildcard on
    # the suffix) -- both new jobs must upload under a name that glob
    # matches, or their digest is silently invisible to the merge step.
    for job_name in (_SELF_HOSTED_JOB, _FALLBACK_JOB):
        steps_raw = _job(job_name).get("steps") or []
        assert isinstance(steps_raw, list)
        upload_steps = [
            s
            for s in steps_raw
            if isinstance(s, dict)
            and str(s.get("uses", "")).startswith("actions/upload-artifact@")
        ]
        assert len(upload_steps) == 1, (
            f"{WORKFLOW_PATH.name}: job {job_name!r} must have exactly one "
            f"upload-artifact step (found {len(upload_steps)})"
        )
        with_block = upload_steps[0].get("with")
        assert isinstance(with_block, dict)
        name = str(with_block.get("name", ""))
        assert name.startswith("digests-go-worker-"), (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s digest artifact name "
            f"{name!r} does not start with 'digests-go-worker-' -- "
            "go-merge's `digests-go-worker-*` download pattern would miss it"
        )
