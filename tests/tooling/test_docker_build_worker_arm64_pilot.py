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


def test_fallback_leg_also_depends_on_the_dind_smoke_test() -> None:
    # codex round 1 (#2180, CHAOS-4906), P1: `needs: pick-runner` alone let
    # this job's own 5-minute queue-wait timer start in PARALLEL with
    # dind-smoke-test's own run (up to 5 minutes), instead of after it. If
    # dind takes close to its full budget, the fallback's timer can expire
    # ("unclaimed", start a hosted build) at nearly the moment the attempt
    # (which correctly needs both pick-runner AND dind-smoke-test) first
    # becomes eligible to start -- both legs then build+push the SAME real
    # arm64 digest, a registry write, not just wasted CPU. Fixed by adding
    # dind-smoke-test to this job's own `needs:` too, so its queue timer
    # starts only once the precondition has actually resolved.
    needs = _needs(_job(_FALLBACK_JOB))
    assert _DIND_JOB in needs, (
        f"{WORKFLOW_PATH.name}: job {_FALLBACK_JOB!r} does not `needs:` "
        f"{_DIND_JOB!r} -- its own queue-wait timer can then start BEFORE "
        "the dind precondition has resolved, racing the self-hosted "
        "attempt (which correctly waits for both) to build+push the same "
        "digest"
    )


def test_fallback_leg_is_gated_by_go_builds_own_eligibility_condition() -> None:
    # codex round 1 (#2180, CHAOS-4906), P2: `if: always()` alone made this
    # job run on every event pick-runner's own gate allows (kill switch +
    # non-fork only), with no regard for go-build's own eligibility
    # condition -- a merge_group event or a docs-only PR would reach this
    # job and build+push a real digest go-merge never even consumes,
    # changing this workflow's behavior on those events relative to before
    # this PR. Fixed by ANDing go-build's own condition into this job's
    # `if:`, while keeping `always()` so a skipped/failed pick-runner or
    # dind-smoke-test still doesn't block this job from running when it IS
    # eligible.
    condition = str(_job(_FALLBACK_JOB).get("if", ""))
    go_build_condition = str(_job("go-build").get("if", ""))
    assert "always(" in condition, (
        f"{WORKFLOW_PATH.name}: job {_FALLBACK_JOB!r}'s `if:` no longer "
        "contains always() -- without it, a skipped or failed pick-runner/"
        "dind-smoke-test would block this job entirely, defeating its own "
        "purpose as the sole source of truth for the required check"
    )
    # Compare on the underlying boolean structure, not exact text -- both
    # are written as `expr1 || expr2 || ...`, so every OR-branch of
    # go-build's own condition must appear verbatim in the fallback's.
    go_build_branches = [b.strip() for b in go_build_condition.split("||")]
    for branch in go_build_branches:
        assert branch in condition, (
            f"{WORKFLOW_PATH.name}: job {_FALLBACK_JOB!r}'s `if:` is "
            f"missing go-build's own eligibility branch {branch!r} -- "
            f"fallback={condition!r} go-build={go_build_condition!r}. "
            "Without every branch, this job can run on an event go-build "
            "itself would have skipped."
        )


# codex round 1 (#2180, CHAOS-4906), P2: a `needs:` dependency alone is not a
# hard block if the DEPENDENT job's own `if:` is (or becomes) a status
# override -- `always()`, `failure()`, `cancelled()` -- since those bypass
# the implicit `success()` check GitHub Actions applies to a plain `if:`
# expression. The PRECEDING test proves the dependency EXISTS; this one
# proves the attempt's own `if:` cannot be edited into ignoring it.
_STATUS_OVERRIDE_FUNCTIONS = ("always(", "failure(", "cancelled(")


def test_self_hosted_attempt_if_is_not_a_status_override() -> None:
    condition = str(_job(_SELF_HOSTED_JOB).get("if", ""))
    assert condition, (
        f"{WORKFLOW_PATH.name}: job {_SELF_HOSTED_JOB!r} has no `if:` at "
        "all -- it would run unconditionally, the same defect this test "
        "guards against, just with no expression to inspect"
    )
    for override in _STATUS_OVERRIDE_FUNCTIONS:
        assert override not in condition, (
            f"{WORKFLOW_PATH.name}: job {_SELF_HOSTED_JOB!r}'s `if:` "
            f"({condition!r}) contains {override!r} -- a status-override "
            "function bypasses the implicit success() check GitHub "
            "Actions applies to needs:, so the dind-smoke-test precondition "
            "the preceding test proves exists could be silently skipped, "
            "cancelled, or failed and this job would run anyway"
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
    # codex round 1 (#2180, CHAOS-4906), P2 guard false-negative: the two
    # assertions above only check the PILOT legs -- neither would notice
    # `go-build` itself silently dropping out of go-merge's `needs:`. Without
    # it, go-merge could start once the fallback's single (worker, arm64)
    # digest exists, before the other 13 matrix artifacts from go-build do,
    # producing an incomplete manifest or a failed digest merge.
    assert "go-build" in needs, (
        f"{WORKFLOW_PATH.name}: go-merge no longer `needs:` 'go-build' -- "
        "without it, go-merge could start as soon as this pilot's single "
        "digest exists, before go-build's other matrix artifacts do"
    )


def _build_step(job_name: str) -> dict[str, object]:
    steps_raw = _job(job_name).get("steps") or []
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
    return matches[0]


def test_both_legs_build_step_with_maps_are_identical() -> None:
    # codex round 1 (#2180, CHAOS-4906), P2 guard false-negative, the
    # author's OWN least-sure line: the two legs are hand-duplicated step
    # sequences with no shared job template -- nothing before this test
    # pinned that their ACTUAL BUILD INPUTS (`with:`: context, file, target,
    # platforms, build-args, tags, outputs/push condition, cache-from,
    # cache-to) stay byte-for-byte equal. A future edit to one copy alone
    # (e.g. a build-arg, a cache setting) would silently make "which leg
    # actually ran" change what image gets built and pushed, with nothing
    # to catch it. `timeout-minutes` (covered by the test above) and the
    # fallback's own `if: steps.own.outputs.run_here == 'true'` ownership
    # guard are the two INTENTIONAL, known differences and are excluded
    # from this comparison on purpose, not by oversight.
    attempt_with = _build_step(_SELF_HOSTED_JOB).get("with")
    fallback_with = _build_step(_FALLBACK_JOB).get("with")
    assert isinstance(attempt_with, dict) and isinstance(fallback_with, dict), (
        f"{WORKFLOW_PATH.name}: one of the two legs' {_BUILD_STEP_NAME!r} "
        "steps has no `with:` block to compare"
    )
    assert attempt_with == fallback_with, (
        f"{WORKFLOW_PATH.name}: the two legs' {_BUILD_STEP_NAME!r} `with:` "
        f"blocks have diverged -- attempt={attempt_with!r} "
        f"fallback={fallback_with!r}. Either leg can be the one that "
        "actually does the real work; a divergence here means WHICH leg "
        "ran silently changes what gets built and pushed"
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
