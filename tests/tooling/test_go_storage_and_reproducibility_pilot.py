"""go.yml's go-container-reproducibility self-hosted-pool routing
(CHAOS-4906, runner contract v1.6) -- and confirmation that
go-storage-integration-shard was reverted to hosted-only.

WHY THIS TEST EXISTS
---------------------
Contract v1.6 (signed 2026-09-03, chris via CF team-lead; supersedes
v1.5.1's fallback-poller pattern for routed jobs, see
/Users/chris/projects/full-chaos/dev-health/.remember/lanes/
lane-4904-acr-runners/contract-v1.6-signed.md): when SELF_HOSTED_RUNNERS
is on, the self-hosted job IS the required check and the aggregator
`needs` it directly; the hosted job runs only when the variable is off
(or the event is a fork PR). Exactly one of the two runs per workflow
run. Both legs share one stable `name:`, so flipping the variable never
leaves a stuck or renamed required check.

go-storage-integration-shard was ALSO routed through this pattern in an
earlier version of this PR, then REVERTED (chris ruling 09-03, via CF):
5 of its 6 shard legs timed out at the pool's 25m inner cap under real
co-tenancy (5-8 concurrent pods across ops+acr, two bigboy-native
suites, and a codex round all sharing one 16-core box), measured against
the SAME shards' consistent ~12min wall on hosted runners across 6 green
runs (11m27s-12m44s, never close to 20min) -- a genuine 2x+ contention
penalty, not a cache-cold or timeout-sizing problem (GOMODCACHE/GOCACHE
were confirmed warm throughout, zero `go: downloading` lines in any
failed shard's log). The pool has no additional cores to give this
suite; go-container-reproducibility and its dind-smoke-test precondition
stay on the pool (both passed clean on the same warm-cache run, well
under their own caps, and are not matrix/concurrency-sensitive the way
six simultaneous shard legs are).

This file asserts the structural invariants a future edit could silently
break without any test noticing:
1. go-container-reproducibility's hosted and self-hosted legs are gated
   on COMPLEMENTARY conditions over the same variable (+ fork-PR
   exclusion) -- exactly one runs, never both, never neither.
2. Both legs share the SAME `name:`.
3. The self-hosted leg depends on `dind-smoke-test` (a real `needs:`,
   not just an `if:`) -- it needs a working Docker daemon for container
   builds.
4. Its real-work step carries an inner `timeout` strictly shorter than
   its own job's `timeout-minutes` -- the contract's own "inner test
   timeouts strictly shorter" clause.
5. It has no leftover v1.5.1 poller-pattern step.
6. go-storage-integration-shard has NO `if:` gating (always runs, plain
   hosted job) and NO self-hosted twin exists -- the revert actually
   landed, not left half-done.
7. The go-storage-integration aggregator needs exactly plan + the single
   hosted shard job, nothing else.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"

_DIND_JOB = "dind-smoke-test"
_PLAN_JOB = "go-storage-integration-plan"

_SHARD_HOSTED = "go-storage-integration-shard"
_SHARD_SELF_HOSTED_REMOVED = "go-storage-integration-shard-self-hosted"
_SHARD_AGGREGATOR = "go-storage-integration"

_REPRO_HOSTED = "go-container-reproducibility"
_REPRO_SELF_HOSTED = "go-container-reproducibility-self-hosted"
_REPRO_RUN_STEP = "Verify reproducible Go images"

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


def _document() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}


def _jobs() -> dict[str, object]:
    jobs = _document().get("jobs")
    assert isinstance(jobs, dict), (
        f"{WORKFLOW_PATH.name}: top-level 'jobs' is not a mapping"
    )
    return jobs


def _job(name: str) -> dict[str, object]:
    job = _jobs().get(name)
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


def _step_by_name(job: dict[str, object], name: str) -> dict[str, object]:
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    matches = [s for s in steps_raw if isinstance(s, dict) and s.get("name") == name]
    assert len(matches) == 1, (
        f"{WORKFLOW_PATH.name}: must have exactly one step named {name!r} "
        f"(found {len(matches)})"
    )
    return matches[0]


def _inner_timeout_minutes(run_text: str) -> int:
    match = re.search(r"\btimeout (\d+)m\b", run_text)
    assert match, (
        "no inner `timeout <N>m` found in run text -- the contract's own "
        "'inner test timeouts strictly shorter' clause needs one"
    )
    return int(match.group(1))


def test_reproducibility_bundle_is_gated_on_complementary_conditions() -> None:
    hosted_if = str(_job(_REPRO_HOSTED).get("if", ""))
    self_hosted_if = str(_job(_REPRO_SELF_HOSTED).get("if", ""))
    assert hosted_if == _HOSTED_CONDITION, (
        f"{WORKFLOW_PATH.name}: {_REPRO_HOSTED!r}'s `if:` ({hosted_if!r}) "
        f"does not match the expected hosted-fallback condition"
    )
    assert self_hosted_if == _SELF_HOSTED_CONDITION, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s `if:` "
        f"({self_hosted_if!r}) does not match the expected "
        "self-hosted-eligible condition"
    )


def test_reproducibility_bundle_shares_one_stable_check_name() -> None:
    assert _job(_REPRO_HOSTED).get("name") == _job(_REPRO_SELF_HOSTED).get("name"), (
        f"{WORKFLOW_PATH.name}: {_REPRO_HOSTED!r} and {_REPRO_SELF_HOSTED!r} "
        "must share the same `name:` -- flipping SELF_HOSTED_RUNNERS must "
        "never rename the required check"
    )


def test_reproducibility_self_hosted_depends_on_dind_smoke_test() -> None:
    needs = _needs(_job(_REPRO_SELF_HOSTED))
    assert _DIND_JOB in needs, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r} does not `needs:` "
        f"{_DIND_JOB!r} -- a real job dependency, not just an `if:`, since "
        "this leg needs a working Docker daemon"
    )


def test_reproducibility_self_hosted_real_work_has_a_shorter_inner_timeout() -> None:
    job = _job(_REPRO_SELF_HOSTED)
    job_timeout = job.get("timeout-minutes")
    assert job_timeout == 36, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s timeout-minutes "
        f"({job_timeout!r}) does not match the contract's sizing (36m --  "
        "measured 18m03s isolated wall x2, run 33822145677, per the "
        "contract's own 'timeout-minutes = measured wall under load x2' "
        "clause)"
    )
    step = _step_by_name(job, _REPRO_RUN_STEP)
    run_text = step.get("run")
    assert isinstance(run_text, str), (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s {_REPRO_RUN_STEP!r} "
        "step has no `run:`"
    )
    inner = _inner_timeout_minutes(run_text)
    assert isinstance(job_timeout, int) and inner < job_timeout, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s inner timeout "
        f"({inner}m) is not strictly shorter than its own timeout-minutes "
        f"({job_timeout}m)"
    )


def test_pool_jobs_have_no_job_level_concurrency_group() -> None:
    # CHAOS-4906 (09-04): job-level `concurrency:` groups were added to
    # these two jobs the same day to serialize pool access, then REVERTED
    # a few hours later. GitHub's concurrency semantics allow at most ONE
    # running AND ONE pending job per group; a THIRD event's job then
    # cancels the still-pending SECOND one, `cancel-in-progress: false`
    # notwithstanding (that setting only protects a job already RUNNING,
    # never one waiting behind it). Measured: with #2197/#2199/#2192
    # pushing minutes apart, #2197's go-container-reproducibility -- a
    # REQUIRED check -- was cancelled at 2m40s by this exact mechanism,
    # turning a green check red. A future edit re-adding a job-level
    # `concurrency:` block to either job would silently reintroduce this;
    # this test exists so that edit fails loud instead.
    for job_name in (_REPRO_SELF_HOSTED, _DIND_JOB):
        job = _job(job_name)
        assert "concurrency" not in job, (
            f"{WORKFLOW_PATH.name}: {job_name!r} has a job-level "
            f"`concurrency:` block ({job.get('concurrency')!r}) -- this "
            "was tried and reverted 09-04 (a required check got cancelled "
            "at 2m40s by GitHub's one-running-one-pending-per-group rule); "
            "see this test's docstring before re-adding it"
        )


def test_reproducibility_self_hosted_has_no_poll_step() -> None:
    # Structural confirmation the v1.5.1 fallback-poller pattern (pick-
    # runner, job_status()/gh api polling, steps.own.outputs.run_here) was
    # actually retired for this job, not left dangling alongside the new
    # gate. This does not assert the poller is gone from the whole file --
    # see test_no_pick_runner_job_survives_anywhere_in_the_file below for
    # that file-wide claim, which only became true once the stacked v1.6
    # pool-expansion PR also converted go-arm64-numeric-parity.
    job = _job(_REPRO_SELF_HOSTED)
    needs = _needs(job)
    assert "pick-runner" not in needs, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r} still `needs:` "
        "pick-runner -- contract v1.6 gates directly on "
        "vars.SELF_HOSTED_RUNNERS, no kill-switch intermediary job needed"
    )
    job_if = str(job.get("if", ""))
    assert "pick-runner" not in job_if, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s `if:` references "
        f"pick-runner ({job_if!r}) -- contract v1.6 gates directly on "
        "vars.SELF_HOSTED_RUNNERS"
    )
    steps_raw = job.get("steps") or []
    assert isinstance(steps_raw, list)
    for step in steps_raw:
        assert isinstance(step, dict)
        run_text = str(step.get("run", ""))
        assert "job_status()" not in run_text, (
            f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r} still has a "
            "poll script -- contract v1.6 retires the fallback poller"
        )
        assert "run_here" not in run_text, (
            f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r} still "
            "references steps.own.outputs.run_here -- contract v1.6 "
            "retires the fallback poller's ownership-decision step"
        )


def test_no_pick_runner_job_survives_anywhere_in_the_file() -> None:
    # CHAOS-4906 follow-up (the v1.6 pool-expansion PR, stacked on this
    # one): go-arm64-numeric-parity converted from the v1.5.1 poller to
    # v1.6 too, so the whole file is now poller-free -- promoted from the
    # narrower per-job check above (which deliberately did NOT make this
    # file-wide claim while go-arm64-numeric-parity still used the
    # poller).
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}
    jobs = document.get("jobs") or {}
    assert "pick-runner" not in jobs, (
        f"{WORKFLOW_PATH.name}: a 'pick-runner' job still exists -- the "
        "v1.5.1 fallback-poller pattern was supposed to be fully retired "
        "from this file"
    )


def test_storage_integration_shard_is_hosted_only() -> None:
    """The revert (chris ruling 09-03) must actually be in the file, not
    just in a commit message: go-storage-integration-shard runs
    unconditionally (no SELF_HOSTED_RUNNERS gate) and no self-hosted twin
    exists anywhere in go.yml."""
    job = _job(_SHARD_HOSTED)
    assert "if" not in job, (
        f"{WORKFLOW_PATH.name}: {_SHARD_HOSTED!r} still carries an `if:` "
        f"({job.get('if')!r}) -- the revert to hosted-only means this job "
        "always runs, no SELF_HOSTED_RUNNERS gate"
    )
    assert job.get("runs-on") == "ubuntu-latest", (
        f"{WORKFLOW_PATH.name}: {_SHARD_HOSTED!r} must run on "
        f"ubuntu-latest, got {job.get('runs-on')!r}"
    )
    assert _SHARD_SELF_HOSTED_REMOVED not in _jobs(), (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED_REMOVED!r} still "
        "exists -- the revert to hosted-only removes this job entirely, "
        "it does not just disable it"
    )


def test_shard_aggregator_needs_exactly_plan_and_hosted_shard() -> None:
    needs = _needs(_job(_SHARD_AGGREGATOR))
    assert set(needs) == {_PLAN_JOB, _SHARD_HOSTED}, (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r} must `needs:` "
        f"exactly {{{_PLAN_JOB!r}, {_SHARD_HOSTED!r}}} now that the "
        f"self-hosted leg is reverted, got {needs!r}"
    )
