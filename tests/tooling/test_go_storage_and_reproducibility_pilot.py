"""go.yml's storage-integration-shard and container-reproducibility
self-hosted-pool routing (CHAOS-4906, runner contract v1.6).

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

This file asserts the structural invariants a future edit could silently
break without any test noticing:
1. The hosted and self-hosted legs of both bundles are gated on
   COMPLEMENTARY conditions over the same variable (+ fork-PR exclusion)
   -- exactly one runs, never both, never neither.
2. Both legs of both bundles share the SAME `name:`.
3. Both self-hosted jobs depend on `dind-smoke-test` (a real `needs:`,
   not just an `if:`) -- both need a working Docker daemon (testcontainers
   for the shards, container builds for reproducibility).
4. Each self-hosted job's real-work step carries an inner `timeout`
   strictly shorter than its own job's `timeout-minutes` -- the
   contract's own "inner test timeouts strictly shorter" clause.
5. The shard bundle's two legs share the identical matrix source.
6. The `go-storage-integration` aggregator needs BOTH shard legs, and its
   own check treats "both skipped" as a routing bug, not a pass.
7. NEITHER self-hosted job's steps contain a poll/wait step (structural
   confirmation the v1.5.1 poller pattern was actually retired here, not
   left dangling alongside the new gate).
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
_SHARD_SELF_HOSTED = "go-storage-integration-shard-self-hosted"
_SHARD_AGGREGATOR = "go-storage-integration"
_SHARD_RUN_STEP = (
    "Run isolated storage integration shard ${{ matrix.target }} ${{ matrix.shard }}"
)

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


def test_bundles_are_gated_on_complementary_conditions() -> None:
    for hosted_name, self_hosted_name in (
        (_SHARD_HOSTED, _SHARD_SELF_HOSTED),
        (_REPRO_HOSTED, _REPRO_SELF_HOSTED),
    ):
        hosted_if = str(_job(hosted_name).get("if", ""))
        self_hosted_if = str(_job(self_hosted_name).get("if", ""))
        assert hosted_if == _HOSTED_CONDITION, (
            f"{WORKFLOW_PATH.name}: {hosted_name!r}'s `if:` ({hosted_if!r}) "
            f"does not match the expected hosted-fallback condition"
        )
        assert self_hosted_if == _SELF_HOSTED_CONDITION, (
            f"{WORKFLOW_PATH.name}: {self_hosted_name!r}'s `if:` "
            f"({self_hosted_if!r}) does not match the expected "
            "self-hosted-eligible condition"
        )


def test_bundles_share_one_stable_check_name() -> None:
    assert _job(_SHARD_HOSTED).get("name") == _job(_SHARD_SELF_HOSTED).get("name"), (
        f"{WORKFLOW_PATH.name}: {_SHARD_HOSTED!r} and {_SHARD_SELF_HOSTED!r} "
        "must share the same templated `name:` -- flipping "
        "SELF_HOSTED_RUNNERS must never rename the required check"
    )
    assert _job(_REPRO_HOSTED).get("name") == _job(_REPRO_SELF_HOSTED).get("name"), (
        f"{WORKFLOW_PATH.name}: {_REPRO_HOSTED!r} and {_REPRO_SELF_HOSTED!r} "
        "must share the same `name:`"
    )


def test_self_hosted_jobs_depend_on_dind_smoke_test() -> None:
    for job_name in (_SHARD_SELF_HOSTED, _REPRO_SELF_HOSTED):
        needs = _needs(_job(job_name))
        assert _DIND_JOB in needs, (
            f"{WORKFLOW_PATH.name}: {job_name!r} does not `needs:` "
            f"{_DIND_JOB!r} -- a real job dependency, not just an `if:`, "
            "since this leg needs a working Docker daemon"
        )


def test_shard_self_hosted_also_depends_on_plan() -> None:
    needs = _needs(_job(_SHARD_SELF_HOSTED))
    assert _PLAN_JOB in needs, (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED!r} must `needs:` "
        f"{_PLAN_JOB!r} too, got {needs!r}"
    )


def test_shard_matrix_sources_are_identical_between_legs() -> None:
    hosted_matrix = _dict_field(_job(_SHARD_HOSTED), "strategy").get("matrix")
    self_hosted_matrix = _dict_field(_job(_SHARD_SELF_HOSTED), "strategy").get("matrix")
    assert hosted_matrix == self_hosted_matrix, (
        f"{WORKFLOW_PATH.name}: {_SHARD_HOSTED!r} and {_SHARD_SELF_HOSTED!r} "
        f"have diverged matrix sources -- hosted={hosted_matrix!r} "
        f"self_hosted={self_hosted_matrix!r}"
    )


def test_self_hosted_real_work_has_a_shorter_inner_timeout() -> None:
    cases = [
        (_SHARD_SELF_HOSTED, _SHARD_RUN_STEP, 30),
        (_REPRO_SELF_HOSTED, _REPRO_RUN_STEP, 25),
    ]
    for job_name, step_name, expected_outer in cases:
        job = _job(job_name)
        job_timeout = job.get("timeout-minutes")
        assert job_timeout == expected_outer, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s timeout-minutes "
            f"({job_timeout!r}) does not match the contract's sizing "
            f"({expected_outer}m, from measured pool wall time)"
        )
        step = _step_by_name(job, step_name)
        run_text = step.get("run")
        assert isinstance(run_text, str), (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s {step_name!r} step has no `run:`"
        )
        inner = _inner_timeout_minutes(run_text)
        assert isinstance(job_timeout, int) and inner < job_timeout, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s inner timeout ({inner}m) "
            f"is not strictly shorter than its own timeout-minutes ({job_timeout}m)"
        )


def test_shard_aggregator_needs_both_legs() -> None:
    needs = _needs(_job(_SHARD_AGGREGATOR))
    assert _SHARD_HOSTED in needs and _SHARD_SELF_HOSTED in needs, (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r} must `needs:` both "
        f"{_SHARD_HOSTED!r} and {_SHARD_SELF_HOSTED!r}, got {needs!r}"
    )
    env = _dict_field(_job(_SHARD_AGGREGATOR), "env")
    assert "go-storage-integration-shard-self-hosted" in str(
        env.get("SHARD_SELF_HOSTED_RESULT", "")
    ), (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r}'s env does not read "
        f"{_SHARD_SELF_HOSTED!r}'s own `.result`"
    )
    run_text = str(
        _step_by_name(
            _job(_SHARD_AGGREGATOR),
            "Require the plan and whichever shard leg ran to have passed",
        ).get("run", "")
    )
    assert "skipped" in run_text and "skipped" in run_text.split("skipped", 1)[1], (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r}'s check must "
        "explicitly reject the both-legs-skipped case as a routing bug"
    )


def test_no_pick_runner_job_survives_anywhere_in_the_file() -> None:
    # CHAOS-4906 follow-up (the v1.6 pool-expansion PR, stacked on this
    # one): go-arm64-numeric-parity converted from the v1.5.1 poller to
    # v1.6 too, so the whole file is now poller-free -- promoted from the
    # narrower per-job check below (which this test's own history notes
    # deliberately did NOT make this file-wide claim, because it wasn't
    # true yet at that PR's tip).
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}
    jobs = document.get("jobs") or {}
    assert "pick-runner" not in jobs, (
        f"{WORKFLOW_PATH.name}: a 'pick-runner' job still exists -- the "
        "v1.5.1 fallback-poller pattern was supposed to be fully retired "
        "from this file"
    )


def test_neither_self_hosted_job_has_a_poll_step() -> None:
    # Structural confirmation the v1.5.1 fallback-poller pattern (pick-
    # runner, job_status()/gh api polling, steps.own.outputs.run_here) was
    # actually retired for THIS PR's two routed jobs, not left dangling
    # alongside the new gate.
    for job_name in (_SHARD_SELF_HOSTED, _REPRO_SELF_HOSTED):
        job = _job(job_name)
        needs = _needs(job)
        assert "pick-runner" not in needs, (
            f"{WORKFLOW_PATH.name}: {job_name!r} still `needs:` "
            "pick-runner -- contract v1.6 gates directly on "
            "vars.SELF_HOSTED_RUNNERS, no kill-switch intermediary job "
            "needed for this PR's own jobs"
        )
        job_if = str(job.get("if", ""))
        assert "pick-runner" not in job_if, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s `if:` references "
            f"pick-runner ({job_if!r}) -- contract v1.6 gates directly on "
            "vars.SELF_HOSTED_RUNNERS"
        )
        steps_raw = job.get("steps") or []
        assert isinstance(steps_raw, list)
        for step in steps_raw:
            assert isinstance(step, dict)
            run_text = str(step.get("run", ""))
            assert "job_status()" not in run_text, (
                f"{WORKFLOW_PATH.name}: {job_name!r} still has a poll "
                "script -- contract v1.6 retires the fallback poller"
            )
            assert "run_here" not in run_text, (
                f"{WORKFLOW_PATH.name}: {job_name!r} still references "
                "steps.own.outputs.run_here -- contract v1.6 retires the "
                "fallback poller's ownership-decision step"
            )
