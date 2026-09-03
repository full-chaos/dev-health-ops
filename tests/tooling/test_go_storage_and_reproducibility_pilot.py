"""go.yml's storage-integration-shard and container-reproducibility
self-hosted-pool pilots (CHAOS-4906).

WHY THIS TEST EXISTS
---------------------
Same pick-runner/attempt/fallback/aggregator shape as go-arm64-numeric-
parity (#2145), applied to two more jobs: `go-storage-integration-shard`
(a MATRIX job, generalising the pattern to one attempt+fallback PAIR per
matrix leg) and `go-container-reproducibility` (a single job, the closest
shape to #2145's own pilot).

This file asserts the structural invariants a future edit could silently
break without any test noticing:
1. Each self-hosted attempt job depends on `pick-runner` (and, for the
   shard bundle, `go-storage-integration-plan` too) via a real `needs:`
   dependency, gated on `needs.pick-runner.outputs.try_self_hosted`.
2. Each fallback job's own build/test step carries an inner `timeout`
   shorter than its job's own `timeout-minutes` -- the double-bound
   lesson from #2145/#2180's own pilots (CF, 09-03, acr's near-miss).
3. Each aggregator (`go-storage-integration`, `go-container-
   reproducibility`) needs ONLY the fallback leg, never the self-hosted
   attempt directly -- a permanently `queued` attempt has no terminal
   state, and a `needs:` on it directly would block the aggregator for up
   to GitHub's 24h self-hosted backstop instead of the fallback's own
   bounded poll.
4. The shard bundle's self-hosted and fallback jobs share the IDENTICAL
   matrix source -- a future edit to one but not the other would silently
   run a different SET of shards through each leg.
5. Each fallback's poll script targets the CORRECT self-hosted job name
   (a fixed string for reproducibility, the matrix-templated own-shard
   name for the shard bundle) -- never a hardcoded name that would poll
   the wrong shard, or another job's name entirely.
6. The gh api query stays SAME-RUN shaped (F2 exemption, same reasoning
   as #2145's own guard) for both fallback poll scripts.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"

_PICK_RUNNER_JOB = "pick-runner"
_PLAN_JOB = "go-storage-integration-plan"

_SHARD_SELF_HOSTED = "go-storage-integration-shard-self-hosted"
_SHARD_FALLBACK = "go-storage-integration-shard-fallback"
_SHARD_AGGREGATOR = "go-storage-integration"
_SHARD_RUN_STEP = (
    "Run isolated storage integration shard ${{ matrix.target }} ${{ matrix.shard }}"
)

_REPRO_SELF_HOSTED = "go-container-reproducibility-self-hosted"
_REPRO_FALLBACK = "go-container-reproducibility-fallback"
_REPRO_AGGREGATOR = "go-container-reproducibility"
_REPRO_RUN_STEP = "Verify reproducible Go images"


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
    """`obj[key]` narrowed to a dict, or `{}` if absent -- avoids repeating
    an `isinstance` check at every `.get("strategy")`/`.get("env")` call
    site (mypy cannot narrow `object` to `dict` through a bare `.get()`)."""
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
        "no inner `timeout <N>m` found in run text -- relying on the "
        "job's own timeout-minutes alone reproduces acr's near-miss: a "
        "job cancelled by its own outer timeout is indistinguishable "
        "from a real failure through this poll contract"
    )
    return int(match.group(1))


def test_shard_self_hosted_depends_on_pick_runner_and_plan() -> None:
    needs = _needs(_job(_SHARD_SELF_HOSTED))
    assert _PICK_RUNNER_JOB in needs and _PLAN_JOB in needs, (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED!r} must `needs:` both "
        f"{_PICK_RUNNER_JOB!r} and {_PLAN_JOB!r}, got {needs!r}"
    )
    condition = str(_job(_SHARD_SELF_HOSTED).get("if", ""))
    assert "needs.pick-runner.outputs.try_self_hosted == 'true'" in condition, (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED!r}'s `if:` ({condition!r}) "
        "no longer gates on pick-runner's own decision"
    )


def test_repro_self_hosted_depends_on_pick_runner() -> None:
    needs = _needs(_job(_REPRO_SELF_HOSTED))
    assert _PICK_RUNNER_JOB in needs, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r} must `needs:` "
        f"{_PICK_RUNNER_JOB!r}, got {needs!r}"
    )
    condition = str(_job(_REPRO_SELF_HOSTED).get("if", ""))
    assert "needs.pick-runner.outputs.try_self_hosted == 'true'" in condition, (
        f"{WORKFLOW_PATH.name}: {_REPRO_SELF_HOSTED!r}'s `if:` ({condition!r}) "
        "no longer gates on pick-runner's own decision"
    )


def test_shard_matrix_sources_are_identical_between_legs() -> None:
    # codex round would find this the moment they diverge: the self-hosted
    # attempt and its fallback MUST enumerate the same shard set, or one
    # leg silently covers a different slice of the matrix than the other.
    self_hosted_matrix = _dict_field(_job(_SHARD_SELF_HOSTED), "strategy").get("matrix")
    fallback_matrix = _dict_field(_job(_SHARD_FALLBACK), "strategy").get("matrix")
    assert self_hosted_matrix == fallback_matrix, (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED!r} and {_SHARD_FALLBACK!r} "
        f"have diverged matrix sources -- self_hosted={self_hosted_matrix!r} "
        f"fallback={fallback_matrix!r}"
    )


def test_both_legs_of_both_bundles_have_a_shorter_inner_timeout() -> None:
    cases = [
        (_SHARD_SELF_HOSTED, _SHARD_RUN_STEP),
        (_SHARD_FALLBACK, _SHARD_RUN_STEP),
        (_REPRO_SELF_HOSTED, _REPRO_RUN_STEP),
        (_REPRO_FALLBACK, _REPRO_RUN_STEP),
    ]
    for job_name, step_name in cases:
        job = _job(job_name)
        job_timeout = job.get("timeout-minutes")
        assert isinstance(job_timeout, int), (
            f"{WORKFLOW_PATH.name}: job {job_name!r} has no integer timeout-minutes"
        )
        step = _step_by_name(job, step_name)
        run_text = step.get("run")
        assert isinstance(run_text, str), (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s {step_name!r} step has no `run:`"
        )
        inner = _inner_timeout_minutes(run_text)
        assert inner < job_timeout, (
            f"{WORKFLOW_PATH.name}: job {job_name!r}'s inner timeout ({inner}m) "
            f"is not strictly shorter than its own timeout-minutes ({job_timeout}m)"
        )


def test_shard_aggregator_needs_only_the_fallback() -> None:
    needs = _needs(_job(_SHARD_AGGREGATOR))
    assert _SHARD_FALLBACK in needs, (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r} does not `needs:` "
        f"{_SHARD_FALLBACK!r}"
    )
    assert _SHARD_SELF_HOSTED not in needs, (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r} `needs:` "
        f"{_SHARD_SELF_HOSTED!r} directly -- a permanently `queued` "
        "self-hosted attempt has no terminal state; this would block the "
        "aggregator for up to GitHub's 24h self-hosted backstop"
    )
    plan_result_ref = str(
        _dict_field(_job(_SHARD_AGGREGATOR), "env").get("SHARD_RESULT", "")
    )
    assert "go-storage-integration-shard-fallback" in plan_result_ref, (
        f"{WORKFLOW_PATH.name}: {_SHARD_AGGREGATOR!r}'s SHARD_RESULT env "
        f"({plan_result_ref!r}) does not read the fallback matrix's own "
        "aggregate `.result`"
    )


def test_repro_aggregator_needs_only_the_fallback() -> None:
    needs = _needs(_job(_REPRO_AGGREGATOR))
    assert _REPRO_FALLBACK in needs, (
        f"{WORKFLOW_PATH.name}: {_REPRO_AGGREGATOR!r} does not `needs:` "
        f"{_REPRO_FALLBACK!r}"
    )
    assert _REPRO_SELF_HOSTED not in needs, (
        f"{WORKFLOW_PATH.name}: {_REPRO_AGGREGATOR!r} `needs:` "
        f"{_REPRO_SELF_HOSTED!r} directly -- same hazard as the shard "
        "bundle's aggregator"
    )


def test_fallback_poll_scripts_target_the_correct_job_name() -> None:
    shard_wait = _step_by_name(
        _job(_SHARD_FALLBACK),
        "Determine this shard's self-hosted attempt outcome, or that it never left the queue",
    )
    shard_env = _dict_field(shard_wait, "env")
    assert (
        shard_env.get("TARGET_JOB_NAME")
        == "go-storage-integration-shard-self-hosted-${{ matrix.target }}-${{ matrix.shard }}"
    ), (
        f"{WORKFLOW_PATH.name}: {_SHARD_FALLBACK!r}'s wait step targets "
        f"{shard_env.get('TARGET_JOB_NAME')!r}, which does not match "
        f"{_SHARD_SELF_HOSTED!r}'s own templated `name:` -- the poll would "
        "look for the wrong job"
    )
    self_hosted_name = _job(_SHARD_SELF_HOSTED).get("name")
    assert self_hosted_name == shard_env.get("TARGET_JOB_NAME"), (
        f"{WORKFLOW_PATH.name}: {_SHARD_SELF_HOSTED!r}'s own `name:` "
        f"({self_hosted_name!r}) no longer matches what "
        f"{_SHARD_FALLBACK!r}'s poll script searches for"
    )

    repro_wait = _step_by_name(
        _job(_REPRO_FALLBACK),
        "Determine the self-hosted attempt's outcome, or that it never left the queue",
    )
    repro_run = str(repro_wait.get("run", ""))
    assert 'select(.name=="go-container-reproducibility-self-hosted")' in repro_run, (
        f"{WORKFLOW_PATH.name}: {_REPRO_FALLBACK!r}'s poll script does not "
        f"filter on {_REPRO_SELF_HOSTED!r}'s own job name"
    )


def test_fallback_poll_scripts_stay_same_run_shaped() -> None:
    # Same F2-exemption reasoning as #2145's own guard
    # (test_go_arm64_runner_fallback_same_run_lookup.py): these queries are
    # scoped to actions/runs/${GITHUB_RUN_ID}/jobs (SAME run), never a
    # cross-run search or a head_sha= discriminator.
    for job_name, step_name in (
        (
            _SHARD_FALLBACK,
            "Determine this shard's self-hosted attempt outcome, or that it never left the queue",
        ),
        (
            _REPRO_FALLBACK,
            "Determine the self-hosted attempt's outcome, or that it never left the queue",
        ),
    ):
        run_text = str(_step_by_name(_job(job_name), step_name).get("run", ""))
        assert "actions/runs/${GITHUB_RUN_ID}/jobs" in run_text, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s poll script no longer "
            "queries the same-run job list"
        )
        assert "actions/runs?" not in run_text, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s poll script now contains "
            "a cross-run search -- the F2 same-run exemption needs "
            "re-examination"
        )
        assert "head_sha=" not in run_text, (
            f"{WORKFLOW_PATH.name}: {job_name!r}'s poll script now "
            "references head_sha= -- the discriminator a cross-run design "
            "would need and this same-run design must not have"
        )
