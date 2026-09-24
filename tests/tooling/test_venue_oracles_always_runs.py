"""`venue-oracles` runs on main and by hand, never as a pull request check,
from exactly one workflow with no path filter.

WHY THIS TEST EXISTS
---------------------
The hosted venue-oracles job is not a PR gate. A PR proves parity by
running the venue oracle of the package it changes locally, before review;
the merge waits only on the normal CI. The job runs on every push to main
(its result there is informational for merges: a red run is fixed forward or
reverted, and no group roll starts until it is green) and on
workflow_dispatch.

WHAT THIS PINS
---------------
1. The job id `venue-oracles` is declared in exactly one workflow file, so
   one commit has exactly one `venue-oracles` context.
2. That workflow triggers on `push` to main and on `workflow_dispatch`, and
   on no pull request or merge-queue event. A `pull_request` trigger added
   back makes it a PR check again; a lost `push` trigger means main is never
   checked, silently.
3. The `push` trigger has no path filter: a filter skips the run on main
   with no record. Relevance is decided INSIDE the job (ci/go_relevance.py),
   which reports its skip.
4. The job has no job-level `if:` (GitHub reports a skipped job as Success).
5. The relevance step takes the push range from `github.event.before`, and
   a workflow_dispatch run (which has no push range) runs the full suite.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

JOB_ID = "venue-oracles"


def _on_block(document: dict) -> dict:
    """Return the workflow's trigger block.

    PyYAML parses the bare key `on` as the BOOLEAN True (YAML 1.1's
    yes/no/on/off), so `document["on"]` is a KeyError on these files.
    """
    return document.get(True) or document.get("on") or {}


def _workflows_declaring_job(job_id: str) -> list[Path]:
    declaring: list[Path] = []
    for path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            continue
        jobs = document.get("jobs") or {}
        if job_id in jobs:
            declaring.append(path)
    return declaring


def test_venue_oracles_job_exists() -> None:
    """Vacuity guard: a rename must fail loud, not pass by finding nothing."""
    declaring = _workflows_declaring_job(JOB_ID)
    assert declaring, (
        f"no workflow under {WORKFLOWS_DIR} declares a job named {JOB_ID!r}. "
        "Either the job was renamed (update JOB_ID here) or deleted (this "
        "whole test file should go with it) -- an empty result must not pass "
        "silently, since every assertion below is vacuous against nothing."
    )


def test_venue_oracles_is_declared_in_exactly_one_workflow() -> None:
    """A second declaration makes two workflows race to report one required
    context, with no guarantee which one branch protection honours.
    """
    declaring = _workflows_declaring_job(JOB_ID)
    names = [path.name for path in declaring]
    assert len(declaring) == 1, (
        f"job {JOB_ID!r} is declared in {len(declaring)} workflow(s): {names}. "
        "Two workflows producing the same required-check context race to "
        "report it, and which one branch protection honours is unspecified. "
        "Keep the job in a single workflow."
    )


def _job_definition(path: Path, job_id: str) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return (document.get("jobs") or {})[job_id]


def test_the_declaring_workflow_runs_on_main_and_by_hand_only() -> None:
    """The trigger set is exactly push (main) and workflow_dispatch."""
    (declaring,) = _workflows_declaring_job(JOB_ID)
    on_block = _on_block(yaml.safe_load(declaring.read_text(encoding="utf-8")))
    assert isinstance(on_block, dict), (
        f"{declaring.name}'s trigger block is {on_block!r}, not a mapping"
    )
    assert sorted(on_block) == ["push", "workflow_dispatch"], (
        f"{declaring.name} triggers on {sorted(on_block)}; it must trigger on "
        "exactly push and workflow_dispatch. A pull_request, "
        "pull_request_target or merge_group trigger makes it a PR check "
        "again; a missing push trigger means main is never checked."
    )
    push = on_block["push"]
    assert isinstance(push, dict), (
        f"{declaring.name}'s push trigger is {push!r}, not a mapping -- "
        "without a branches list it runs on every branch push"
    )
    assert push.get("branches") == ["main"], (
        f"{declaring.name}'s push trigger has branches {push.get('branches')!r}; "
        "it must be exactly ['main']"
    )
    assert not {"paths", "paths-ignore", "branches-ignore", "tags"} & set(push), (
        f"{declaring.name}'s push trigger declares a filter ({sorted(push)}). "
        "A path filter skips the run on main with no record; decide "
        "relevance INSIDE the job (see ci/go_relevance.py)."
    )


def test_the_job_carries_no_conditional_skip() -> None:
    """A reproduced round: a job-level `if: false` (or any other condition
    that can evaluate false) makes GitHub report the job `Success` while it
    ran nothing at all -- indistinguishable, in the merge UI, from a real
    pass, and just as silent as a path filter or a missing trigger. The job
    that produces a required context must have no `if:` of its own; every
    conditional decision belongs to individual STEPS inside it, which is
    exactly how this job already decides whether to run the real oracle
    suite or report an honest skip.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    job = _job_definition(declaring, JOB_ID)
    assert "if" not in job, (
        f"the {JOB_ID!r} job in {declaring.name} carries a job-level `if:` "
        f"({job.get('if')!r}). GitHub reports a conditionally-skipped job as "
        "Success, including when it is required -- indistinguishable from a "
        "real pass. Move any conditional logic into a STEP inside the job, "
        "never onto the job itself."
    )


def test_the_relevance_step_wires_the_push_fallback_sha() -> None:
    """`ci/go_relevant_diff.sh`'s own tests prove the SCRIPT resolves a
    multi-commit push correctly given the right BASE_SHA -- they hand it
    directly, never through this workflow's actual env expression. This pins
    the WIRING those tests cannot see: the workflow itself must actually
    supply `github.event.before` as the push-event fallback, or a push
    silently reverts to the single-commit `HEAD^...HEAD` case the script
    only falls back to when no BASE_SHA is set at all.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    job = _job_definition(declaring, JOB_ID)
    steps = job.get("steps") or []
    relevance_steps = [s for s in steps if s.get("id") == "relevance"]
    assert len(relevance_steps) == 1, (
        f"expected exactly one step with id: relevance in {declaring.name}'s "
        f"{JOB_ID!r} job, found {len(relevance_steps)}"
    )
    env = relevance_steps[0].get("env") or {}
    base_sha_expr = env.get("BASE_SHA") or ""
    assert base_sha_expr.replace(" ", "") == "${{github.event.before}}", (
        f"the relevance step's BASE_SHA expression ({base_sha_expr!r}) does "
        "not fall back to github.event.before. Without it, a push event "
        "leaves BASE_SHA empty and ci/go_relevant_diff.sh falls back to "
        "HEAD^...HEAD, which only ever sees the last commit of a "
        "multi-commit push."
    )


def test_a_dispatch_run_runs_the_full_suite() -> None:
    """A workflow_dispatch run has no push range: `github.event.before` is
    empty, and ci/go_relevant_diff.sh would fall back to HEAD^...HEAD and
    could skip the suite on a commit that changes no Go path. The relevance
    step must answer relevant=true for that event before any diff is read.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    job = _job_definition(declaring, JOB_ID)
    (step,) = [s for s in job.get("steps") or [] if s.get("id") == "relevance"]
    env = step.get("env") or {}
    assert (env.get("EVENT_NAME") or "").replace(" ", "") == "${{github.event_name}}", (
        f"the relevance step's EVENT_NAME is {env.get('EVENT_NAME')!r}, not "
        "${{ github.event_name }}"
    )
    script = step.get("run") or ""
    guard = 'if [ "${EVENT_NAME}" = "workflow_dispatch" ]; then'
    assert guard in script, (
        "the relevance step does not branch on workflow_dispatch before it "
        "reads the diff"
    )
    branch = script[script.index(guard) : script.index("fi", script.index(guard))]
    assert (
        "printf 'relevant=true\\n' >> \"${GITHUB_OUTPUT}\"" in branch
        and "exit 0" in branch
    ), (
        "the workflow_dispatch branch of the relevance step must write "
        "relevant=true and stop, so the full suite runs"
    )
    assert script.index(guard) < script.index("go_relevant_diff.sh"), (
        "the workflow_dispatch branch must come before the diff is read"
    )


def test_the_declaring_workflow_is_venue_oracles_yml() -> None:
    """Pin the expected home, so a future move is a deliberate rename of this
    test rather than a silent drift nobody notices.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    assert declaring.name == "venue-oracles.yml", (
        f"{JOB_ID!r} now lives in {declaring.name}, not venue-oracles.yml. "
        "If this is a deliberate move, update this pin in the same change."
    )


def test_go_yml_no_longer_declares_venue_oracles() -> None:
    """`go.yml` re-gaining this job would silently restore the path-filtered,
    permanently-unsatisfiable required check.
    """
    go_workflow = WORKFLOWS_DIR / "go.yml"
    document = yaml.safe_load(go_workflow.read_text(encoding="utf-8"))
    jobs = document.get("jobs") or {}
    assert JOB_ID not in jobs, (
        f"go.yml declares a job named {JOB_ID!r} again. go.yml is "
        "path-filtered by design (it does the real Go-relevant work); "
        f"{JOB_ID!r} is a REQUIRED check and must live only in an "
        "always-triggering workflow (see venue-oracles.yml)."
    )
