"""`venue-oracles` must live in exactly one workflow, and that workflow must
have no path filter -- or the required check it produces can go permanently
unsatisfiable.

WHY THIS TEST EXISTS
---------------------
A workflow that is path-filtered can never satisfy a required status check
for a diff that touches none of its listed paths: the workflow never
triggers at all, so the required context never posts. GitHub then reports
the PR as `mergeStateStatus: BLOCKED` forever, with every other check green
-- not pending, not failed, simply never produced. No rebase fixes it,
because the workflow that would produce the context genuinely never runs
for that diff.

Deciding a required check's relevance in the workflow TRIGGER, rather than
inside an always-run job, produces exactly this class of unsatisfiable PR.
`venue-oracles` lives in its own workflow with no path filter, deciding
relevance INSIDE the job via `ci/go_relevance.py` instead -- the same
pattern `go-quality.yml` already uses for its own required check.

WHAT THIS PINS
---------------
1. The job id `venue-oracles` is declared in exactly one workflow file. A
   second declaration (e.g. someone re-adding it to `go.yml` "just for
   coverage") makes two workflows race to report one required context, with
   no guarantee which one branch protection honours.
2. That one workflow declares both a `pull_request` and a `push` trigger,
   and neither carries a `paths` filter. A filter reintroduced here silently
   reopens the hole this test exists to close -- and would do so quietly,
   since CI itself stays green for every diff that still matches the (now
   narrower) filter. A trigger removed outright is just as dangerous and
   just as silent.
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


def test_the_declaring_workflow_has_no_path_filter() -> None:
    """A `paths` filter here means a PR whose diff matches none of it never
    triggers this workflow, silently, because every OTHER check on such a PR
    stays green.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    document = yaml.safe_load(declaring.read_text(encoding="utf-8"))
    on_block = _on_block(document)

    # `merge_group` carries no `paths` concept in GitHub Actions (there is no
    # diff to filter against a merge-queue candidate), so only pull_request
    # and push are checked for a filter -- but ALL THREE trigger keys must be
    # present at all (checked separately below), since an absent trigger is
    # exactly as dangerous as a filtered one and just as silent.
    for event in ("pull_request", "push"):
        # A reproduced round: `if trigger is None: continue` here made every
        # test in this file still pass after DELETING the pull_request
        # trigger entirely -- the required check would then never fire on a
        # pull_request at all, invisibly, and this guard said nothing. The
        # trigger's ABSENCE is exactly as dangerous as a path filter on it,
        # so it must fail loud too, not be skipped as "nothing to check".
        assert event in on_block, (
            f"{declaring.name} declares no {event!r} trigger at all. "
            f"{JOB_ID!r} is a required status check and must always fire on "
            f"every {event}, or the required context silently never posts "
            "for that event class."
        )
        trigger = on_block[event]
        assert isinstance(trigger, dict), (
            f"{declaring.name}'s {event!r} trigger is {trigger!r}, not a "
            "mapping -- cannot check for a paths filter"
        )
        assert "paths" not in trigger and "paths-ignore" not in trigger, (
            f"{declaring.name}'s {event!r} trigger declares a path filter "
            f"({sorted(trigger)}). {JOB_ID!r} is a required status check: a "
            "path filter here means a PR whose diff matches none of it never "
            "triggers this workflow, the required context never posts, and "
            "the PR is permanently BLOCKED however green its other checks "
            "are. Decide relevance INSIDE the job (see ci/go_relevance.py), "
            "never in the trigger."
        )


def test_the_declaring_workflow_has_a_merge_group_trigger() -> None:
    """A reproduced round: deleting `merge_group` entirely left every other
    test in this file passing. A merge-queue run must produce this required
    context too, or a repo with the merge queue enabled cannot merge through
    it.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    on_block = _on_block(yaml.safe_load(declaring.read_text(encoding="utf-8")))
    assert "merge_group" in on_block, (
        f"{declaring.name} declares no 'merge_group' trigger. {JOB_ID!r} is a "
        "required status check and must fire on a merge-queue run too."
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
    base_sha_expr = ((relevance_steps[0].get("env") or {}).get("BASE_SHA")) or ""
    assert "github.event.before" in base_sha_expr, (
        f"the relevance step's BASE_SHA expression ({base_sha_expr!r}) does "
        "not fall back to github.event.before. Without it, a push event "
        "leaves BASE_SHA empty and ci/go_relevant_diff.sh falls back to "
        "HEAD^...HEAD, which only ever sees the last commit of a "
        "multi-commit push."
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
