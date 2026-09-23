"""`venue-oracles` must live in exactly one workflow, and that workflow must
have no path filter -- or the required check it produces can go permanently
unsatisfiable.

WHY THIS TEST EXISTS (CHAOS-6337)
----------------------------------
`venue-oracles` started as a job inside `go.yml`, whose `pull_request`/`push`
triggers are path-filtered to Go-relevant paths. Once #2847 (CHAOS-6314)
promoted it to a REQUIRED status check on main, that path filter became a
trap: a PR touching none of go.yml's listed paths (a Helm chart, a shell CI
fixture, a docs page) never triggers go.yml at all, so the required
`venue-oracles` context never posts. GitHub reports the PR as
`mergeStateStatus: BLOCKED` forever, with every other check green -- not
pending, not failed, simply never produced. No rebase fixes it, because the
workflow that would produce the context genuinely never runs for that diff.
Reproduced live on #2853.

Same root cause class as CHAOS-4834 (`go-quality`): deciding a required
check's relevance in the workflow TRIGGER, rather than inside an always-run
job, produces PRs the check can never satisfy. The fix moved `venue-oracles`
into its own workflow (`venue-oracles.yml`) with no path filter, deciding
relevance INSIDE the job via `ci/go_relevance.py` instead -- exactly
`go-quality.yml`'s already-proven pattern.

WHAT THIS PINS
---------------
1. The job id `venue-oracles` is declared in exactly one workflow file. A
   second declaration (e.g. someone re-adding it to `go.yml` "just for
   coverage") reproduces CHAOS-4834's OTHER failure mode: two runners racing
   to report one required context, with no guarantee which one branch
   protection honours.
2. That one workflow's `pull_request` and `push` triggers carry no `paths`
   filter. A filter reintroduced here silently reopens the exact hole this
   ticket exists to close -- and would do so quietly, since CI itself stays
   green for every diff that still matches the (now narrower) filter.
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
    """A second declaration reproduces CHAOS-4834's duplicate-context failure."""
    declaring = _workflows_declaring_job(JOB_ID)
    names = [path.name for path in declaring]
    assert len(declaring) == 1, (
        f"job {JOB_ID!r} is declared in {len(declaring)} workflow(s): {names}. "
        "Two workflows producing the same required-check context race to "
        "report it, and which one branch protection honours is unspecified -- "
        "that is the exact CHAOS-4834 failure this repo already paid for once, "
        "in go-quality's history. Keep the job in a single workflow."
    )


def test_the_declaring_workflow_has_no_path_filter() -> None:
    """A `paths` filter here reopens CHAOS-6337: the required check goes dark
    on any PR whose diff misses the (now narrower) filter, silently, because
    every OTHER check on such a PR stays green.
    """
    (declaring,) = _workflows_declaring_job(JOB_ID)
    document = yaml.safe_load(declaring.read_text(encoding="utf-8"))
    on_block = _on_block(document)

    for event in ("pull_request", "push"):
        trigger = on_block.get(event)
        if trigger is None:
            continue
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
    """The specific regression this ticket fixes: `go.yml` re-gaining the job
    would silently restore the path-filtered, permanently-unsatisfiable
    required check.
    """
    go_workflow = WORKFLOWS_DIR / "go.yml"
    document = yaml.safe_load(go_workflow.read_text(encoding="utf-8"))
    jobs = document.get("jobs") or {}
    assert JOB_ID not in jobs, (
        f"go.yml declares a job named {JOB_ID!r} again. go.yml is "
        "path-filtered by design (it does the real Go-relevant work); "
        f"{JOB_ID!r} is a REQUIRED check and must live only in an "
        "always-triggering workflow (see venue-oracles.yml and CHAOS-6337)."
    )
