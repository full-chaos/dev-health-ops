"""go.yml's self-hosted fallback must stay a SAME-RUN job lookup.

WHY THIS TEST EXISTS
---------------------
Runner-routing contract v1.4/v1.5.1's F2 clause governs staleness
discrimination for a CROSS-RUN candidate lookup: given several jobs that
might match (from different runs or a re-run of the same run), pick the one
that is genuinely "ours" using a created_at floor, with a run_attempt > 1
sub-clause so a re-run doesn't silently adopt a stale attempt-1 sibling's
result. acr's design (CHAOS-4904) needs this because it resolves a
DIFFERENT run by head SHA + workflow name + event + ref before polling it.

This file's `job_status()` (in the `go-arm64-numeric-parity-fallback` job's
`wait` step -- the fallback leg polls the self-hosted leg's OWN job entry
in this same run) does not do that. It queries
`repos/{repo}/actions/runs/${GITHUB_RUN_ID}/jobs` -- SAME run, by its own
run id -- and relies on GitHub's documented `filter=latest` default on that
endpoint to exclude a prior attempt's jobs before the response ever reaches
this script. There is no candidate LIST to discriminate: a re-run of this
exact run never returns the old attempt's job at all, so F2's timestamp-floor
logic (and F5's tie-break, for the same reason -- nothing to sort) has
nothing to do here. v1.4 already recorded this exemption; v1.5.1's added
re-run sub-clause doesn't change it either, checked again 2026-09-03.

An exemption that is only ever asserted in a comment rots the moment the
code it describes changes underneath it. This test is the guard: it reads
the actual `run:` block and asserts the query stays same-run-shaped. If a
future refactor moves this toward a cross-run design (a `head_sha=` query
parameter, or an `actions/runs?...` search endpoint instead of
`actions/runs/{id}/jobs`), this test fails -- which is exactly the signal to
re-examine whether the F2 exemption above still holds, rather than carrying
a stale comment forward silently.

WHAT THIS ASSERTS
-----------------
The `run:` block of the `go-arm64-numeric-parity-fallback` job's `wait` step:
1. Queries `actions/runs/${GITHUB_RUN_ID}/jobs` (or an equivalent same-run
   job-list path) -- not `actions/runs?` (a cross-run search).
2. Contains no `head_sha=` query parameter anywhere -- the discriminator a
   cross-run design would need and this one must not have.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"
JOB_NAME = "go-arm64-numeric-parity-fallback"
STEP_ID = "wait"


def _wait_step_script() -> str:
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}
    job = (document.get("jobs") or {}).get(JOB_NAME)
    assert job is not None, (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} not found -- this test's "
        "target job was renamed or removed; update JOB_NAME above rather "
        "than letting this assertion go stale"
    )
    steps = job.get("steps") or []
    matches = [s for s in steps if s.get("id") == STEP_ID]
    assert len(matches) == 1, (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} must have exactly one step "
        f"with id {STEP_ID!r} (found {len(matches)}) -- this test's target "
        "step was renamed, duplicated, or removed"
    )
    script = matches[0].get("run")
    assert isinstance(script, str) and script.strip(), (
        f"{WORKFLOW_PATH.name}: step {STEP_ID!r} in job {JOB_NAME!r} has no "
        "run: script to inspect"
    )
    return script


def test_job_status_query_is_same_run_not_cross_run() -> None:
    script = _wait_step_script()

    # Must query THIS run's own job list. `actions/runs/<run-id-expr>/jobs`
    # covers both the literal ${{ github.run_id }} and env-var forms
    # (${GITHUB_RUN_ID}) a shell script would actually use.
    same_run_pattern = re.compile(
        r"actions/runs/\S*(?:GITHUB_RUN_ID|github\.run_id)\S*/jobs"
    )
    assert same_run_pattern.search(script), (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} step {STEP_ID!r} no longer "
        "queries actions/runs/<this run's id>/jobs -- if this moved to a "
        "different lookup shape, the F2/F5 same-run exemption recorded next "
        "to job_status() needs re-examination, not silent removal"
    )

    # Must NOT be a cross-run search (`actions/runs?...`, which resolves a
    # DIFFERENT run by query parameters) or carry a head_sha discriminator
    # anywhere -- either one is exactly the cross-run shape F2 exists for.
    assert "actions/runs?" not in script, (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} step {STEP_ID!r} now "
        "contains a cross-run search (`actions/runs?...`) -- this is the "
        "F2-relevant design change the same-run exemption comment warns "
        "about; re-apply the v1.5.1 staleness-discrimination fixtures "
        "rather than keeping the exemption comment"
    )
    assert "head_sha=" not in script, (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} step {STEP_ID!r} now "
        "references head_sha= -- the discriminator a cross-run candidate "
        "lookup needs and this same-run design must not have; re-examine "
        "the F2 exemption comment next to job_status() rather than keeping "
        "it as-is"
    )
