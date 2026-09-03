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

import pytest
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


def _the_gh_api_line(script: str) -> str:
    """The one physical line that actually performs the API request.

    codex round 1 (#2145, CHAOS-4906): the ORIGINAL version of this test
    searched the WHOLE script for the same-run pattern and for the absence
    of the two cross-run markers -- which a same-run-shaped string sitting
    in an UNUSED variable elsewhere in the script satisfies trivially, while
    the REAL request (built from a different variable, on a different line)
    can be a genuine cross-run lookup. Reproduced: a constructed script with
    `same_run_probe="actions/runs/${GITHUB_RUN_ID}/jobs"` (dead, never
    passed to `gh api`) alongside a real `gh api ... actions/workflows/
    go.yml/runs -f "${field_name}=${GITHUB_SHA}"` call (a genuine cross-run
    search, `field_name` holding the literal text `head_sha` only through a
    variable, so it never appears as the substring `head_sha=`) satisfied
    all three of the old whole-script checks. Scoping every check to the
    single line that actually invokes `gh api` closes this: a same-run
    string not on that line no longer counts, and the real request's own
    URL is what gets checked, not a decoy elsewhere in the script.

    Matches on `gh api` followed by a quote or a `--` flag (how every real
    invocation is shaped: `gh api "repos/...` or `gh api --method GET
    "repos/...`) rather than a bare substring search -- this script's own
    COMMENTS mention "gh api" repeatedly (documenting past incidents), and
    its error messages mention "(gh api kept failing)" in prose, both of
    which would otherwise inflate the match count and mask the one real
    invocation entirely. Verified against this exact script: a bare
    substring search matches 10 lines (comments + prose); this pattern
    matches exactly the 1 real invocation.

    Does NOT handle a `gh api` invocation split across a line continuation
    (`\\` at end of line) -- the production script has exactly one `gh api`
    call, on one physical line, today; a future multi-line form would need
    this widened, not silently miscounted.
    """
    invocation_pattern = re.compile(r'gh api\s+(?:"|--)')
    matches = [line for line in script.splitlines() if invocation_pattern.search(line)]
    assert len(matches) == 1, (
        f"{WORKFLOW_PATH.name}: job {JOB_NAME!r} step {STEP_ID!r} has "
        f"{len(matches)} lines containing 'gh api', expected exactly 1 -- "
        "this guard scopes its checks to THE line that performs the "
        "request; update it (and this test) if the step now makes more "
        "than one API call, rather than silently checking only the first"
    )
    return matches[0]


_SAME_RUN_PATTERN = re.compile(
    r"actions/runs/\S*(?:GITHUB_RUN_ID|github\.run_id)\S*/jobs"
)


def _assert_same_run_shaped(script: str) -> None:
    """The guard's actual logic, factored out so a regression test can feed
    it a CONSTRUCTED script (the codex round 1 evasion) without needing a
    second copy of go.yml to point it at."""
    api_line = _the_gh_api_line(script)

    # Must query THIS run's own job list. `actions/runs/<run-id-expr>/jobs`
    # covers both the literal ${{ github.run_id }} and env-var forms
    # (${GITHUB_RUN_ID}) a shell script would actually use. Checked against
    # the ACTUAL API-invoking line, not the whole script -- see
    # _the_gh_api_line's docstring for why that distinction is load-bearing.
    assert _SAME_RUN_PATTERN.search(api_line), (
        f"job {JOB_NAME!r} step {STEP_ID!r}'s actual `gh api` call "
        f"({api_line!r}) is not a same-run actions/runs/<this run's id>/"
        "jobs lookup -- if this moved to a different lookup shape, the "
        "F2/F5 same-run exemption recorded next to job_status() needs "
        "re-examination, not silent removal"
    )

    # Must NOT be a cross-run search (`actions/runs?...`, which resolves a
    # DIFFERENT run by query parameters) or carry a head_sha discriminator
    # anywhere on that line -- either one is exactly the cross-run shape F2
    # exists for.
    assert "actions/runs?" not in api_line, (
        f"job {JOB_NAME!r} step {STEP_ID!r}'s `gh api` call now contains a "
        "cross-run search (`actions/runs?...`) -- this is the F2-relevant "
        "design change the same-run exemption comment warns about; "
        "re-apply the v1.5.1 staleness-discrimination fixtures rather than "
        "keeping the exemption comment"
    )
    assert "head_sha=" not in api_line, (
        f"job {JOB_NAME!r} step {STEP_ID!r}'s `gh api` call now references "
        "head_sha= -- the discriminator a cross-run candidate lookup needs "
        "and this same-run design must not have; re-examine the F2 "
        "exemption comment next to job_status() rather than keeping it "
        "as-is"
    )


def test_job_status_query_is_same_run_not_cross_run() -> None:
    _assert_same_run_shaped(_wait_step_script())


def test_guard_rejects_a_dead_decoy_beside_a_real_cross_run_call() -> None:
    """codex round 1 (#2145, CHAOS-4906), reproduced as a permanent
    regression: a same-run-shaped string sitting in a variable that is
    NEVER passed to `gh api`, beside a REAL `gh api` call that is a genuine
    cross-run search, satisfied every check the guard had before this
    fix -- because those checks searched the whole script text, and a dead
    decoy string is indistinguishable from a real query to a substring
    search. `field_name` is deliberately a separate variable so the literal
    text `head_sha=` never appears verbatim either -- the OLD guard's third
    check passed on this exact construction too. This must now raise,
    because `_assert_same_run_shaped` scopes every check to the one real
    `gh api` invocation, which is the cross-run `actions/workflows/go.yml/
    runs` endpoint."""
    decoy_script = (
        'same_run_probe="actions/runs/${GITHUB_RUN_ID}/jobs"\n'
        "field_name=head_sha\n"
        'raw=$(gh api --method GET "repos/${GITHUB_REPOSITORY}'
        '/actions/workflows/go.yml/runs" -f "${field_name}=${GITHUB_SHA}")\n'
    )
    with pytest.raises(AssertionError, match="not a same-run"):
        _assert_same_run_shaped(decoy_script)
