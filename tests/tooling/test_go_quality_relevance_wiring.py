"""`go-quality.yml`'s relevance step must actually supply the push-event
BASE_SHA fallback, and must use the NUL-safe diff producer.

WHY THIS TEST EXISTS
---------------------
`ci/go_relevant_diff.sh`'s own tests prove the SCRIPT resolves a multi-commit
push correctly given the right BASE_SHA -- they hand it directly, never
through a real workflow's env expression. Neither that suite nor
`ci/go_relevance.py`'s own tests can see a regression in a WORKFLOW'S wiring:
a future edit that drops `github.event.before` from this step's `BASE_SHA`
expression, or reverts the diff producer to a plain `git diff --name-only`,
would silently reopen the multi-commit-push and rename/non-ASCII-path gaps a
prior fix closed for `venue-oracles.yml`'s twin step -- the shared script
would still be correct, but nothing would call it correctly.

This pins the WORKFLOW's own wiring, the one thing the shared script's tests
cannot reach.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GO_QUALITY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "go-quality.yml"

# CHAOS-6690: the work runs in the matrix job `go-quality-leg`; `go-quality` is the
# fan-in that keeps the required context name.
JOB_ID = "go-quality-leg"


def _job_definition() -> dict:
    document = yaml.safe_load(GO_QUALITY_WORKFLOW.read_text(encoding="utf-8"))
    return (document.get("jobs") or {})[JOB_ID]


def test_the_relevance_step_wires_the_push_fallback_sha() -> None:
    job = _job_definition()
    steps = job.get("steps") or []
    relevance_steps = [s for s in steps if s.get("id") == "relevance"]
    assert len(relevance_steps) == 1, (
        f"expected exactly one step with id: relevance in {GO_QUALITY_WORKFLOW.name}'s "
        f"{JOB_ID!r} job, found {len(relevance_steps)}"
    )
    base_sha_expr = ((relevance_steps[0].get("env") or {}).get("BASE_SHA")) or ""
    # A reproduced round: `"github.event.before" in base_sha_expr` is a
    # SUBSTRING check, so an expression like
    # `${{ github.event.commits[0].id || github.event.before }}` -- which
    # reaches `commits[0].id` FIRST on a push event, never `before` -- still
    # contained the substring and passed, while the real diff-range wiring
    # was wrong (a multi-commit push resolved to the wrong sha and silently
    # dropped earlier commits from the range). Pin the exact canonical
    # fallback chain instead: `before` must be the LAST alternative, reached
    # only when pull_request.base.sha and merge_group.base_sha are both
    # absent -- exactly GitHub Actions' `||` short-circuit semantics for a
    # push event, and exactly what ci/go_relevant_diff.sh's callers require.
    expected = (
        "${{ github.event.pull_request.base.sha || "
        "github.event.merge_group.base_sha || github.event.before }}"
    )
    assert base_sha_expr == expected, (
        f"the relevance step's BASE_SHA expression is {base_sha_expr!r}, "
        f"expected exactly {expected!r}. A push event must fall back to "
        "github.event.before as the LAST alternative -- any other position "
        "or any other fallback ahead of it can resolve to the wrong sha on "
        "a push and silently narrow or corrupt the diff range "
        "ci/go_relevant_diff.sh computes."
    )


def test_the_relevance_step_uses_the_nul_safe_diff_producer() -> None:
    job = _job_definition()
    steps = job.get("steps") or []
    relevance_steps = [s for s in steps if s.get("id") == "relevance"]
    assert len(relevance_steps) == 1, (
        f"expected exactly one step with id: relevance, found {len(relevance_steps)}"
    )
    run_block = relevance_steps[0].get("run") or ""
    assert "ci/go_relevant_diff.sh" in run_block, (
        f"{GO_QUALITY_WORKFLOW.name}'s relevance step does not call "
        "ci/go_relevant_diff.sh. A plain `git diff --name-only` here loses a "
        "renamed-out path to git's default rename collapse, and a "
        "non-ASCII path to git's default quoting -- both silently, both "
        "already fixed once in ci/go_relevant_diff.sh."
    )
    assert "git diff --name-only" not in run_block, (
        f"{GO_QUALITY_WORKFLOW.name}'s relevance step still calls a plain "
        "`git diff --name-only`, the exact lossy shape ci/go_relevant_diff.sh "
        "exists to replace."
    )
