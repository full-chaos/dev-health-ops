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

JOB_ID = "go-quality"


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
    assert "github.event.before" in base_sha_expr, (
        f"the relevance step's BASE_SHA expression ({base_sha_expr!r}) does "
        "not fall back to github.event.before. Without it, a push event "
        "leaves BASE_SHA empty and ci/go_relevant_diff.sh falls back to "
        "HEAD^...HEAD, which only ever sees the last commit of a "
        "multi-commit push."
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
