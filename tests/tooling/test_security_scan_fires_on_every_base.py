"""CHAOS-6973: Security Scan must fire on a stacked PR (base != main).

`.github/workflows/security-scan.yml` used to declare `pull_request:
branches: [main]`. GitHub Actions filters a `pull_request` trigger by the
PR's TARGET (base) branch, not its head -- so a stacked PR, whose base is a
sibling feature branch rather than `main`, never matched that filter at all.
Confirmed on #3330/#3331 (both stacked, base != main): `gh api
commits/<sha>/check-runs` showed zero Security Scan runs, not even a
cancelled one, while #3329 (same stack, base=main) got a real pass. Every
stacked PR in this repo's history silently skipped Gitleaks/pip-audit/
Semgrep.

This test pins the fix structurally (the trigger config); it cannot itself
open a live stacked PR. The PR this ticket lands in carries the executed
proof separately: a stacked test PR (base != main) whose Security Scan job
actually ran.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _ROOT / ".github" / "workflows" / "security-scan.yml"


def _triggers() -> dict:
    document = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8")) or {}
    # PyYAML's default resolver reads the bare scalar key `on` as the
    # boolean True, not the string "on" -- the same gotcha
    # test_workflow_concurrency_groups.py already works around.
    triggers = document.get(True) or document.get("on")
    assert triggers, f"{_WORKFLOW.name} declares no top-level trigger"
    return triggers


def test_pull_request_trigger_matches_every_base_branch() -> None:
    triggers = _triggers()
    assert "pull_request" in triggers, (
        "security-scan.yml no longer runs on pull_request"
    )
    pr_trigger = triggers["pull_request"] or {}
    branches = pr_trigger.get("branches")
    assert branches == ["**"], (
        f"security-scan.yml's pull_request.branches is {branches!r}, want ['**'] -- "
        "a base-branch filter here means a stacked PR (base != main) never triggers "
        "Gitleaks/pip-audit/Semgrep at all (CHAOS-6973)"
    )


def test_push_trigger_stays_scoped_to_main() -> None:
    # The fix widens ONLY pull_request. A push trigger matching every branch
    # would double-run this workflow on every feature-branch push in
    # addition to its PR run -- test.yml/typecheck.yml do not do this either.
    triggers = _triggers()
    push_trigger = triggers.get("push") or {}
    assert push_trigger.get("branches") == ["main"], (
        f"push.branches changed to {push_trigger.get('branches')!r} -- this fix must "
        "widen pull_request only, not push"
    )
