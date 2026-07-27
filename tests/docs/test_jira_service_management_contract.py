"""Jira / JSM public-guide guard.

The internal Jira Service Management provider contract and the canonical operational
model were program-internal source evidence, never published pages. This guard keeps the
public administrator guide task-oriented and free of the internal provider-contract
detail (API hosts, admission matrices) that must not leak into customer documentation.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_GUIDE = ROOT / "docs" / "admin" / "data-sources" / "jira-atlassian.md"


def test_public_jira_guide_is_task_oriented_and_omits_internal_contract() -> None:
    assert PUBLIC_GUIDE.is_file(), f"missing public Jira guide: {PUBLIC_GUIDE}"
    public = PUBLIC_GUIDE.read_text(encoding="utf-8")
    assert "page_id: admin-jira" in public
    assert "Connect and verify Jira" in public
    assert "not yet a supported\nadministrator workflow" in public
    assert "Jira Service Management provider contract" not in public
    assert "api.atlassian.com/jsm/incidents" not in public
