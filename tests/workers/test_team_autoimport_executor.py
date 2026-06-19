from __future__ import annotations

import logging
from typing import Any

from dev_health_ops.workers import team_autoimport


def test_run_team_autoimport_skips_non_capable_provider(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport(
        provider="launchdarkly",
        org_id="org-1",
        credentials={"token": "secret"},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "provider_not_import_capable"
    assert result["projects_imported"] == 0
    assert "provider is not import-capable" in caplog.text


def test_run_team_autoimport_skips_missing_populator(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport(
        provider="ms-teams",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={"sync_options": {"auto_import_teams": True}},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "populator_not_available"
    assert result["members_imported"] == 0
    assert "no populator module is available" in caplog.text


def test_run_team_autoimport_calls_resolved_populator(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"members_imported": 2}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    result = team_autoimport.run_team_autoimport(
        provider="jira",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={"project_keys": ["OPS"]},
    )

    assert result == {
        "status": "success",
        "provider": "jira",
        "org_id": "org-1",
        "members_imported": 2,
    }
    assert calls == [
        {
            "org_id": "org-1",
            "credentials": {"token": "secret"},
            "scope": {"project_keys": ["OPS"]},
        }
    ]
