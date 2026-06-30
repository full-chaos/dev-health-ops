from __future__ import annotations

import logging
from typing import Any

import pytest

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
        analytics_db_url="clickhouse://config-dsn",
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
            "scope": {
                "project_keys": ["OPS"],
                "analytics_db": "clickhouse://config-dsn",
            },
        }
    ]


def test_run_team_autoimport_strict_noops_non_capable_provider(caplog) -> None:
    """A provider with no reference tier (e.g. launchdarkly) must NOT fail the
    strict pre-sync discovery stage — it is a successful no-op so the run can
    proceed to dispatch (CHAOS-2740)."""
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport_strict(
        provider="launchdarkly",
        org_id="org-1",
        credentials={"token": "secret"},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "provider_not_import_capable"
    assert result["projects_imported"] == 0
    assert "not import-capable" in caplog.text
    # No reference keys claimed -> _verify_reference_readback no-ops and the
    # discovery ledger is stamped success.
    assert "reference_team_keys" not in result
    assert "reference_sprint_ids" not in result


def test_run_team_autoimport_strict_raises_capable_provider_missing_populator(
    monkeypatch,
) -> None:
    """Genuine failures are preserved: a capable provider whose populator module
    is unavailable still fails strict discovery visibly."""
    monkeypatch.setattr(team_autoimport, "_provider_capability", lambda provider: True)
    monkeypatch.setattr(team_autoimport, "_resolve_populator", lambda provider: None)

    with pytest.raises(ValueError, match="populator is unavailable"):
        team_autoimport.run_team_autoimport_strict(
            provider="github",
            org_id="org-1",
            credentials={"token": "secret"},
        )
