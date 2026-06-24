from __future__ import annotations

from datetime import date
from typing import Any

from dev_health_ops.backfill import runner as backfill_runner


def test_backfill_autoimport_false_or_absent_does_not_call(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        backfill_runner, "run_team_autoimport", lambda **kwargs: calls.append(kwargs)
    )

    assert (
        backfill_runner._run_team_autoimport_for_backfill(
            provider="jira",
            org_id="org-1",
            credentials={},
            sync_options={},
            sync_config_id="cfg-1",
            since=date(2026, 1, 1),
            before=date(2026, 1, 7),
            window_count=1,
            analytics_db_url=None,
        )
        is None
    )
    assert (
        backfill_runner._run_team_autoimport_for_backfill(
            provider="jira",
            org_id="org-1",
            credentials={},
            sync_options={"auto_import_teams": False},
            sync_config_id="cfg-1",
            since=date(2026, 1, 1),
            before=date(2026, 1, 7),
            window_count=1,
            analytics_db_url=None,
        )
        is None
    )
    assert calls == []


def test_backfill_autoimport_true_calls_once(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_team_autoimport(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(
        backfill_runner, "run_team_autoimport", fake_run_team_autoimport
    )

    result = backfill_runner._run_team_autoimport_for_backfill(
        provider="jira",
        org_id="org-1",
        credentials={"email": "dev@example.com"},
        sync_options={"auto_import_teams": True, "project_keys": ["OPS"]},
        sync_config_id="cfg-1",
        since=date(2026, 1, 1),
        before=date(2026, 1, 7),
        window_count=1,
        analytics_db_url="clickhouse://backfill-dsn",
    )

    assert result == {"status": "success"}
    assert len(calls) == 1
    assert calls[0] == {
        "provider": "jira",
        "org_id": "org-1",
        "credentials": {"email": "dev@example.com"},
        "analytics_db_url": "clickhouse://backfill-dsn",
        "scope": {
            "mode": "backfill",
            "sync_config_id": "cfg-1",
            "sync_options": {"auto_import_teams": True, "project_keys": ["OPS"]},
            "window_count": 1,
            "since": "2026-01-01",
            "before": "2026-01-07",
        },
    }
