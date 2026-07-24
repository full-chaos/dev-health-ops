from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.external_ingest import recompute
from dev_health_ops.workers import external_ingest_recompute as bridge


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self) -> _Rows:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self.rows


class _Session:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.executed: list[tuple[Any, dict[str, Any] | None]] = []
        self.commits = 0

    def execute(
        self, statement: Any, parameters: dict[str, Any] | None = None
    ) -> _Rows:
        self.executed.append((statement, parameters))
        return _Rows(self.rows)

    def commit(self) -> None:
        self.commits += 1


def _session_factory(session: _Session):
    @contextmanager
    def factory():
        yield session

    return factory


def _claim() -> bridge._GoBridgeClaim:
    return bridge._GoBridgeClaim(
        job_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        bridge_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        org_id="org-1",
        source_system="github",
        source_instance="Acme/API",
    )


def test_load_bridge_scope_is_allowlisted_and_coalesces_ingestion_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "bridgeVersion": 1,
        "bridgeKind": bridge.GO_COMPATIBILITY_BRIDGE_KIND,
        "bridgeId": _claim().bridge_id,
        "repoIds": ["repo-a", "repo-b"],
        "teamIds": ["team-a"],
        "recordKinds": ["commit.v1", "review.v1"],
        "windowStartedAt": "2026-07-01T00:00:00Z",
        "windowEndedAt": "2026-07-23T00:00:00Z",
    }
    session = _Session(
        [
            {"ingestion_id": "ingestion-b", "recompute_scope": payload},
            {"ingestion_id": "ingestion-a", "recompute_scope": payload},
            {
                "ingestion_id": "other",
                "recompute_scope": {**payload, "bridgeId": "other-bridge"},
            },
        ]
    )
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync", _session_factory(session)
    )
    loaded = bridge._load_go_bridge_scope(_claim())
    assert loaded == {**payload, "ingestionIds": ["ingestion-a", "ingestion-b"]}


def test_load_bridge_scope_rejects_non_allowlisted_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _Session(
        [
            {
                "ingestion_id": "ingestion-a",
                "recompute_scope": {
                    "bridgeVersion": 1,
                    "bridgeKind": "arbitrary.python.module",
                    "bridgeId": _claim().bridge_id,
                },
            }
        ]
    )
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync", _session_factory(session)
    )
    with pytest.raises(ValueError, match="unsupported Go external recompute bridge"):
        bridge._load_go_bridge_scope(_claim())


def test_bridge_invokes_current_python_dispatch_and_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "bridgeVersion": 1,
        "bridgeKind": bridge.GO_COMPATIBILITY_BRIDGE_KIND,
        "bridgeId": _claim().bridge_id,
        "repoIds": ["repo-a"],
        "teamIds": ["team-a"],
        "recordKinds": ["commit.v1"],
        "windowStartedAt": "2026-07-01T00:00:00Z",
        "windowEndedAt": "2026-07-23T00:00:00Z",
        "ingestionIds": ["ingestion-a"],
    }
    calls: list[dict[str, Any]] = []
    marked: list[tuple[str, str]] = []

    def dispatch(**kwargs: Any) -> SimpleNamespace:
        calls.append(kwargs)
        return SimpleNamespace(status="dispatched")

    monkeypatch.setattr(bridge, "_load_go_bridge_scope", lambda _claim: payload)
    monkeypatch.setattr(
        recompute,
        "dispatch_and_persist_scope",
        dispatch,
    )
    monkeypatch.setattr(
        bridge,
        "_mark_go_bridge",
        lambda job_id, status: marked.append((job_id, status)),
    )
    assert bridge._dispatch_go_bridge_claim(_claim()) == "dispatched"
    assert calls == [
        {
            "org_id": "org-1",
            "source_system": "github",
            "source_instance": "Acme/API",
            "ingestion_ids": ["ingestion-a"],
            "repo_ids": ["repo-a"],
            "team_ids": ["team-a"],
            "record_kinds": ["commit.v1"],
            "window_start": datetime(2026, 7, 1, tzinfo=timezone.utc),
            "window_end": datetime(2026, 7, 23, tzinfo=timezone.utc),
        }
    ]
    assert marked == [(_claim().job_id, "bridge_dispatched")]


def test_bridge_scope_keeps_python_capped_plan_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS", "25")
    monkeypatch.setenv("EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS", "14")
    plan = recompute.plan_recompute(
        recompute.RecomputeScope(
            org_id="org-1",
            source_system="github",
            source_instance="Acme/API",
            repo_ids=frozenset(f"repo-{index:02d}" for index in range(30)),
            team_ids=frozenset({"team-a"}),
            record_kinds=frozenset({"commit.v1"}),
            ingestion_ids=frozenset({"ingestion-a", "ingestion-b"}),
            window_start=datetime(2026, 6, 1, tzinfo=timezone.utc),
            window_end=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )
    )
    assert len(plan.repo_ids) == 25
    assert plan.backfill_days == 14
    assert plan.capped_repos is True
    assert plan.capped_days is True


def test_crash_after_dispatch_is_completed_without_duplicate_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marked: list[tuple[str, str]] = []
    monkeypatch.setattr(bridge, "_load_go_bridge_scope", lambda _claim: None)
    monkeypatch.setattr(
        bridge,
        "_mark_go_bridge",
        lambda job_id, status: marked.append((job_id, status)),
    )
    assert bridge._dispatch_go_bridge_claim(_claim()) == "already_terminal"
    assert marked == [(_claim().job_id, "bridge_dispatched")]
