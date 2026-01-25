from __future__ import annotations

import builtins
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import pytest


from dev_health_ops.work_graph.investment.categorize import CategorizationOutcome
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    materialize_investments,
)


class FakeSink:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.client = object()
        self.investment_rows = []
        self.quote_rows = []

    def ensure_schema(self) -> None:
        return None

    def write_work_unit_investments(self, rows) -> None:
        self.investment_rows.extend(rows)

    def write_work_unit_investment_quotes(self, rows) -> None:
        self.quote_rows.extend(rows)

    def close(self) -> None:
        return None


def _sample_data():
    repo_id = str(uuid.uuid4())
    edge = {
        "edge_id": "edge-1",
        "source_type": "issue",
        "source_id": "jira:ABC-1",
        "target_type": "commit",
        "target_id": f"{repo_id}@abc123",
        "repo_id": repo_id,
        "confidence": 0.9,
    }
    work_items = [
        {
            "work_item_id": "jira:ABC-1",
            "provider": "jira",
            "repo_id": repo_id,
            "title": "Fix login outage",
            "description": "Resolve authentication failures",
            "type": "incident",
            "labels": ["outage"],
            "parent_id": "",
            "epic_id": "",
            "created_at": datetime.now(timezone.utc) - timedelta(days=2),
            "updated_at": datetime.now(timezone.utc) - timedelta(days=1),
            "completed_at": datetime.now(timezone.utc) - timedelta(days=1),
        }
    ]
    commits = [
        {
            "repo_id": repo_id,
            "hash": "abc123",
            "message": "Fix login outage",
            "author_when": datetime.now(timezone.utc) - timedelta(days=1),
            "committer_when": datetime.now(timezone.utc) - timedelta(days=1),
        }
    ]
    return repo_id, [edge], work_items, commits


def _patch_queries(monkeypatch, edges, work_items, commits):
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_graph_edges",
        lambda client, repo_ids=None: edges,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_items",
        lambda client, work_item_ids: work_items,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_item_active_hours",
        lambda client, work_item_ids: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_pull_requests",
        lambda client, repo_numbers: [],
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commits",
        lambda client, repo_commits: commits,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commit_churn",
        lambda client, repo_commits: {f"{commits[0]['repo_id']}@abc123": 10.0},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_parent_titles",
        lambda client, work_item_ids: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_repo_ids_for_teams",
        lambda client, team_ids: [],
    )


@pytest.mark.asyncio
async def test_materialize_invokes_sink(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    now = datetime.now(timezone.utc)
    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=[repo_id],
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model="test-model",
    )

    stats = await materialize_investments(config)
    assert stats["records"] == 1
    assert len(sink.investment_rows) == 1
    record = sink.investment_rows[0]
    assert record.work_unit_type == "incident"
    assert record.work_unit_name == "Fix login outage"


@pytest.mark.asyncio
async def test_materialize_does_not_write_files(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    original_open = builtins.open

    def _guard_open(path, mode="r", *args, **kwargs):
        if any(flag in mode for flag in ("w", "a", "x")):
            raise AssertionError(f"File write attempted: {path}")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _guard_open)
    monkeypatch.setattr(
        Path,
        "write_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Path.write_text called")
        ),
    )
    monkeypatch.setattr(
        Path,
        "write_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Path.write_bytes called")
        ),
    )

    now = datetime.now(timezone.utc)
    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=[repo_id],
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model="test-model",
    )

    stats = await materialize_investments(config)
    assert stats["records"] == 1
