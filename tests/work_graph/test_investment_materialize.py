from __future__ import annotations

import asyncio
import builtins
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.work_graph.investment.categorize import CategorizationOutcome
from dev_health_ops.work_graph.investment.llm_schema import EvidenceQuote
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    materialize_investments,
)


class FakeSink:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.client = object()
        self.investment_rows: list[WorkUnitInvestmentRecord] = []
        self.quote_rows: list[WorkUnitInvestmentEvidenceQuoteRecord] = []
        self.membership_rows: list = []
        self.membership_run_records: list = []

    def ensure_schema(self) -> None:
        return None

    def write_work_unit_investments(self, rows) -> None:
        self.investment_rows.extend(rows)

    def write_work_unit_investment_quotes(self, rows) -> None:
        self.quote_rows.extend(rows)

    def write_work_unit_memberships(self, rows) -> None:
        self.membership_rows.extend(rows)

    def write_membership_run(self, record) -> None:
        self.membership_run_records.append(record)

    def close(self) -> None:
        return None


class FakeProvider:
    async def aclose(self) -> None:
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
        lambda client, repo_ids=None, **kwargs: edges,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_items",
        lambda client, work_item_ids, **kwargs: work_items,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_item_active_hours",
        lambda client, work_item_ids, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_pull_requests",
        lambda client, repo_numbers, **kwargs: [],
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commits",
        lambda client, repo_commits, **kwargs: commits,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commit_churn",
        lambda client, repo_commits, **kwargs: {
            f"{commit['repo_id']}@{commit['hash']}": 10.0 for commit in commits
        },
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_parent_titles",
        lambda client, work_item_ids, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_repo_ids_for_teams",
        lambda client, team_ids, **kwargs: [],
    )


@pytest.mark.asyncio
async def test_materialize_invokes_sink(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
            warnings=["probability_sum_renormalized:0.9500"],
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
    assert json.loads(record.categorization_errors_json) == [
        "probability_sum_renormalized:0.9500"
    ]


@pytest.mark.asyncio
async def test_materialize_llm_concurrency_one_serializes(monkeypatch):
    repo_one = str(uuid.uuid4())
    repo_two = str(uuid.uuid4())
    edges = [
        {
            "edge_id": "edge-1",
            "source_type": "issue",
            "source_id": "jira:ABC-1",
            "target_type": "commit",
            "target_id": f"{repo_one}@abc123",
            "repo_id": repo_one,
            "confidence": 0.9,
        },
        {
            "edge_id": "edge-2",
            "source_type": "issue",
            "source_id": "jira:ABC-2",
            "target_type": "commit",
            "target_id": f"{repo_two}@def456",
            "repo_id": repo_two,
            "confidence": 0.9,
        },
    ]
    now = datetime.now(timezone.utc)
    work_items = [
        {
            "work_item_id": "jira:ABC-1",
            "provider": "jira",
            "repo_id": repo_one,
            "title": "Fix login outage",
            "description": "Resolve authentication failures. " * 20,
            "type": "incident",
            "labels": ["outage"],
            "parent_id": "",
            "epic_id": "",
            "created_at": now - timedelta(days=2),
            "updated_at": now - timedelta(days=1),
            "completed_at": now - timedelta(days=1),
        },
        {
            "work_item_id": "jira:ABC-2",
            "provider": "jira",
            "repo_id": repo_two,
            "title": "Add checkout flow",
            "description": "Ship checkout workflow improvements. " * 20,
            "type": "task",
            "labels": ["feature"],
            "parent_id": "",
            "epic_id": "",
            "created_at": now - timedelta(days=2),
            "updated_at": now - timedelta(days=1),
            "completed_at": now - timedelta(days=1),
        },
    ]
    commits = [
        {
            "repo_id": repo_one,
            "hash": "abc123",
            "message": "Fix login outage",
            "author_when": now - timedelta(days=1),
            "committer_when": now - timedelta(days=1),
        },
        {
            "repo_id": repo_two,
            "hash": "def456",
            "message": "Add checkout flow",
            "author_when": now - timedelta(days=1),
            "committer_when": now - timedelta(days=1),
        },
    ]
    sink = FakeSink()
    active = 0
    max_active = 0

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0)
            return CategorizationOutcome(
                subcategories={"feature_delivery.roadmap": 1.0},
                evidence_quotes=[],
                uncertainty="Limited evidence.",
                status="ok",
                errors=[],
            )
        finally:
            active -= 1

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=None,
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model="test-model",
        llm_concurrency=1,
    )

    stats = await materialize_investments(config)
    assert stats["records"] == 2
    assert max_active == 1


@pytest.mark.asyncio
async def test_materialize_passes_configured_llm_credentials(monkeypatch):
    sink = FakeSink()
    captured = {}

    def _fake_get_provider(name, model=None, *, api_key=None, base_url=None):
        captured.update(
            {"name": name, "model": model, "api_key": api_key, "base_url": base_url}
        )
        return FakeProvider()

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        _fake_get_provider,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_graph_edges",
        lambda client, repo_ids=None, **kwargs: [],
    )

    now = datetime.now(timezone.utc)
    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=None,
        llm_provider="openai",
        persist_evidence_snippets=False,
        llm_model="gpt-4o-mini",
        llm_api_key="sk-inline-secret",
        llm_base_url="https://inline.invalid/v1",
    )

    stats = await materialize_investments(config)

    assert stats == {"components": 0, "records": 0, "quotes": 0}
    assert captured == {
        "name": "openai",
        "model": "gpt-4o-mini",
        "api_key": "sk-inline-secret",
        "base_url": "https://inline.invalid/v1",
    }
    assert "sk-inline-secret" not in repr(config)


@pytest.mark.asyncio
async def test_materialize_writes_records_with_org_id(monkeypatch):
    """Written rows must carry config.org_id so the org-scoped /investment
    reader (WHERE org_id = %(org_id)s) can see them (CHAOS-2374).

    The first fix only dispatched the task; rows were still written with the
    default org_id='' while the reader filtered on the real org id, leaving the
    view empty. This test exercises record construction end-to-end.
    """
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[
                EvidenceQuote(
                    quote="Resolve authentication failures",
                    source_type="issue_desc",
                    source_id="jira:ABC-1",
                )
            ],
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
        persist_evidence_snippets=True,
        llm_model="test-model",
        org_id="org-real-123",
    )

    stats = await materialize_investments(config)
    assert stats["records"] == 1
    assert sink.investment_rows, "expected at least one investment row"
    # Every written investment row must carry the real org id.
    assert all(r.org_id == "org-real-123" for r in sink.investment_rows)
    # Evidence quotes must be org-tagged too (same reader-scoping concern).
    assert sink.quote_rows, "expected at least one evidence quote row"
    assert all(q.org_id == "org-real-123" for q in sink.quote_rows)


@pytest.mark.asyncio
async def test_materialize_records_default_org_id_empty(monkeypatch):
    """With no org_id configured, rows fall back to '' (not None) so the
    dataclass/sink column stays a String — and no accidental org is invented.
    """
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
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

    await materialize_investments(config)
    assert sink.investment_rows
    assert all(r.org_id == "" for r in sink.investment_rows)


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
