from __future__ import annotations

import asyncio
import builtins
import json
import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dev_health_ops.llm import LLMAuthError, LLMRateLimitError
from dev_health_ops.llm.providers.batch import (
    BatchCapability,
    BatchItemRequest,
    BatchItemResult,
    BatchItemStatus,
    BatchJobState,
    BatchJobStatus,
    BatchJobSubmission,
    BatchProviderFeature,
)
from dev_health_ops.metrics.schemas import (
    LLMTokenUsageRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
    WorkUnitRepoEffortRecord,
)
from dev_health_ops.work_graph.investment.categorize import CategorizationOutcome
from dev_health_ops.work_graph.investment.llm_schema import EvidenceQuote
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    _effective_model_version,
    materialize_investments,
)
from dev_health_ops.work_graph.investment.utils import ensure_full_subcategory_vector


class FakeSink:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.client = object()
        self.investment_rows: list[WorkUnitInvestmentRecord] = []
        self.repo_effort_rows: list[WorkUnitRepoEffortRecord] = []
        self.quote_rows: list[WorkUnitInvestmentEvidenceQuoteRecord] = []
        self.llm_token_rows: list[LLMTokenUsageRecord] = []
        self.membership_rows: list = []
        self.membership_run_records: list = []
        self.query_calls: list[tuple[str, dict]] = []
        self.query_result_factory: Callable[[str, dict], list[dict]] | None = None

    def ensure_schema(self) -> None:
        return None

    def write_work_unit_investments(self, rows) -> None:
        self.investment_rows.extend(rows)

    def write_work_unit_repo_effort(self, rows) -> None:
        self.repo_effort_rows.extend(rows)

    def write_work_unit_investment_quotes(self, rows) -> None:
        self.quote_rows.extend(rows)

    def write_llm_token_usage(self, rows) -> None:
        self.llm_token_rows.extend(rows)

    def write_work_unit_memberships(self, rows) -> None:
        self.membership_rows.extend(rows)

    def write_membership_run(self, record) -> None:
        self.membership_run_records.append(record)

    def query_dicts(self, query: str, parameters: dict) -> list[dict]:
        self.query_calls.append((query, parameters))
        if self.query_result_factory is not None:
            return self.query_result_factory(query, parameters)
        return []

    def close(self) -> None:
        return None


class FakeProvider:
    async def aclose(self) -> None:
        return None


class FakeBatchProvider(FakeProvider):
    def __init__(self, status: BatchJobStatus = BatchJobStatus.SUCCEEDED) -> None:
        self.requests: list[BatchItemRequest] = []
        self.status = status
        self.fetch_calls = 0
        self.cancel_calls = 0

    def batch_capability(self, model=None):
        return BatchCapability(
            provider="openai",
            model=model or "gpt-test",
            supported=True,
            features=frozenset(
                {
                    BatchProviderFeature.SUBMIT,
                    BatchProviderFeature.POLL,
                    BatchProviderFeature.FETCH_RESULTS,
                }
            ),
        )

    async def submit_batch(self, items):
        self.requests = list(items)
        return BatchJobSubmission(
            provider_job_id="provider-batch-1",
            provider="openai",
            model="gpt-test",
            item_count=len(items),
        )

    async def poll_batch(self, provider_job_id):
        return BatchJobState(
            provider_job_id=provider_job_id,
            status=self.status,
            total_count=len(self.requests),
            completed_count=len(self.requests)
            if self.status == BatchJobStatus.SUCCEEDED
            else 0,
            failed_count=len(self.requests)
            if self.status == BatchJobStatus.FAILED
            else 0,
        )

    async def fetch_batch_results(self, provider_job_id):
        self.fetch_calls += 1
        payload = {
            "subcategories": ensure_full_subcategory_vector(
                {"feature_delivery.roadmap": 1.0}
            ),
            "evidence_quotes": [
                {"quote": "Ship workflow improvement 1", "source": "issue", "id": "E1"}
            ],
            "uncertainty": "Limited evidence.",
        }
        return [
            BatchItemResult(
                custom_id=request.custom_id,
                raw_response=json.dumps(payload),
                provider_metadata={"input_tokens": 11, "output_tokens": 7},
            )
            for request in self.requests
        ]

    async def cancel_batch(self, provider_job_id):
        self.cancel_calls += 1


class PartialBatchProvider(FakeBatchProvider):
    async def fetch_batch_results(self, provider_job_id):
        self.fetch_calls += 1
        results: list[BatchItemResult] = []
        for idx, request in enumerate(self.requests):
            if idx == 0:
                payload = {
                    "subcategories": ensure_full_subcategory_vector(
                        {"feature_delivery.roadmap": 1.0}
                    ),
                    "evidence_quotes": [
                        {
                            "quote": "Ship workflow improvement 1",
                            "source": "issue",
                            "id": "E1",
                        }
                    ],
                    "uncertainty": "Limited evidence.",
                }
                results.append(
                    BatchItemResult(
                        custom_id=request.custom_id,
                        raw_response=json.dumps(payload),
                        provider_metadata={"input_tokens": 11, "output_tokens": 7},
                    )
                )
            elif idx == 1:
                results.append(
                    BatchItemResult(
                        custom_id=request.custom_id,
                        error_code="provider_item_failed",
                        error_message="provider rejected item",
                        provider_metadata={"status_code": 429},
                    )
                )
        return results


class RaisingBatchProvider(FakeBatchProvider):
    def __init__(self, failure_stage: str) -> None:
        super().__init__()
        self.failure_stage = failure_stage

    async def submit_batch(self, items):
        self.requests = list(items)
        if self.failure_stage == "submit":
            raise RuntimeError("submit failed")
        return await super().submit_batch(items)

    async def poll_batch(self, provider_job_id):
        if self.failure_stage == "poll":
            raise RuntimeError("poll failed")
        return await super().poll_batch(provider_job_id)


def test_effective_model_version_uses_org_scoped_resolution(monkeypatch):
    calls: list[tuple[str, str | None, str | None]] = []

    def resolve(provider: str, model: str | None, *, org_id: str | None = None):
        calls.append((provider, model, org_id))
        return "org-byo-model"

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_model_name", resolve
    )

    version = _effective_model_version("openai", None, org_id="org-a")

    assert calls == [("openai", None, "org-a")]
    assert "model=org-byo-model" in version


def test_materialize_config_defaults_to_sync_batch_mode():
    config = MaterializeConfig(
        dsn="clickhouse://localhost:9000/default",
        from_ts=datetime.now(timezone.utc) - timedelta(days=1),
        to_ts=datetime.now(timezone.utc),
        repo_ids=None,
        llm_provider="mock",
        persist_evidence_snippets=True,
        llm_model=None,
    )

    assert config.llm_batch_mode == "sync"
    assert config.llm_batch_min_items == 25


def test_materialize_config_rejects_invalid_batch_mode():
    with pytest.raises(ValueError, match="llm_batch_mode"):
        MaterializeConfig(
            dsn="clickhouse://localhost:9000/default",
            from_ts=datetime.now(timezone.utc) - timedelta(days=1),
            to_ts=datetime.now(timezone.utc),
            repo_ids=None,
            llm_provider="mock",
            persist_evidence_snippets=True,
            llm_model=None,
            llm_batch_mode="invalid",
        )


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


def _multi_component_data(count: int):
    now = datetime.now(timezone.utc)
    repo_ids = [str(uuid.uuid4()) for _ in range(count)]
    edges = []
    work_items = []
    commits = []
    for idx, repo_id in enumerate(repo_ids, start=1):
        issue_id = f"jira:ABC-{idx}"
        commit_hash = f"abc{idx}"
        edges.append(
            {
                "edge_id": f"edge-{idx}",
                "source_type": "issue",
                "source_id": issue_id,
                "target_type": "commit",
                "target_id": f"{repo_id}@{commit_hash}",
                "repo_id": repo_id,
                "confidence": 0.9,
            }
        )
        work_items.append(
            {
                "work_item_id": issue_id,
                "provider": "jira",
                "repo_id": repo_id,
                "title": f"Work item {idx}",
                "description": f"Ship workflow improvement {idx}. " * 20,
                "type": "task",
                "labels": ["feature"],
                "parent_id": "",
                "epic_id": "",
                "created_at": now - timedelta(days=2),
                "updated_at": now - timedelta(days=1),
                "completed_at": now - timedelta(days=1),
            }
        )
        commits.append(
            {
                "repo_id": repo_id,
                "hash": commit_hash,
                "message": f"Work item {idx}",
                "author_when": now - timedelta(days=1),
                "committer_when": now - timedelta(days=1),
            }
        )
    return repo_ids, edges, work_items, commits


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


async def _ok_categorize(bundle, llm_provider, llm_model=None, provider=None):
    return CategorizationOutcome(
        subcategories={"feature_delivery.roadmap": 1.0},
        evidence_quotes=[],
        uncertainty="Limited evidence.",
        status="ok",
        errors=[],
    )


def _patch_successful_materialize(monkeypatch, sink, edges, work_items, commits):
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: FakeProvider(),
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _ok_categorize,
    )


@pytest.mark.asyncio
async def test_materialize_requires_org_for_real_provider(monkeypatch):
    sink = FakeSink()
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError, match=r"--org.*--allow-unscoped"):
        await materialize_investments(
            MaterializeConfig(
                dsn="clickhouse://localhost:8123/default",
                from_ts=now - timedelta(days=5),
                to_ts=now,
                repo_ids=None,
                llm_provider="openai",
                persist_evidence_snippets=False,
                llm_model="test-model",
            )
        )


@pytest.mark.asyncio
async def test_materialize_allow_unscoped_permits_empty_org_real_provider(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()
    _patch_successful_materialize(monkeypatch, sink, edges, work_items, commits)
    now = datetime.now(timezone.utc)

    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_id],
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="test-model",
            allow_unscoped=True,
        )
    )

    assert stats["records"] == 1
    assert sink.investment_rows[0].org_id == ""


@pytest.mark.asyncio
async def test_materialize_mock_provider_permits_empty_org(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()
    _patch_successful_materialize(monkeypatch, sink, edges, work_items, commits)
    now = datetime.now(timezone.utc)

    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_id],
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
        )
    )

    assert stats["records"] == 1
    assert sink.investment_rows[0].org_id == ""


@pytest.mark.asyncio
async def test_materialize_provider_batch_writes_investment_records(monkeypatch):
    _repo_ids, edges, work_items, commits = _multi_component_data(1)
    sink = FakeSink()
    batch_provider = FakeBatchProvider()
    telemetry_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.record_batch_completion",
        lambda **kwargs: telemetry_calls.append(kwargs),
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=True,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert len(batch_provider.requests) == 1
    assert sink.investment_rows[0].categorization_status == "ok"
    assert sink.investment_rows[0].subcategory_distribution_json[
        "feature_delivery.roadmap"
    ] == pytest.approx(1.0)
    assert sink.quote_rows[0].quote == "Ship workflow improvement 1"
    assert sink.llm_token_rows[0].input_tokens == 11
    assert sink.llm_token_rows[0].output_tokens == 7
    assert sink.llm_token_rows[0].model == "gpt-test"
    assert telemetry_calls == [
        {
            "provider": "openai",
            "model": "gpt-test",
            "prompt_version": "investment-categorization-v2",
            "duration_seconds": telemetry_calls[0]["duration_seconds"],
            "input_tokens": 11,
            "output_tokens": 7,
            "output_chars": telemetry_calls[0]["output_chars"],
            "succeeded": True,
        }
    ]
    duration_seconds = telemetry_calls[0]["duration_seconds"]
    output_chars = telemetry_calls[0]["output_chars"]
    assert isinstance(duration_seconds, (int, float))
    assert isinstance(output_chars, int)
    assert duration_seconds >= 0
    assert output_chars > 0


@pytest.mark.asyncio
async def test_materialize_provider_batch_uses_chunk_scoped_correlation(monkeypatch):
    _repo_ids, edges, work_items, commits = _multi_component_data(2)
    sink = FakeSink()
    batch_provider = FakeBatchProvider()
    created_jobs: list[dict] = []

    def _record_created_job(**kwargs):
        created_jobs.append(kwargs)
        return "batch-job-1"

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        _record_created_job,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            run_id="shared-run",
            component_indexes=[1],
            chunk_index=1,
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert created_jobs[0]["run_id"] == "shared-run"
    assert created_jobs[0]["model"] == "gpt-test"
    assert created_jobs[0]["local_correlation_id"] == "shared-run:chunk:1"
    assert created_jobs[0]["specs"][0].custom_id == "shared-run:chunk:1-0"
    assert batch_provider.requests[0].custom_id == "shared-run:chunk:1-0"


@pytest.mark.asyncio
async def test_materialize_provider_batch_failed_job_falls_back_without_fetch(
    monkeypatch,
):
    _repo_ids, edges, work_items, commits = _multi_component_data(1)
    sink = FakeSink()
    batch_provider = FakeBatchProvider(status=BatchJobStatus.FAILED)
    item_transitions: list[dict] = []
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: item_transitions.append(kwargs),
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert batch_provider.fetch_calls == 0
    assert sink.investment_rows[0].categorization_status == "llm_task_failed"
    assert any(
        transition["status"] == BatchItemStatus.FALLBACK.value
        and transition["audit"] == {"reason": "provider_batch_failed"}
        for transition in item_transitions
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["submit", "poll"])
async def test_materialize_provider_batch_exceptions_terminalize_items(
    monkeypatch, failure_stage: str
):
    _repo_ids, edges, work_items, commits = _multi_component_data(1)
    sink = FakeSink()
    batch_provider = RaisingBatchProvider(failure_stage)
    item_transitions: list[dict] = []
    telemetry_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: item_transitions.append(kwargs),
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.record_batch_completion",
        lambda **kwargs: telemetry_calls.append(kwargs),
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert sink.investment_rows[0].categorization_status == "llm_task_failed"
    assert any(
        transition["status"] == BatchItemStatus.FALLBACK.value
        and transition["audit"] == {"reason": f"provider_batch_{failure_stage}_failed"}
        for transition in item_transitions
    )
    assert telemetry_calls[0]["model"] == "gpt-test"
    assert telemetry_calls[0]["succeeded"] is False
    assert batch_provider.cancel_calls == (1 if failure_stage == "poll" else 0)


@pytest.mark.asyncio
async def test_materialize_provider_batch_partial_item_failures_fall_back(monkeypatch):
    _repo_ids, edges, work_items, commits = _multi_component_data(3)
    sink = FakeSink()
    batch_provider = PartialBatchProvider()
    item_transitions: list[dict] = []
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: item_transitions.append(kwargs),
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 3
    assert [row.categorization_status for row in sink.investment_rows].count("ok") == 1
    assert [row.categorization_status for row in sink.investment_rows].count(
        "llm_task_failed"
    ) == 2
    assert any(
        transition["status"] == BatchItemStatus.FALLBACK.value
        and transition.get("provider_error", {}).get("code") == "provider_item_failed"
        for transition in item_transitions
    )
    assert any(
        transition["status"] == BatchItemStatus.FALLBACK.value
        and transition.get("audit") == {"reason": "missing_batch_result"}
        for transition in item_transitions
    )


@pytest.mark.asyncio
async def test_materialize_provider_batch_validation_error_terminalizes_item(
    monkeypatch,
):
    _repo_ids, edges, work_items, commits = _multi_component_data(1)
    sink = FakeSink()
    batch_provider = FakeBatchProvider()
    item_transitions: list[dict] = []

    async def _raise_validation_error(*args, **kwargs):
        raise RuntimeError("repair provider unavailable")

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle_completion",
        _raise_validation_error,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: item_transitions.append(kwargs),
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert sink.investment_rows[0].categorization_status == "llm_task_failed"
    assert any(
        transition["status"] == BatchItemStatus.FALLBACK.value
        and transition["audit"] == {"reason": "batch_result_validation_failed"}
        for transition in item_transitions
    )


@pytest.mark.asyncio
async def test_materialize_provider_batch_timeout_cancels_batch(monkeypatch):
    _repo_ids, edges, work_items, commits = _multi_component_data(1)
    sink = FakeSink()
    batch_provider = FakeBatchProvider(status=BatchJobStatus.RUNNING)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: batch_provider,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._create_batch_job",
        lambda **kwargs: "batch-job-1",
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_job",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._transition_batch_item",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize._update_batch_counts",
        lambda **kwargs: None,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=None,
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="gpt-test",
            org_id="org-a",
            llm_batch_mode="provider_batch",
            llm_batch_poll_interval_seconds=0.01,
            llm_batch_timeout_seconds=0.01,
        )
    )

    assert stats["records"] == 1
    assert batch_provider.cancel_calls == 1
    assert batch_provider.fetch_calls == 0


@pytest.mark.asyncio
async def test_materialize_real_provider_with_org_unaffected(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()
    _patch_successful_materialize(monkeypatch, sink, edges, work_items, commits)
    now = datetime.now(timezone.utc)

    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_id],
            llm_provider="openai",
            persist_evidence_snippets=False,
            llm_model="test-model",
            org_id="org-123",
        )
    )

    assert stats["records"] == 1
    assert sink.investment_rows[0].org_id == "org-123"


@pytest.mark.asyncio
async def test_materialize_invokes_sink(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()
    llm_models: list[str | None] = []

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        llm_models.append(llm_model)
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
            warnings=["weights_normalized:0.9500"],
            llm_calls=1,
            input_tokens=123,
            output_tokens=45,
            llm_model="test-model",
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_model_name",
        lambda provider, model, org_id=None: "org-byo-model",
    )

    now = datetime.now(timezone.utc)
    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=[repo_id],
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model=None,
    )

    stats = await materialize_investments(config)
    assert stats["records"] == 1
    assert stats["llm_calls"] == 1
    assert stats["llm_input_tokens"] == 123
    assert stats["llm_output_tokens"] == 45
    assert llm_models == ["org-byo-model"]
    assert sink.llm_token_rows[0].model == "org-byo-model"
    assert stats["llm_failure_counts"] == {}
    assert len(sink.investment_rows) == 1
    record = sink.investment_rows[0]
    assert record.work_unit_type == "incident"
    assert record.work_unit_name == "Fix login outage"
    assert json.loads(record.categorization_errors_json) == [
        "weights_normalized:0.9500"
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
async def test_materialize_adaptive_concurrency_halves_and_recovers(
    monkeypatch, caplog
):
    repo_ids, edges, work_items, commits = _multi_component_data(6)
    sink = FakeSink()
    call_count = 0

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise LLMRateLimitError("rate limited", provider="mock", model="test-model")
        await asyncio.sleep(0.01)
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
        )

    caplog.set_level(logging.INFO)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=repo_ids,
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            llm_concurrency=4,
        )
    )

    messages = [record.getMessage() for record in caplog.records]
    assert stats["records"] == 6
    assert stats["llm_failure_counts"] == {"rate_limit": 2}
    assert any("adaptive concurrency reduced to 2/4" in msg for msg in messages)
    assert any("adaptive concurrency recovered to 3/4" in msg for msg in messages)


@pytest.mark.asyncio
async def test_materialize_uses_org_llm_concurrency_override(monkeypatch):
    repo_ids, edges, work_items, commits = _multi_component_data(3)
    sink = FakeSink()
    active = 0
    max_active = 0

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.01)
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
    monkeypatch.setattr(
        "dev_health_ops.llm.credentials.resolve_llm_org_settings_concurrency",
        lambda *, org_id=None: 1 if org_id == "org-123" else None,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=repo_ids,
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            llm_concurrency=3,
            org_id="org-123",
        )
    )

    assert stats["records"] == 3
    assert max_active == 1


@pytest.mark.asyncio
async def test_materialize_clamps_excessive_org_llm_concurrency(monkeypatch, caplog):
    repo_ids, edges, work_items, commits = _multi_component_data(40)
    sink = FakeSink()
    active = 0
    max_active = 0

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.02)
            return CategorizationOutcome(
                subcategories={"feature_delivery.roadmap": 1.0},
                evidence_quotes=[],
                uncertainty="Limited evidence.",
                status="ok",
                errors=[],
            )
        finally:
            active -= 1

    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.llm.credentials.resolve_llm_org_settings_concurrency",
        lambda *, org_id=None: 99 if org_id == "org-123" else None,
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=repo_ids,
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            llm_concurrency=3,
            org_id="org-123",
        )
    )

    messages = [record.getMessage() for record in caplog.records]
    assert stats["records"] == 40
    assert max_active == 32
    assert any("LLM concurrency 99 exceeds maximum 32" in msg for msg in messages)


@pytest.mark.asyncio
async def test_materialize_fatal_llm_error_cancels_and_writes_no_rows(monkeypatch):
    repo_one = str(uuid.uuid4())
    repo_two = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
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
    work_items = [
        {
            "work_item_id": "jira:ABC-1",
            "provider": "jira",
            "repo_id": repo_one,
            "title": "Fix billing outage",
            "description": "Resolve customer billing failures. " * 20,
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
            "message": "Fix billing outage",
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
    call_count = 0
    sleeper_cancelled = False

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal call_count, sleeper_cancelled
        call_count += 1
        if call_count == 1:
            raise RuntimeError("insufficient_quota: billing hard limit reached")
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            sleeper_cancelled = True
            raise
        raise AssertionError("pending LLM task was not cancelled")

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
        llm_concurrency=2,
    )

    with pytest.raises(LLMAuthError, match="quota exhausted"):
        await materialize_investments(config)

    assert sleeper_cancelled is True
    assert sink.investment_rows == []
    assert sink.quote_rows == []


@pytest.mark.asyncio
async def test_materialize_fatal_llm_error_flushes_completed_token_usage(monkeypatch):
    repo_one = str(uuid.uuid4())
    repo_two = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
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
    work_items = [
        {
            "work_item_id": "jira:ABC-1",
            "provider": "jira",
            "repo_id": repo_one,
            "title": "Fix billing outage",
            "description": "Resolve customer billing failures. " * 20,
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
            "message": "Fix billing outage",
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
    call_count = 0

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return CategorizationOutcome(
                subcategories={"feature_delivery.roadmap": 1.0},
                evidence_quotes=[],
                uncertainty="Limited evidence.",
                status="ok",
                errors=[],
                warnings=[],
                llm_calls=1,
                input_tokens=123,
                output_tokens=45,
                llm_model="test-model",
            )
        raise RuntimeError("insufficient_quota: billing hard limit reached")

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
        org_id="org-token-test",
        run_id="run-token-test",
    )

    with pytest.raises(LLMAuthError, match="quota exhausted"):
        await materialize_investments(config)

    assert sink.investment_rows == []
    assert len(sink.llm_token_rows) == 1
    token_row = sink.llm_token_rows[0]
    assert token_row.org_id == "org-token-test"
    assert token_row.run_id == "run-token-test"
    assert token_row.source == "investment_materialize"
    assert token_row.input_tokens == 123
    assert token_row.output_tokens == 45
    assert token_row.calls == 1


@pytest.mark.asyncio
async def test_materialize_none_provider_fails_closed_before_writes(monkeypatch):
    sink = FakeSink()

    def _fetch_edges_should_not_run(*args, **kwargs):
        raise AssertionError("none provider must fail before graph categorization")

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_graph_edges",
        _fetch_edges_should_not_run,
    )

    now = datetime.now(timezone.utc)
    config = MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=None,
        llm_provider="none",
        persist_evidence_snippets=False,
        llm_model=None,
    )

    with pytest.raises(LLMAuthError, match="cannot materialize"):
        await materialize_investments(config)

    assert sink.investment_rows == []
    assert sink.quote_rows == []


@pytest.mark.asyncio
async def test_materialize_passes_configured_llm_credentials(monkeypatch):
    sink = FakeSink()
    captured = {}

    def _fake_get_provider(
        name, model=None, *, org_id=None, api_key=None, base_url=None
    ):
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
        allow_unscoped=True,
    )

    stats = await materialize_investments(config)

    assert stats == {
        "components": 0,
        "records": 0,
        "quotes": 0,
        "oversized_components": 0,
        "dropped_edges": 0,
        "dropped_nodes": 0,
    }
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
async def test_materialize_allocates_multi_repo_pr_effort(monkeypatch):
    now = datetime.now(timezone.utc)
    repo_one = str(uuid.uuid4())
    repo_two = str(uuid.uuid4())
    edges = [
        {
            "edge_id": "edge-1",
            "source_type": "issue",
            "source_id": "linear:ABC-1",
            "target_type": "pr",
            "target_id": f"{repo_one}#pr1",
            "repo_id": repo_one,
            "confidence": 0.9,
        },
        {
            "edge_id": "edge-2",
            "source_type": "issue",
            "source_id": "linear:ABC-1",
            "target_type": "pr",
            "target_id": f"{repo_two}#pr2",
            "repo_id": repo_two,
            "confidence": 0.9,
        },
    ]
    work_items = [
        {
            "work_item_id": "linear:ABC-1",
            "provider": "linear",
            "repo_id": None,
            "title": "Ship cross-repo workflow",
            "description": "Ship cross-repo workflow. " * 20,
            "type": "story",
            "labels": ["feature"],
            "parent_id": "",
            "epic_id": "",
            "created_at": now - timedelta(days=2),
            "updated_at": now - timedelta(days=1),
            "completed_at": now - timedelta(days=1),
        }
    ]
    prs = [
        {
            "repo_id": repo_one,
            "number": 1,
            "title": "Frontend workflow",
            "created_at": now - timedelta(days=2),
            "merged_at": now - timedelta(days=1),
            "additions": 30,
            "deletions": 10,
        },
        {
            "repo_id": repo_two,
            "number": 2,
            "title": "Backend workflow",
            "created_at": now - timedelta(days=2),
            "merged_at": now - timedelta(days=1),
            "additions": 20,
            "deletions": 40,
        },
    ]
    sink = FakeSink()

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.get_provider",
        lambda *args, **kwargs: FakeProvider(),
    )
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
        lambda client, repo_numbers, **kwargs: prs,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commits",
        lambda client, repo_commits, **kwargs: [],
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commit_churn",
        lambda client, repo_commits, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_parent_titles",
        lambda client, work_item_ids, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_repo_ids_for_teams",
        lambda client, team_ids, **kwargs: [],
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _ok_categorize,
    )

    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_one, repo_two],
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            org_id="org-real-123",
        )
    )

    assert stats["records"] == 1
    assert stats["repo_effort_records"] == 2
    investment_row = sink.investment_rows[0]
    assert investment_row.repo_id is None
    assert investment_row.effort_metric == "churn_loc"
    assert investment_row.effort_value == pytest.approx(100.0)
    by_repo = {str(row.repo_id): row for row in sink.repo_effort_rows}
    assert by_repo[repo_one].effort_value == pytest.approx(40.0)
    assert by_repo[repo_one].allocation_weight == pytest.approx(0.4)
    assert by_repo[repo_one].allocation_source == "pr_churn"
    assert by_repo[repo_two].effort_value == pytest.approx(60.0)
    assert by_repo[repo_two].allocation_weight == pytest.approx(0.6)
    assert sum(row.effort_value for row in sink.repo_effort_rows) == pytest.approx(
        investment_row.effort_value,
        abs=1e-6,
    )


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
async def test_materialize_skips_fresh_existing_input_hash_by_default(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()

    def _existing_key(_query: str, parameters: dict) -> list[dict]:
        return [
            {
                "work_unit_id": parameters["work_unit_ids"][0],
                "categorization_input_hash": parameters["input_hashes"][0],
            }
        ]

    async def _categorize_should_not_run(*args, **kwargs):
        raise AssertionError("unchanged bundle should not call LLM")

    sink.query_result_factory = _existing_key
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _categorize_should_not_run,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_id],
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            org_id="org-123",
        )
    )

    assert stats["records"] == 0
    assert stats["skipped_existing"] == 1
    assert sink.investment_rows == []
    assert sink.query_calls


@pytest.mark.asyncio
async def test_materialize_force_recomputes_existing_input_hash(monkeypatch):
    repo_id, edges, work_items, commits = _sample_data()
    work_items[0]["description"] = "Resolve authentication failures. " * 20
    sink = FakeSink()
    categorize_calls = 0

    def _existing_key(_query: str, parameters: dict) -> list[dict]:
        return [
            {
                "work_unit_id": parameters["work_unit_ids"][0],
                "categorization_input_hash": parameters["input_hashes"][0],
            }
        ]

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        nonlocal categorize_calls
        categorize_calls += 1
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="Limited evidence.",
            status="ok",
            errors=[],
        )

    sink.query_result_factory = _existing_key
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    now = datetime.now(timezone.utc)
    stats = await materialize_investments(
        MaterializeConfig(
            dsn="clickhouse://localhost:8123/default",
            from_ts=now - timedelta(days=5),
            to_ts=now,
            repo_ids=[repo_id],
            llm_provider="mock",
            persist_evidence_snippets=False,
            llm_model="test-model",
            org_id="org-123",
            force=True,
        )
    )

    assert categorize_calls == 1
    assert stats["records"] == 1
    assert stats["skipped_existing"] == 0
    assert len(sink.investment_rows) == 1


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
