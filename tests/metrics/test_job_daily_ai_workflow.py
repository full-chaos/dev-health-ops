"""CHAOS-2187: daily-job AI workflow extraction wiring.

Covers ``job_daily._extract_ai_workflow_for_day`` — the helper that turns the
day's PR rows into AI workflow runs and Work Graph edges. These edge tables
were previously never populated because the extractor had no production
call site (CHAOS-2187 for issue/artifact/review edges).

CHAOS-5234/CHAOS-3092: this file used to ALSO cover the review/deployment/
incident edge extraction (CHAOS-2367) that lived in the same function --
that half (extract_review_deployment_incident_edges, the work_graph_edges
family) is deleted, chris's standing rule (CHAOS-5233): once a family's Go
executor is on main, its Python compute is deleted, never kept alive just
for a rot guard. WorkGraphEdgesExecutor (native Go) is now the sole
computer/writer of those three tables (closes CHAOS-5216 by construction:
single native reader). _extract_ai_workflow_for_day now returns a 3-tuple
(runs, artifact_edges, issue_edges) instead of the old 6-tuple.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.job_daily import _extract_ai_workflow_for_day

ORG_ID = "22222222-2222-2222-2222-222222222222"
REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
START = datetime(2026, 6, 8, tzinfo=timezone.utc)
END = datetime(2026, 6, 9, tzinfo=timezone.utc)


class _Sink:
    """Routes query_dicts calls by source table."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.queries.append(query)
        if "FROM git_pull_requests" in query:
            return [
                {
                    "repo_id": REPO_ID,
                    "number": 7,
                    "title": "Add caching",
                    # PR body carries an explicit AI assistance declaration so
                    # the extractor's body signal fires deterministically.
                    "body": "Generated with Claude Code",
                    "head_branch": "feature/cache",
                    "author_name": "dev-a",
                    "author_email": "dev-a@example.com",
                    "created_at": START,
                    "merged_at": START,
                    "closed_at": None,
                    "last_synced": START,
                },
                {
                    # No AI signal: must not produce a run.
                    "repo_id": REPO_ID,
                    "number": 8,
                    "title": "Fix typo",
                    "body": "",
                    "head_branch": "fix/typo",
                    "author_name": "dev-b",
                    "author_email": "dev-b@example.com",
                    "created_at": START,
                    "merged_at": None,
                    "closed_at": None,
                    "last_synced": START,
                },
            ]
        if "FROM work_graph_issue_pr" in query:
            return [{"repo_id": REPO_ID, "pr_number": 7, "work_item_id": "jira:ABC-1"}]
        return []


def test_extracts_runs_and_ai_workflow_edges() -> None:
    sink = _Sink()
    runs, artifact_edges, issue_edges = _extract_ai_workflow_for_day(
        primary_sink=sink,
        org_id=ORG_ID,
        start=START,
        end=END,
        repo_id=None,
        repo_provider_by_id={str(REPO_ID): "github"},
    )

    assert len(runs) == 1
    assert str(runs[0].org_id) == ORG_ID
    assert runs[0].provider == "github"

    assert len(artifact_edges) == 1
    assert artifact_edges[0].artifact_id == f"{REPO_ID}:7"

    assert len(issue_edges) == 1
    assert issue_edges[0].issue_id == "jira:ABC-1"


def test_non_uuid_org_skips_extraction_without_queries() -> None:
    sink = _Sink()
    result = _extract_ai_workflow_for_day(
        primary_sink=sink,
        org_id="not-a-uuid",
        start=START,
        end=END,
        repo_id=None,
        repo_provider_by_id={},
    )

    assert result == ([], [], [])
    assert sink.queries == []


def test_unknown_repo_provider_falls_back_to_unknown() -> None:
    sink = _Sink()
    runs, _artifacts, _issues = _extract_ai_workflow_for_day(
        primary_sink=sink,
        org_id=ORG_ID,
        start=START,
        end=END,
        repo_id=REPO_ID,
        repo_provider_by_id={},
    )

    assert len(runs) == 1
    assert runs[0].provider == "unknown"


def test_infrastructure_errors_propagate() -> None:
    """Review fix 2: ClickHouse failures must fail the job, not silently
    produce an empty (= 'no AI activity') day."""

    class _BrokenSink:
        def query_dicts(self, query: str, parameters: dict[str, Any]) -> list:
            raise RuntimeError("clickhouse unavailable")

    import pytest

    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        _extract_ai_workflow_for_day(
            primary_sink=_BrokenSink(),
            org_id=ORG_ID,
            start=START,
            end=END,
            repo_id=None,
            repo_provider_by_id={},
        )


def test_malformed_rows_are_dropped_row_locally() -> None:
    """Review fix 2: one garbage row must not abort the day's extraction."""

    class _MalformedRowSink(_Sink):
        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            rows = super().query_dicts(query, parameters)
            if "FROM git_pull_requests" in query:
                rows = rows + [
                    {"repo_id": "not-a-uuid", "number": 9, "title": "bad"},
                    {"repo_id": REPO_ID, "number": None, "title": "bad"},
                ]
            return rows

    runs, artifacts, issues = _extract_ai_workflow_for_day(
        primary_sink=_MalformedRowSink(),
        org_id=ORG_ID,
        start=START,
        end=END,
        repo_id=None,
        repo_provider_by_id={str(REPO_ID): "github"},
    )

    # The well-formed rows still extract exactly as in the happy-path test.
    assert len(runs) == 1
    assert len(artifacts) == 1
    assert len(issues) == 1
