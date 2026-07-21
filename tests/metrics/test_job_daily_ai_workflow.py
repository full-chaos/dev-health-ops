"""CHAOS-2187 + CHAOS-2367: daily-job AI workflow extraction wiring.

Covers ``job_daily._extract_ai_workflow_for_day`` — the helper that turns the
day's PR/review/deployment/incident rows into AI workflow runs and Work
Graph edges. These edge tables were previously never populated because the
extractor had no production call site (CHAOS-2187 for issue/artifact/review
edges; CHAOS-2367 for pr_deployment and deployment_incident edges, where the
extractor existed but was called without deployments/incidents rows).
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
        if "FROM git_pull_request_reviews" in query:
            return [
                {
                    "repo_id": REPO_ID,
                    "number": 7,
                    "review_id": "rev_7_0",
                    "state": "APPROVED",
                    "submitted_at": START,
                    "last_synced": START,
                }
            ]
        if "FROM deployments" in query:
            return [
                {
                    # Native PR link: produces a confidence-1.0 edge.
                    "repo_id": REPO_ID,
                    "deployment_id": "deploy-1",
                    "pull_request_number": 7,
                    "started_at": START,
                    "finished_at": START,
                    "deployed_at": START,
                    "last_synced": START,
                }
            ]
        if "FROM incidents" in query:
            return [
                {
                    "repo_id": REPO_ID,
                    "incident_id": "inc-1",
                    "started_at": START,
                    "last_synced": START,
                }
            ]
        return []


def test_extracts_runs_and_all_edge_kinds() -> None:
    sink = _Sink()
    (
        runs,
        artifact_edges,
        issue_edges,
        review_edges,
        pr_deploy_edges,
        deploy_incident_edges,
    ) = _extract_ai_workflow_for_day(
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

    assert len(review_edges) == 1
    assert review_edges[0].pr_id == f"{REPO_ID}:7"
    assert review_edges[0].review_outcome_id == "rev_7_0"
    assert review_edges[0].outcome == "APPROVED"

    assert len(pr_deploy_edges) == 1
    assert pr_deploy_edges[0].pr_id == f"{REPO_ID}:7"
    assert pr_deploy_edges[0].deployment_id == "deploy-1"
    assert pr_deploy_edges[0].confidence == 1.0
    assert pr_deploy_edges[0].source == "native"

    assert len(deploy_incident_edges) == 1
    assert deploy_incident_edges[0].deployment_id == "deploy-1"
    assert deploy_incident_edges[0].incident_id == "inc-1"
    # incidents rows carry no deployment_id, so the link is the same-day
    # same-repo heuristic.
    assert deploy_incident_edges[0].source == "heuristic"


def test_mapped_canonical_incident_is_deduplicated_before_ai_linkage() -> None:
    class _CanonicalIncidentSink(_Sink):
        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            if "operational_incidents" in query:
                assert parameters["org_id"] == ORG_ID
                assert "repo_id IS NOT NULL" in query
                assert query.count("WHERE org_id = {org_id:String}") >= 2
                canonical_row = {
                    "repo_id": REPO_ID,
                    "incident_id": "pd-1",
                    "status": "resolved",
                    "started_at": START,
                    "resolved_at": END,
                    "last_synced": END,
                }
                return [canonical_row, canonical_row]
            return super().query_dicts(query, parameters)

    result = _extract_ai_workflow_for_day(
        primary_sink=_CanonicalIncidentSink(),
        org_id=ORG_ID,
        start=START,
        end=END,
        repo_id=None,
        repo_provider_by_id={str(REPO_ID): "github"},
    )

    assert len(result[-1]) == 1
    assert result[-1][0].incident_id == "pd-1"


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

    assert result == ([], [], [], [], [], [])
    assert sink.queries == []


def test_unknown_repo_provider_falls_back_to_unknown() -> None:
    sink = _Sink()
    runs, _artifacts, _issues, _reviews, _deploys, _incidents = (
        _extract_ai_workflow_for_day(
            primary_sink=sink,
            org_id=ORG_ID,
            start=START,
            end=END,
            repo_id=REPO_ID,
            repo_provider_by_id={},
        )
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
            if "FROM git_pull_request_reviews" in query:
                rows = rows + [{"repo_id": object(), "number": 3}]
            if "FROM deployments" in query:
                rows = rows + [
                    {"repo_id": "not-a-uuid", "deployment_id": "d-bad"},
                    {"repo_id": REPO_ID, "deployment_id": ""},
                ]
            if "FROM incidents" in query:
                rows = rows + [{"repo_id": REPO_ID, "incident_id": None}]
            return rows

    runs, artifacts, issues, reviews, deploys, incidents = _extract_ai_workflow_for_day(
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
    assert len(reviews) == 1
    assert len(deploys) == 1
    assert len(incidents) == 1
