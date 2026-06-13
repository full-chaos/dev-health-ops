"""Live-ClickHouse verification for the CHAOS-2180 Wave-2 review fixes.

Covers (each against a throwaway random org so runs are isolated):

1. Daily-job rerun produces NO query-visible duplicate AI workflow edges
   through the real reader path (``load_ai_workflow_graph_for_pr`` — the
   UNION ALL now reads every ReplacingMergeTree with FINAL).
2. Detector anti-join and doc-drift join predicates carry the tenant key:
   org B's rows can no longer suppress or pollute org A's detections even
   when repo_id / PR number / commit hash collide across orgs.
3. Governance allowlist precedence: exact tool+model rows beat wildcard
   rows, latest version wins, and overlapping policy rows never fan an
   artifact out into duplicates.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def _insert_prs(sink, org_id: str, repo_id: uuid.UUID, prs: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    sink.client.insert(
        "git_pull_requests",
        [
            [
                repo_id,
                pr["number"],
                pr.get("title", ""),
                pr.get("body", ""),
                pr.get("author_name", "alice"),
                pr.get("author_email", "alice@example.com"),
                pr.get("created_at", now - timedelta(days=2)),
                pr.get("merged_at"),
                pr.get("head_branch", "feature/x"),
                now,
                org_id,
            ]
            for pr in prs
        ],
        column_names=[
            "repo_id",
            "number",
            "title",
            "body",
            "author_name",
            "author_email",
            "created_at",
            "merged_at",
            "head_branch",
            "last_synced",
            "org_id",
        ],
    )


def _insert_attributions(
    sink,
    org_id: uuid.UUID,
    repo_id: uuid.UUID,
    subjects: list[str],
    *,
    kind: str = "ai_assisted",
    evidence: dict | None = None,
    observed_at: datetime | None = None,
) -> None:
    observed = observed_at or (datetime.now(timezone.utc) - timedelta(hours=1))
    sink.client.insert(
        "ai_attribution",
        [
            [
                uuid.uuid4(),
                org_id,
                "github",
                "pull_request",
                subject,
                repo_id,
                kind,
                "pr_label",
                0.9,
                json.dumps(evidence or {"label": "ai-assisted"}),
                observed,
                observed,
            ]
            for subject in subjects
        ],
        column_names=[
            "record_id",
            "org_id",
            "provider",
            "subject_type",
            "subject_id",
            "repo_id",
            "kind",
            "source",
            "confidence",
            "evidence",
            "observed_at",
            "ingested_at",
        ],
    )


async def test_daily_job_rerun_has_no_reader_visible_duplicate_edges() -> None:
    """Fix 1: write extraction twice, read once — counts must be rerun-stable."""
    from dev_health_ops.api.queries.client import get_global_client
    from dev_health_ops.metrics.job_daily import _extract_ai_workflow_for_day
    from dev_health_ops.work_graph.ai_workflow import load_ai_workflow_graph_for_pr

    sink = _sink()
    org = uuid.uuid4()
    repo = uuid.uuid4()
    day_start = datetime.combine(
        date.today() - timedelta(days=1), time.min, tzinfo=timezone.utc
    )
    day_end = day_start + timedelta(days=1)
    created = day_start + timedelta(hours=3)

    _insert_prs(
        sink,
        str(org),
        repo,
        [
            {
                "number": 7,
                "title": "Add caching",
                "body": "Generated with Claude Code",
                "created_at": created,
                "merged_at": created + timedelta(hours=2),
            }
        ],
    )
    sink.client.insert(
        "git_pull_request_reviews",
        [
            [
                repo,
                7,
                "rev_7_0",
                "bob@example.com",
                "APPROVED",
                created,
                created,
                str(org),
            ]
        ],
        column_names=[
            "repo_id",
            "number",
            "review_id",
            "reviewer",
            "state",
            "submitted_at",
            "last_synced",
            "org_id",
        ],
    )
    sink.client.insert(
        "work_graph_issue_pr",
        [[repo, "jira:ABC-1", 7, 1.0, "native", "test", created, str(org)]],
        column_names=[
            "repo_id",
            "work_item_id",
            "pr_number",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
            "org_id",
        ],
    )

    def _run_once() -> None:
        (
            runs,
            artifacts,
            issues,
            reviews,
            pr_deploys,
            deploy_incidents,
        ) = _extract_ai_workflow_for_day(
            primary_sink=sink,
            org_id=str(org),
            start=day_start,
            end=day_end,
            repo_id=None,
            repo_provider_by_id={str(repo): "github"},
        )
        assert runs and artifacts and issues and reviews
        sink.write_ai_workflow_runs(runs)
        sink.write_ai_workflow_artifact_edges(artifacts)
        sink.write_ai_workflow_issue_edges(issues)
        sink.write_work_graph_pr_review_outcome_edges(reviews)
        if pr_deploys:
            sink.write_work_graph_pr_deployment_edges(pr_deploys)
        if deploy_incidents:
            sink.write_work_graph_deployment_incident_edges(deploy_incidents)

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    client = await get_global_client(CLICKHOUSE_URI)
    pr_root = f"{repo}:7"

    _run_once()
    first = await load_ai_workflow_graph_for_pr(client, str(org), pr_root)
    assert len(first.edges) == 3  # generates, has_ai_workflow, has_review_outcome
    assert not first.partial

    _run_once()  # rerun the same day — same deterministic ids, new computed_at
    second = await load_ai_workflow_graph_for_pr(client, str(org), pr_root)
    assert len(second.edges) == len(first.edges)
    assert sorted(e.edge_id for e in second.edges) == sorted(
        e.edge_id for e in first.edges
    )
    assert len(second.nodes) == len(first.nodes)
    assert not second.partial


async def test_dep_update_anti_join_is_tenant_isolated() -> None:
    """Fix 3: org B attribution rows must not suppress org A's detections."""
    from dev_health_ops.api.graphql.models.ai import AIOpportunityKind
    from dev_health_ops.api.queries.client import get_global_client
    from dev_health_ops.metrics.opportunities.ai_detector import AIOpportunityDetector

    sink = _sink()
    org_a = uuid.uuid4()
    org_b = uuid.uuid4()
    repo = uuid.uuid4()
    numbers = list(range(1, 7))

    _insert_prs(
        sink,
        str(org_a),
        repo,
        [
            {"number": n, "title": f"Bump lodash version from 1.{n} to 2.{n}"}
            for n in numbers
        ],
    )
    # Same repo_id and PR numbers, but the AI attribution lives in org B.
    _insert_attributions(sink, org_b, repo, [str(n) for n in numbers])

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    client = await get_global_client(CLICKHOUSE_URI)
    result_a = await AIOpportunityDetector(client).detect(str(org_a), limit=50)
    kinds_a = {item.kind for item in result_a}
    assert AIOpportunityKind.DEPENDENCY_UPDATES in kinds_a

    # Org B has attributions but no PRs: nothing should fire there.
    result_b = await AIOpportunityDetector(client).detect(str(org_b), limit=50)
    assert AIOpportunityKind.DEPENDENCY_UPDATES not in {i.kind for i in result_b}


async def test_doc_drift_join_is_tenant_isolated() -> None:
    """Fix 3: org B doc-file stats on colliding commit hashes must not hide
    org A's documentation drift."""
    from dev_health_ops.api.graphql.models.ai import AIOpportunityKind
    from dev_health_ops.api.queries.client import get_global_client
    from dev_health_ops.metrics.opportunities.ai_detector import (
        _DOC_DRIFT_MIN_CODE_COMMITS,
        AIOpportunityDetector,
    )

    sink = _sink()
    org_a = uuid.uuid4()
    org_b = uuid.uuid4()
    repo = uuid.uuid4()
    when = datetime.now(timezone.utc) - timedelta(days=2)
    hashes = [
        f"hash-{org_a.hex[:8]}-{i}" for i in range(_DOC_DRIFT_MIN_CODE_COMMITS + 5)
    ]

    sink.client.insert(
        "git_commits",
        [
            [repo, h, "alice", "alice@example.com", when, when, when, str(org_a)]
            for h in hashes
        ],
        column_names=[
            "repo_id",
            "hash",
            "author_name",
            "author_email",
            "author_when",
            "committer_when",
            "last_synced",
            "org_id",
        ],
    )
    # Org A: pure code churn, zero doc files.
    sink.client.insert(
        "git_commit_stats",
        [
            [repo, h, f"src/mod_{i}.py", 10, 2, when, str(org_a)]
            for i, h in enumerate(hashes)
        ],
        column_names=[
            "repo_id",
            "commit_hash",
            "file_path",
            "additions",
            "deletions",
            "last_synced",
            "org_id",
        ],
    )
    # Org B: doc-file stats for the SAME repo/commit hashes. Without the
    # org key in the join these rows would zero out org A's drift signal.
    sink.client.insert(
        "git_commit_stats",
        [[repo, h, "docs/readme.md", 5, 0, when, str(org_b)] for h in hashes],
        column_names=[
            "repo_id",
            "commit_hash",
            "file_path",
            "additions",
            "deletions",
            "last_synced",
            "org_id",
        ],
    )

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    client = await get_global_client(CLICKHOUSE_URI)
    result_a = await AIOpportunityDetector(client).detect(str(org_a), limit=50)
    drift = [i for i in result_a if i.kind is AIOpportunityKind.DOCUMENTATION_DRIFT]
    assert drift, "org B doc rows leaked into org A's doc-drift join"
    assert drift[0].repo_id == str(repo)


async def test_allowlist_exact_model_beats_wildcard_without_fanout() -> None:
    """Fix 4: precedence is exact > wildcard, latest version wins, one row
    per artifact."""
    from dev_health_ops.audit.ai_governance.loaders import AIGovernanceLoader
    from dev_health_ops.audit.ai_governance.models import (
        AIToolAllowlistEntry,
        ToolAllowlistStatus,
    )

    sink = _sink()
    org = uuid.uuid4()
    repo = uuid.uuid4()
    day = date.today()
    observed = datetime.combine(day, time(hour=10), tzinfo=timezone.utc)

    sink.write_ai_tool_allowlist(
        [
            AIToolAllowlistEntry(
                org_id=str(org),
                tool_name="testtool",
                model_name=None,
                status=ToolAllowlistStatus.ALLOWED,
                reason="wildcard policy",
            ),
            AIToolAllowlistEntry(
                org_id=str(org),
                tool_name="testtool",
                model_name="model-y",
                status=ToolAllowlistStatus.DISALLOWED,
                reason="exact policy",
            ),
        ]
    )
    _insert_attributions(
        sink,
        org,
        repo,
        ["1"],
        evidence={"tool_name": "testtool", "model_name": "model-y"},
        observed_at=observed,
    )
    _insert_attributions(
        sink,
        org,
        repo,
        ["2"],
        evidence={"tool_name": "testtool", "model_name": "model-z"},
        observed_at=observed,
    )

    loader = AIGovernanceLoader(sink)
    artifacts = loader.load_artifacts_for_day(org_id=str(org), day=day)
    by_subject = {}
    for artifact in artifacts:
        # Overlapping wildcard + exact rows must not duplicate artifacts.
        assert artifact.subject_id not in by_subject, "artifact fan-out"
        by_subject[artifact.subject_id] = artifact

    assert by_subject["1"].tool_allowlist_status is ToolAllowlistStatus.DISALLOWED
    assert by_subject["2"].tool_allowlist_status is ToolAllowlistStatus.ALLOWED

    # A newer version of the exact row must win via argMax(computed_at).
    sink.write_ai_tool_allowlist(
        [
            AIToolAllowlistEntry(
                org_id=str(org),
                tool_name="testtool",
                model_name="model-y",
                status=ToolAllowlistStatus.DEPRECATED,
                reason="updated exact policy",
            )
        ]
    )
    refreshed = {
        a.subject_id: a for a in loader.load_artifacts_for_day(org_id=str(org), day=day)
    }
    assert refreshed["1"].tool_allowlist_status is ToolAllowlistStatus.DEPRECATED
    assert refreshed["2"].tool_allowlist_status is ToolAllowlistStatus.ALLOWED


async def test_blank_model_rows_resolve_as_wildcard_never_phantom_exact() -> None:
    """Fix 2 (final pass): a legacy ''-model row (inserted raw, bypassing the
    normalising dataclass) must behave as wildcard, and an artifact with NO
    model evidence (JSONExtractString yields '') must resolve via wildcard —
    never phantom-match a '' "exact" key."""
    from dev_health_ops.audit.ai_governance.loaders import AIGovernanceLoader
    from dev_health_ops.audit.ai_governance.models import ToolAllowlistStatus

    sink = _sink()
    org = uuid.uuid4()
    repo = uuid.uuid4()
    day = date.today()
    observed = datetime.combine(day, time(hour=9), tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    # Legacy '' row written raw (the dataclass would normalise it away).
    sink.client.insert(
        "ai_tool_allowlist",
        [[str(org), "legacytool", "", "disallowed", "legacy blank row", now, now]],
        column_names=[
            "org_id",
            "tool_name",
            "model_name",
            "status",
            "reason",
            "updated_at",
            "computed_at",
        ],
    )
    # Artifact with tool evidence but NO model evidence.
    _insert_attributions(
        sink,
        org,
        repo,
        ["1"],
        evidence={"tool_name": "legacytool"},
        observed_at=observed,
    )
    # Artifact WITH a model: must also resolve via the ''-as-wildcard row,
    # never via a phantom '' exact key.
    _insert_attributions(
        sink,
        org,
        repo,
        ["2"],
        evidence={"tool_name": "legacytool", "model_name": "some-model"},
        observed_at=observed,
    )

    loader = AIGovernanceLoader(sink)
    artifacts = {
        a.subject_id: a for a in loader.load_artifacts_for_day(org_id=str(org), day=day)
    }
    assert len(artifacts) == 2  # no fan-out
    assert artifacts["1"].tool_allowlist_status is ToolAllowlistStatus.DISALLOWED
    assert artifacts["2"].tool_allowlist_status is ToolAllowlistStatus.DISALLOWED
