"""Unit tests for AIAttributionClickHouseLoader (CHAOS-2744).

Pins the SQL shape of the read path backing the dedicated `/ai/attribution`
page: both `load_mix` and `load_evidence` must read `ai_attribution_resolved`
only, scope with the UUID-safe org filter (the table's `org_id` column is
`UUID`, not `String` -- see `OrgScopedQuery.filter_uuid`), and never fabricate
or drop provenance columns.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest

from dev_health_ops.metrics.loaders.ai_attribution import AIAttributionClickHouseLoader

ORG_ID = "org-test"
REPO_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
REPO_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

START = datetime(2026, 5, 1, tzinfo=timezone.utc)
END = datetime(2026, 5, 8, tzinfo=timezone.utc)


def _capture(rows: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]], Any]:
    captured_queries: list[str] = []
    captured_params: list[dict[str, Any]] = []

    async def fake_qd(_client: Any, query: str, params: Any) -> list[dict[str, Any]]:
        captured_queries.append(query)
        captured_params.append(params)
        return rows

    return captured_queries, captured_params, fake_qd


# -----------------------------------------------------------------------------
# load_mix
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_mix_reads_ai_attribution_resolved_only():
    queries, _params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_mix(start=START, end=END)

    sql = queries[0]
    assert "FROM ai_attribution_resolved" in sql
    assert "GROUP BY kind" in sql
    assert "ai_attribution FINAL" not in sql


@pytest.mark.asyncio
async def test_load_mix_uses_uuid_safe_org_filter():
    """ai_attribution_resolved.org_id is UUID -- the filter must cast with
    toString(), not compare the column directly to a String literal."""
    queries, params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_mix(start=START, end=END)

    sql = queries[0]
    assert "toString(org_id) = {org_id:String}" in sql
    assert params[0]["org_id"] == ORG_ID


@pytest.mark.asyncio
async def test_load_mix_empty_org_id_omits_org_filter():
    queries, params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id="")
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_mix(start=START, end=END)

    assert "org_id" not in queries[0]
    assert "org_id" not in params[0]


@pytest.mark.asyncio
async def test_load_mix_applies_single_repo_filter():
    queries, params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_mix(start=START, end=END, repo_id=REPO_A)

    assert "AND repo_id = {repo_id:UUID}" in queries[0]
    assert params[0]["repo_id"] == str(REPO_A)


@pytest.mark.asyncio
async def test_load_mix_applies_repo_ids_filter():
    queries, params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_mix(start=START, end=END, repo_ids=[REPO_A, REPO_B])

    assert "toString(repo_id) IN {repo_ids:Array(String)}" in queries[0]
    assert params[0]["repo_ids"] == [str(REPO_A), str(REPO_B)]


@pytest.mark.asyncio
async def test_load_mix_returns_grouped_counts():
    rows = [{"kind": "ai_assisted", "count": 5}, {"kind": "unknown", "count": 2}]
    _queries, _params, fake_qd = _capture(rows)
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        result = await loader.load_mix(start=START, end=END)

    assert result == rows


# -----------------------------------------------------------------------------
# load_evidence
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_evidence_selects_full_provenance_columns():
    queries, _params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_evidence(start=START, end=END)

    sql = queries[0]
    for column in (
        "subject_type",
        "subject_id",
        "repo_id",
        "provider",
        "kind",
        "source",
        "confidence",
        "actor",
        "evidence",
        "observed_at",
    ):
        assert column in sql, f"expected {column!r} to be selected"
    assert "FROM ai_attribution_resolved" in sql


@pytest.mark.asyncio
async def test_load_evidence_omits_limit_clause_when_no_limit():
    queries, _params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_evidence(start=START, end=END)

    assert "LIMIT" not in queries[0]


@pytest.mark.asyncio
async def test_load_evidence_applies_limit_offset_clause():
    queries, params, fake_qd = _capture([])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        await loader.load_evidence(start=START, end=END, limit=25, offset=10)

    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in queries[0]
    assert params[0]["limit"] == 25
    assert params[0]["offset"] == 10


@pytest.mark.asyncio
async def test_load_evidence_parses_repo_id_and_preserves_provenance():
    raw = {
        "subject_type": "pull_request",
        "subject_id": "42",
        "repo_id": str(REPO_A),
        "provider": "github",
        "kind": "ai_assisted",
        "source": "pr_label",
        "confidence": 0.95,
        "actor": "github-copilot",
        "evidence": '{"label": "ai-assisted"}',
        "observed_at": datetime(2026, 5, 2, tzinfo=timezone.utc),
    }
    _queries, _params, fake_qd = _capture([raw])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        rows = await loader.load_evidence(start=START, end=END)

    assert len(rows) == 1
    row = rows[0]
    assert row["repo_id"] == REPO_A
    assert row["source"] == "pr_label"
    assert row["confidence"] == 0.95
    assert row["evidence"] == '{"label": "ai-assisted"}'
    assert row["actor"] == "github-copilot"


@pytest.mark.asyncio
async def test_load_evidence_handles_missing_repo_id():
    """Work-item-level attribution records may carry a null repo_id."""
    raw = {
        "subject_type": "issue",
        "subject_id": "work-item-9",
        "repo_id": None,
        "provider": "linear",
        "kind": "agent_created",
        "source": "manual",
        "confidence": 1.0,
        "actor": "human",
        "evidence": "{}",
        "observed_at": datetime(2026, 5, 2, tzinfo=timezone.utc),
    }
    _queries, _params, fake_qd = _capture([raw])
    loader = AIAttributionClickHouseLoader(object(), org_id=ORG_ID)
    with patch("dev_health_ops.api.queries.client.query_dicts", side_effect=fake_qd):
        rows = await loader.load_evidence(start=START, end=END)

    assert rows[0]["repo_id"] is None
