"""Tests for the _assert_pyformat_safe guard in client.py (CHAOS-2566).

The guard prevents the entire class of bugs where a ClickHouse query built with
pyformat %(name)s params contains a bare literal '%' that clickhouse-connect
misreads as a positional conversion, producing:

    TypeError: not enough arguments for format string

Tests cover:
1. Unescaped '%' in pyformat query → guard raises ValueError with actionable msg
2. Properly escaped '%%' → guard does NOT raise
3. Server-side {name:Type} placeholder present → guard skips (does NOT raise)
4. No '%' at all → no raise
5. Anti-false-positive: real investment pyformat query passes the guard
6. Anti-false-positive: real ai_detector server-side query passes the guard
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

import dev_health_ops.api.queries.investment as investment_module
from dev_health_ops.api.queries.client import _assert_pyformat_safe
from dev_health_ops.api.queries.investment import fetch_investment_breakdown

# ---------------------------------------------------------------------------
# Unit tests for _assert_pyformat_safe
# ---------------------------------------------------------------------------


def test_unescaped_percent_in_pyformat_query_raises() -> None:
    """A bare '%' in a pyformat query must raise ValueError with a clear message."""
    query = """
        SELECT count() FROM work_unit_investments
        WHERE lower(categorization_model_version) LIKE '%mock%'
          AND org_id = %(org_id)s
    """
    params = {"org_id": "org-abc"}
    with pytest.raises(ValueError, match="unescaped literal '%'"):
        _assert_pyformat_safe(query, params)


def test_unescaped_percent_error_includes_fragment() -> None:
    """The ValueError must include a fragment near the offending '%'."""
    query = "SELECT * FROM t WHERE col LIKE '%needle%' AND x = %(x)s"
    params = {"x": "val"}
    with pytest.raises(ValueError, match="needle"):
        _assert_pyformat_safe(query, params)


def test_escaped_percent_does_not_raise() -> None:
    """'%%' is the correct escape; the guard must accept it."""
    query = """
        SELECT count() FROM work_unit_investments
        WHERE lower(categorization_model_version) LIKE '%%mock%%'
          AND org_id = %(org_id)s
    """
    params = {"org_id": "org-abc"}
    _assert_pyformat_safe(query, params)  # must not raise


def test_server_side_placeholder_skips_validation() -> None:
    """When the query has a {name:Type} placeholder, the guard must skip entirely.

    ai_detector.py queries use server-side binding and legitimately contain
    bare LIKE '%...%' patterns — the guard must not fire on them.
    """
    query = """
        SELECT count() FROM git_pull_requests AS pr
        WHERE lower(pr.author_name) NOT LIKE '%bot%'
          AND pr.repo_id = {repo_id:UUID}
    """
    params = {"repo_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
    _assert_pyformat_safe(query, params)  # must not raise


def test_no_percent_at_all_does_not_raise() -> None:
    """A query with no '%' characters at all must pass silently."""
    query = "SELECT count() FROM t WHERE org_id = %(org_id)s"
    params = {"org_id": "org-x"}
    _assert_pyformat_safe(query, params)  # must not raise


def test_non_mapping_params_skips_validation() -> None:
    """Non-mapping params (e.g. a list) are not the pyformat path; skip."""
    query = "SELECT * FROM t WHERE col LIKE '%bad%'"
    _assert_pyformat_safe(query, ["positional"])  # must not raise
    _assert_pyformat_safe(query, None)  # must not raise


def test_multiple_unescaped_percents_raises_on_first() -> None:
    """Multiple bare '%' patterns — guard raises on the first one found."""
    query = "SELECT * FROM t WHERE a LIKE '%foo%' AND b LIKE '%bar%' AND x = %(x)s"
    params = {"x": 1}
    with pytest.raises(ValueError, match="unescaped literal '%'"):
        _assert_pyformat_safe(query, params)


def test_only_valid_named_conversions_does_not_raise() -> None:
    """A query with only %(name)s / %(name)d conversions and no bare '%' is fine."""
    query = (
        "SELECT * FROM t"
        " WHERE ts >= %(start_ts)s"
        " AND ts < %(end_ts)s"
        " AND org_id = %(org_id)s"
    )
    params = {"start_ts": "2024-01-01", "end_ts": "2024-02-01", "org_id": "o"}
    _assert_pyformat_safe(query, params)  # must not raise


# ---------------------------------------------------------------------------
# Anti-false-positive: real query builders must pass the guard
# ---------------------------------------------------------------------------


def test_real_investment_breakdown_query_passes_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fetch_investment_breakdown's query must pass _assert_pyformat_safe.

    This is the main pyformat investment query.  It uses %(name)s params and
    no bare '%', so the guard must accept it without raising.
    """
    captured: dict[str, Any] = {}

    async def _stub(
        _sink: Any, query: str, params: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(investment_module, "query_dicts", _stub)

    asyncio.run(
        fetch_investment_breakdown(
            sink=None,  # type: ignore[arg-type]
            start_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_ts=datetime(2024, 2, 1, tzinfo=timezone.utc),
            scope_filter="",
            scope_params={},
            org_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
    )

    assert "query" in captured, "stub was not called"
    # Must not raise — the investment query is properly escaped.
    _assert_pyformat_safe(captured["query"], captured["params"])


def test_real_ai_detector_server_side_query_passes_guard() -> None:
    """A representative ai_detector server-side query must pass the guard.

    ai_detector queries use {name:Type} placeholders and contain bare LIKE
    '%...%' patterns.  The guard must skip validation for them (server-side
    binding path).
    """
    # Minimal representative query from ai_detector._doc_drift_opportunities,
    # which uses {repo_id:UUID} and {min_commits:UInt32} server-side placeholders
    # alongside bare LIKE patterns from _DOC_FILE_EXPR.
    query = """
        SELECT
            toString(c.repo_id) AS repo_id,
            uniqExactIf(c.hash, NOT (file_path LIKE '%.md' OR file_path LIKE '%.rst'))
                AS code_commits,
            countIf(file_path LIKE '%.md' OR file_path LIKE '%.rst') AS doc_changes
        FROM git_commits AS c
        WHERE c.committer_when >= now() - INTERVAL 30 DAY
          AND c.repo_id = {repo_id:UUID}
        GROUP BY repo_id
        HAVING code_commits >= {min_commits:UInt32} AND doc_changes = 0
        LIMIT 100
    """
    params = {
        "repo_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "min_commits": 20,
    }
    # Must not raise — server-side placeholder detected, validation skipped.
    _assert_pyformat_safe(query, params)
