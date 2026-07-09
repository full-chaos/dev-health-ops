"""Defense-in-depth org_id re-check for drilldown queries (CHAOS security sprint)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.queries.drilldown import fetch_issues, fetch_pull_requests
from dev_health_ops.api.services.auth import _current_org_id, set_current_org_id


class _Sink:
    def __init__(self) -> None:
        self.last_query: str | None = None
        self.last_params: dict[str, Any] | None = None

    def query_dicts(
        self, query: str, params: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        self.last_query = query
        self.last_params = params
        return []


@pytest.mark.asyncio
async def test_fetch_pull_requests_rejects_empty_org_id():
    try:
        _current_org_id.set(None)
        with pytest.raises(ValueError, match="org_id"):
            await fetch_pull_requests(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_pull_requests_rejects_context_mismatch():
    try:
        set_current_org_id("org-A")
        with pytest.raises(PermissionError, match="org_id mismatch"):
            await fetch_pull_requests(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="org-B",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_issues_rejects_context_mismatch():
    try:
        set_current_org_id("org-A")
        with pytest.raises(PermissionError, match="org_id mismatch"):
            await fetch_issues(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="org-B",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_issues_allows_matching_org_id():
    try:
        set_current_org_id("org-X")
        sink = _Sink()
        await fetch_issues(
            sink,
            start_day=date(2024, 1, 1),
            end_day=date(2024, 1, 2),
            scope_filter="",
            scope_params={},
            org_id="org-X",
        )
        assert sink.last_params is not None
        assert sink.last_params["org_id"] == "org-X"
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_issues_reads_team_identity_from_wita():
    try:
        set_current_org_id("org-X")
        sink = _Sink()
        await fetch_issues(
            sink,
            start_day=date(2024, 1, 1),
            end_day=date(2024, 1, 2),
            scope_filter="AND t.team_id IN %(team_ids)s",
            scope_params={"team_ids": ["team-new"]},
            org_id="org-X",
        )

        assert sink.last_query is not None
        assert "FROM work_item_cycle_times AS wct FINAL" in sink.last_query
        assert "FROM work_item_team_attributions FINAL" in sink.last_query
        assert "LEFT JOIN" in sink.last_query
        assert "nullIf(t.team_id, '') AS team_id" in sink.last_query
        assert "wct.team_id" not in sink.last_query
        assert sink.last_params is not None
        assert sink.last_params["team_ids"] == ["team-new"]
    finally:
        _current_org_id.set(None)
