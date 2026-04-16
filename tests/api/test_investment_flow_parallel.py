"""Assert build_investment_flow_response runs repo and team edge fetches in parallel."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_repo_and_team_edges_fetched_in_parallel(monkeypatch):
    import dev_health_ops.api.services.investment_flow as mod

    active = 0
    peak = 0

    async def slow_fetch(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return []
        finally:
            active -= 1

    monkeypatch.setattr(mod, "fetch_investment_subcategory_edges", slow_fetch)
    monkeypatch.setattr(mod, "fetch_investment_team_edges", slow_fetch)

    class _FakeSink:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(mod, "clickhouse_client", lambda _url: _FakeSink())
    monkeypatch.setattr(mod, "require_clickhouse_backend", lambda _s: None)
    monkeypatch.setattr(mod, "_tables_present", AsyncMock(return_value=True))
    monkeypatch.setattr(mod, "_columns_present", AsyncMock(return_value=True))
    monkeypatch.setattr(mod, "resolve_repo_filter_ids", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        mod, "build_scope_filter_multi", lambda *_a, **_kw: ("", {})
    )

    class _Scope:
        level = "org"

    filters = MagicMock()
    filters.scope = _Scope()
    filters.themes = []
    filters.subcategories = []

    monkeypatch.setattr(
        mod, "time_window", lambda _f: (date(2026, 1, 1), date(2026, 1, 7), None, None)
    )
    monkeypatch.setattr(mod, "_split_category_filters", lambda _f: ([], []))

    await mod.build_investment_flow_response(
        db_url="clickhouse://x", filters=filters, org_id="org"
    )

    assert peak >= 2, f"expected concurrent fetches, saw peak={peak}"
