from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.queries import filters


@pytest.mark.asyncio
async def test_fetch_filter_options_only_returns_email_shaped_developers(
    monkeypatch: pytest.MonkeyPatch,
):
    async def fake_query_dicts(
        _client: object,
        query: str,
        _params: dict[str, Any],
    ) -> list[dict[str, str]]:
        if "FROM teams" in query:
            return [{"value": "team-a"}]
        if "FROM repos" in query:
            return [{"value": "org/repo"}]
        if "author_email" in query:
            return [
                {"value": "alice@example.com"},
                {"value": "bot@users.noreply.github.com"},
                {"value": "github:octocat"},
                {"value": "Alice Smith"},
                {"value": "Alice <alice@example.com>"},
                {"value": ""},
            ]
        if "issue_type_norm" in query:
            return [{"value": "bug"}]
        if "work_item_state_durations_daily" in query:
            return [{"value": "review"}]
        raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(filters, "query_dicts", fake_query_dicts)

    options = await filters.fetch_filter_options(object(), org_id="org-1")

    assert options["developers"] == [
        "alice@example.com",
        "bot@users.noreply.github.com",
    ]
