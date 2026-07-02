"""Tests for CHAOS-2778: fixtures must not silently mix into a synced org.

Synthetic fixture units written into an org that already holds live
connector-synced data (github/gitlab/jira/linear) pollute Investment
allocation and every team/repo rollup with repos and teams that do not exist
for that tenant. `dev-hops fixtures generate` therefore refuses such orgs
unless --allow-mixed-org is passed.

Covers:
1. _detect_live_providers finds live providers via the ClickHouse client and
   ignores non-live labels.
2. Non-ClickHouse stores skip the scan (empty set — guard inert).
3. Missing tables (fresh database) are treated as "nothing synced yet".
4. _ensure_org_unpolluted raises MixedOrgError listing the providers found.
5. allow_mixed_org=True bypasses the scan entirely.
6. run_fixtures_generation surfaces the refusal as exit code 1 before any
   generation work happens.
7. The generate parser wires --allow-mixed-org (default False).
"""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from dev_health_ops.fixtures.runner import (
    LIVE_PROVIDERS,
    MixedOrgError,
    _detect_live_providers,
    _ensure_org_unpolluted,
    register_commands,
    run_fixtures_generation,
)
from dev_health_ops.storage import ClickHouseStore

_ORG = "70d529e0-3c06-4597-8480-794fd02328b6"


class _StubResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _StubClient:
    """Mimics the sync clickhouse-connect client surface used by the guard."""

    def __init__(self, rows_by_table: dict[str, list[tuple[Any, ...]]]) -> None:
        self.rows_by_table = rows_by_table
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        self.queries.append((query, parameters))
        for table, rows in self.rows_by_table.items():
            if f"FROM {table} " in query or query.rstrip().endswith(f"FROM {table}"):
                return _StubResult(rows)
        raise RuntimeError(f"no stub rows for query: {query}")


def _stub_store(
    rows_by_table: dict[str, list[tuple[Any, ...]]],
) -> tuple[ClickHouseStore, _StubClient]:
    store = ClickHouseStore("clickhouse://stub:8123/stub")
    client = _StubClient(rows_by_table)
    store.client = client
    return store, client


@pytest.mark.asyncio
async def test_detect_live_providers_finds_synced_providers() -> None:
    store, client = _stub_store(
        {"work_items": [("github",), ("linear",)], "repos": [("github",)]}
    )
    found = await _detect_live_providers(store, _ORG)
    assert found == {"github", "linear"}
    # Both tables are scanned, parameterized with the org and live-provider set.
    assert len(client.queries) == 2
    for _, params in client.queries:
        assert params["org_id"] == _ORG
        assert params["live"] == list(LIVE_PROVIDERS)


@pytest.mark.asyncio
async def test_detect_live_providers_empty_for_fixture_only_org() -> None:
    store, _ = _stub_store({"work_items": [], "repos": []})
    assert await _detect_live_providers(store, _ORG) == set()


@pytest.mark.asyncio
async def test_detect_live_providers_skips_non_clickhouse_store() -> None:
    class NotClickHouse:
        client = _StubClient({"work_items": [("github",)]})

    assert await _detect_live_providers(NotClickHouse(), _ORG) == set()


@pytest.mark.asyncio
async def test_detect_live_providers_tolerates_missing_tables() -> None:
    class _ExplodingClient:
        def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
            raise RuntimeError("Table default.work_items does not exist")

    store = ClickHouseStore("clickhouse://stub:8123/stub")
    store.client = _ExplodingClient()
    assert await _detect_live_providers(store, _ORG) == set()


@pytest.mark.asyncio
async def test_detect_live_providers_fails_closed_on_connectivity_error() -> None:
    """A transient outage must NOT silently disarm the guard (codex HIGH)."""

    class _DownClient:
        def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
            raise RuntimeError("Connection refused: clickhouse:8123")

    store = ClickHouseStore("clickhouse://stub:8123/stub")
    store.client = _DownClient()
    with pytest.raises(RuntimeError, match="Connection refused"):
        await _detect_live_providers(store, _ORG)


@pytest.mark.asyncio
async def test_ensure_org_unpolluted_raises_with_provider_names() -> None:
    store, _ = _stub_store({"work_items": [("linear",)], "repos": [("github",)]})
    with pytest.raises(MixedOrgError) as excinfo:
        await _ensure_org_unpolluted(store, _ORG, allow_mixed_org=False)
    message = str(excinfo.value)
    assert _ORG in message
    assert "github" in message and "linear" in message
    assert "--allow-mixed-org" in message


@pytest.mark.asyncio
async def test_allow_mixed_org_bypasses_scan() -> None:
    class _MustNotQuery:
        def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
            raise AssertionError("scan must not run when allow_mixed_org=True")

    store = ClickHouseStore("clickhouse://stub:8123/stub")
    store.client = _MustNotQuery()
    await _ensure_org_unpolluted(store, _ORG, allow_mixed_org=True)


def _generate_namespace(**overrides: Any) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    ns = parser.parse_args(
        ["fixtures", "generate", "--sink", "clickhouse://stub:8123/stub"]
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


@pytest.mark.asyncio
async def test_run_fixtures_generation_refuses_mixed_org(monkeypatch) -> None:
    store, client = _stub_store({"work_items": [("github",)], "repos": []})

    async def _fake_run_with_store(db_url, db_type, handler, org_id):
        return await handler(store)

    monkeypatch.setattr(
        "dev_health_ops.fixtures.runner.run_with_store", _fake_run_with_store
    )
    ns = _generate_namespace(org=_ORG)
    assert await run_fixtures_generation(ns) == 1
    # The refusal happened before any generation writes: the only client calls
    # were the two guard scans.
    assert len(client.queries) == 2


def test_generate_parser_wires_allow_mixed_org_flag() -> None:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    default_ns = parser.parse_args(["fixtures", "generate"])
    assert default_ns.allow_mixed_org is False
    flagged_ns = parser.parse_args(["fixtures", "generate", "--allow-mixed-org"])
    assert flagged_ns.allow_mixed_org is True
