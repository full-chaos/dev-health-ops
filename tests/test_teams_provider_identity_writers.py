"""CHAOS-9432: every Python writer of ``teams`` carries the provider identity.

``teams`` is a ReplacingMergeTree(updated_at): the newest version wins, so
``provider_team_id``, ``provider_name`` and ``gone_at`` survive a later write
only if the writer copies them from the version it replaces. A review found a
second writer (``ClickHouseCore.insert_teams``) that reset them; this module
holds both writers to the same rule and scans ``src`` for a third.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

SRC = Path(__file__).resolve().parents[1] / "src" / "dev_health_ops"
NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)
GONE = datetime(2026, 10, 1, tzinfo=timezone.utc)


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _FakeClient:
    """A stored version of one team, read by the carry query and replaced by insert."""

    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[Any]], list[str]]] = []

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> _Result:
        assert "provider_team_id" in sql
        params = parameters or {}
        assert "org-a" in params["org_ids"] and "gh:platform" in params["ids"]
        return _Result([("org-a", "gh:platform", "4711", "Platform", GONE)])

    def insert(
        self, table: str, matrix: list[list[Any]], *, column_names: list[str], **_: Any
    ) -> None:
        self.inserts.append((table, matrix, column_names))


def _core() -> tuple[ClickHouseMetricsSink, _FakeClient]:
    sink = object.__new__(ClickHouseMetricsSink)
    client = _FakeClient()
    sink.client = client  # type: ignore[attr-defined]
    return sink, client


@pytest.mark.asyncio
async def test_core_insert_teams_carries_the_stored_provider_identity() -> None:
    sink, client = _core()

    await sink.insert_teams(
        [
            {
                "id": "gh:platform",
                "name": "Edited",
                "org_id": "org-a",
                "updated_at": NOW,
            },
            {"id": "gh:new", "name": "New", "org_id": "org-a", "updated_at": NOW},
        ]
    )

    ((_, matrix, columns),) = client.inserts
    assert columns[-3:] == ["provider_team_id", "provider_name", "gone_at"]
    by_id = {row[0]: dict(zip(columns, row, strict=True)) for row in matrix}
    assert by_id["gh:platform"]["provider_team_id"] == "4711"
    assert by_id["gh:platform"]["provider_name"] == "Platform"
    assert by_id["gh:platform"]["gone_at"] == GONE
    assert (
        by_id["gh:new"]["provider_team_id"],
        by_id["gh:new"]["provider_name"],
        by_id["gh:new"]["gone_at"],
    ) == ("", "", None)


_INSERT_NAMES = {"insert", "_insert_rows", "insert_rows"}


def _inserts_into_teams(call: ast.Call) -> bool:
    """A call that writes the ``teams`` table: ``x.insert("teams", ...)`` or
    ``asyncio.to_thread(x.insert, "teams", ...)``."""
    if not any(isinstance(a, ast.Constant) and a.value == "teams" for a in call.args):
        return False
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in _INSERT_NAMES:
        return True
    return any(
        isinstance(a, ast.Attribute) and a.attr in _INSERT_NAMES for a in call.args
    )


def _teams_insert_functions() -> list[tuple[Path, str, str]]:
    """Every function in src that inserts into the ``teams`` table."""
    found: list[tuple[Path, str, str]] = []
    for path in sorted(SRC.rglob("*.py")):
        if "migrations" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        if '"teams"' not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and _inserts_into_teams(call):
                    found.append(
                        (path, node.name, ast.get_source_segment(source, node) or "")
                    )
    return found


def test_every_python_teams_writer_carries_the_provider_identity() -> None:
    writers = _teams_insert_functions()
    # A scan that finds nothing is not a pass: the writers known on 2026-10-16.
    names = {(path.name, name) for path, name, _ in writers}
    assert ("clickhouse.py", "insert_teams") in names
    assert ("core.py", "insert_teams") in names
    for path, name, body in writers:
        assert "provider_team_id" in body or "PROVIDER_IDENTITY_COLUMNS" in body, (
            f"{path.relative_to(SRC)}::{name} inserts into teams without "
            "provider_team_id, provider_name and gone_at (CHAOS-9432): carry them "
            "with storage.team_provider_identity"
        )
        assert (
            "fetch_team_provider_identity" in body
            or "_carry_team_provider_identity" in body
        ), (
            f"{path.relative_to(SRC)}::{name} does not carry the provider identity "
            "of the version it replaces"
        )
