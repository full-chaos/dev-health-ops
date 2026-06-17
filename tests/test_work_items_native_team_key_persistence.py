"""Persistence coverage for the CHAOS-2467 raw work_items columns.

Migration 050 adds ``native_team_key`` and ``project_name`` to the raw
``work_items`` table. These tests prove BOTH raw-work-item write paths
serialize the new columns (with their values), so a normalize -> persist ->
reload round-trip no longer drops the Linear team key or the project name.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.metrics.sinks.clickhouse.work_graph import WorkGraphMixin
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.storage.clickhouse import ClickHouseStore


def _linear_work_item() -> WorkItem:
    return WorkItem(
        work_item_id="linear:CHAOS-1",
        provider="linear",
        title="In-project issue",
        type="task",
        status="in_progress",
        status_raw="In Progress",
        native_team_key="CHAOS",
        project_key=None,
        project_id="proj-uuid-123",
        project_name="Q1 Platform Revamp",
        org_id="org-1",
    )


def test_work_graph_sink_persists_native_team_key_and_project_name() -> None:
    sink = WorkGraphMixin.__new__(WorkGraphMixin)
    sink.client = MagicMock()

    sink.write_work_items([_linear_work_item()])

    args, kwargs = sink.client.insert.call_args
    assert args[0] == "work_items"
    column_names = kwargs["column_names"]
    matrix = args[1]

    assert "native_team_key" in column_names
    assert "project_name" in column_names

    row = matrix[0]
    assert row[column_names.index("native_team_key")] == "CHAOS"
    assert row[column_names.index("project_name")] == "Q1 Platform Revamp"
    # The team key is NOT smuggled back into project_key.
    assert row[column_names.index("project_key")] == ""
    assert row[column_names.index("project_id")] == "proj-uuid-123"


@pytest.mark.asyncio
async def test_async_store_insert_persists_native_team_key_and_project_name() -> None:
    store = ClickHouseStore("clickhouse://localhost:8123/stats")
    captured: dict[str, Any] = {}

    async def _capture(
        table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        captured["table"] = table
        captured["columns"] = columns
        captured["rows"] = rows

    setattr(store, "_insert_rows", AsyncMock(side_effect=_capture))

    await store.insert_work_items([_linear_work_item()])

    columns = captured["columns"]
    rows = captured["rows"]
    assert captured["table"] == "work_items"
    assert "native_team_key" in columns
    assert "project_name" in columns

    row = rows[0]
    assert row["native_team_key"] == "CHAOS"
    assert row["project_name"] == "Q1 Platform Revamp"
    assert row["project_key"] == ""
    assert row["project_id"] == "proj-uuid-123"
