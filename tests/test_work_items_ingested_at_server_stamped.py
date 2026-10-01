"""CHAOS-7265: work_items.ingested_at is stamped by the server (DEFAULT now64(3)).

Both Python work_items writers must name their columns and leave ingested_at
out of them, whatever value a caller could compute: a client-supplied value is
a client clock behind a reader's cursor. The check looks at what the writer
SENDS, so a writer that adds the column (with any value) fails it.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.metrics.sinks.clickhouse.work_graph import WorkGraphMixin
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.storage.clickhouse import ClickHouseStore


def _item() -> WorkItem:
    return WorkItem(
        work_item_id="linear:CHAOS-1",
        provider="linear",
        title="t",
        type="task",
        status="in_progress",
        status_raw="In Progress",
        org_id="org-1",
    )


def test_work_graph_sink_never_sends_ingested_at() -> None:
    sink = WorkGraphMixin.__new__(WorkGraphMixin)
    sink.client = MagicMock()

    sink.write_work_items([_item()])

    args, kwargs = sink.client.insert.call_args
    assert args[0] == "work_items"
    assert kwargs["column_names"], "the writer must name its columns"
    assert "ingested_at" not in kwargs["column_names"]


@pytest.mark.asyncio
async def test_async_store_never_sends_ingested_at() -> None:
    store = ClickHouseStore("clickhouse://localhost:8123/stats")
    captured: dict[str, Any] = {}

    async def _capture(
        table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        captured["columns"] = columns
        captured["rows"] = rows

    setattr(store, "_insert_rows", AsyncMock(side_effect=_capture))

    await store.insert_work_items([_item()])

    assert captured["columns"], "the writer must name its columns"
    assert "ingested_at" not in captured["columns"]
    assert all("ingested_at" not in row for row in captured["rows"])
