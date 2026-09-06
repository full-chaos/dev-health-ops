"""Tests for CHAOS-2293: aliased repo filters must not corrupt param bindings.

``repo_filter.replace("repo_id", "p.repo_id")`` rewrote the ``{repo_id:UUID}``
parameter name to ``{p.repo_id:UUID}``; ClickHouse rejects dotted parameter
names with SYNTAX_ERROR (code 62), failing every per-repo daily metrics run.

This file's testops-specific coverage (load_testops_pipeline_data/
load_testops_test_data/load_testops_historical_failed_case_names -- repo
filter param naming, row caps, historical aggregation) was deleted by
CHAOS-5245 alongside those loader methods themselves: their only production
caller (job_daily.py) is gone, and the native Go readers in
testops_native_clickhouse.go are the replacement. load_work_items's own
CHAOS-4361 coverage below is unrelated and survives.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

# Matches any ClickHouse parameter binding whose name contains a dot,
# e.g. "{p.repo_id:UUID}" — always a syntax error server-side.
DOTTED_PARAM = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_.]+:")


@pytest.fixture()
def mock_query_dicts():
    """Patch _clickhouse_query_dicts to capture SQL and params."""
    with patch(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        new_callable=AsyncMock,
        return_value=[],
    ) as mock:
        yield mock


@pytest.mark.asyncio
async def test_load_work_items_excludes_description_and_bounds_block_size(
    mock_query_dicts,
):
    """CHAOS-4361: `description` (the one unbounded Nullable(String) column
    on `work_items` -- full issue/PR body text) must never leave ClickHouse
    for this read. Neither of load_work_items's two callers in job_daily.py
    reads `.description` -- one only collects `work_item_id` for dependency
    source-id lookups, the other feeds bug-MTTR/team-attribution/cycle-time
    computes, none of which touch it. `SELECT * EXCEPT (description)` (not a
    hand-maintained column list) plus a query-time `max_block_size` bound
    the peak size of any single `read_str_col` call -- the 2026-08-27
    incident's traceback -- for a long-lived repo's open-item backlog."""
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    await loader.load_work_items(start, end, repo_id=uuid.uuid4())

    assert mock_query_dicts.call_count == 2
    item_sql = mock_query_dicts.call_args_list[0].args[1]
    trans_sql = mock_query_dicts.call_args_list[1].args[1]
    assert "SELECT * EXCEPT (description)" in item_sql
    assert "description" not in trans_sql.replace("SELECT * EXCEPT (description)", "")
    for sql in (item_sql, trans_sql):
        assert "SETTINGS max_block_size = 8192" in sql
        assert not DOTTED_PARAM.search(sql), (
            f"dotted param binding leaked into SQL: {sql}"
        )


@pytest.mark.asyncio
async def test_load_work_items_row_without_description_key_builds_a_valid_work_item(
    mock_query_dicts,
):
    """A row dict that never carries a `description` key (exactly what the
    trimmed SELECT now returns) must still build a `WorkItem` -- the field
    has a `None` default, and `to_dataclass` only sets keys present in the
    row, so an absent key is not the same failure mode as an explicit
    `None` value and needs its own coverage."""
    mock_query_dicts.side_effect = [
        [
            {
                "repo_id": str(uuid.uuid4()),
                "work_item_id": "gh:owner/repo#1",
                "provider": "github",
                "title": "some title",
                "type": "bug",
                "status": "open",
                "status_raw": "open",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
        [],  # transitions: unused by this test
    ]
    loader = ClickHouseDataLoader(client=object(), org_id="acme-corp")
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    items, _ = await loader.load_work_items(start, end, repo_id=uuid.uuid4())

    assert len(items) == 1
    assert items[0].description is None
    assert items[0].work_item_id == "gh:owner/repo#1"
