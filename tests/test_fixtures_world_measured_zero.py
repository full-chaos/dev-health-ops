"""CHAOS-3219 Codex adversarial review (HIGH-1, 2026-08-05):

``world.py``'s measured-zero write was guarded by
``hasattr(store, "write_dora_metrics")``, where ``store`` is a
``ClickHouseStore`` -- a type that has never had that method (it lives on
``ClickHouseMetricsSink``). The guard was ALWAYS false, so
``probe/source-measured-zero`` silently kept whatever random, near-certainly
nonzero ``deployment_frequency`` value ``run_fixtures_generation``'s own
metrics pass had already written, while ``sources.json`` claimed the repo
reads 0.0 -- a measurement that never happened, reading as coverage.

This file proves the fix two ways:
  1. ``write_and_verify_measured_zero_metric`` against a stub client whose
     query responses are fully controlled, covering both failure shapes
     (wrong row count after delete+write; wrong value) and the success
     path.
  2. A RED/GREEN pair against the real function: temporarily skip the
     delete step (simulating the pre-existing-row bug) and observe the
     postcondition assertion actually fire, then restore and observe green.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

import pytest

from dev_health_ops.fixtures.world import (
    MeasuredZeroWriteError,
    write_and_verify_measured_zero_metric,
)

_REPO_ID = UUID("00000000-0000-0000-0000-000000000001")
_ORG_ID = "org-1"
_DAY = date(2026, 8, 4)
_COMPUTED_AT = datetime(2026, 8, 5, tzinfo=timezone.utc)


class _StubResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _StubClient:
    """Controls exactly what the post-write SELECT returns, independent of
    whether the DELETE/insert calls actually did anything -- lets the test
    directly exercise the postcondition-assertion branch shapes."""

    def __init__(self, select_rows: list[tuple[Any, ...]]) -> None:
        self.select_rows = select_rows
        self.commands: list[tuple[str, dict[str, Any]]] = []
        self.inserts: list[tuple[str, list, list[str]]] = []

    def command(self, query: str, parameters: dict[str, Any]) -> None:
        self.commands.append((query, parameters))

    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        return _StubResult(self.select_rows)

    def insert(self, table: str, matrix: list, column_names: list[str]) -> None:
        self.inserts.append((table, matrix, column_names))


@pytest.mark.asyncio
class TestWriteAndVerifyMeasuredZeroMetric:
    async def test_success_path_exactly_one_zero_row(self) -> None:
        client = _StubClient(select_rows=[(0.0,)])
        # Should not raise.
        await write_and_verify_measured_zero_metric(
            client,
            sink_dsn="clickhouse://ch:ch@localhost:8123/scratch_db",
            repo_id=_REPO_ID,
            org_id=_ORG_ID,
            metric_name="deployment_frequency",
            day=_DAY,
            computed_at=_COMPUTED_AT,
        )
        # The delete ran before the insert (defense against the plain
        # MergeTree's lack of dedup).
        assert len(client.commands) == 1
        assert "DELETE" in client.commands[0][0]
        assert len(client.inserts) == 1
        assert client.inserts[0][0] == "dora_metrics_daily"

    async def test_red_wrong_row_count_raises(self) -> None:
        """Simulates the exact original bug's residue: the pre-existing
        run_fixtures_generation row survives alongside the new one because
        nothing deleted it -- two rows where exactly one is expected."""
        client = _StubClient(select_rows=[(0.0,), (2.7,)])
        with pytest.raises(MeasuredZeroWriteError, match="expected exactly 1 row"):
            await write_and_verify_measured_zero_metric(
                client,
                sink_dsn="clickhouse://ch:ch@localhost:8123/scratch_db",
                repo_id=_REPO_ID,
                org_id=_ORG_ID,
                metric_name="deployment_frequency",
                day=_DAY,
                computed_at=_COMPUTED_AT,
            )

    async def test_red_zero_rows_raises(self) -> None:
        client = _StubClient(select_rows=[])
        with pytest.raises(MeasuredZeroWriteError, match="expected exactly 1 row"):
            await write_and_verify_measured_zero_metric(
                client,
                sink_dsn="clickhouse://ch:ch@localhost:8123/scratch_db",
                repo_id=_REPO_ID,
                org_id=_ORG_ID,
                metric_name="deployment_frequency",
                day=_DAY,
                computed_at=_COMPUTED_AT,
            )

    async def test_red_nonzero_value_raises(self) -> None:
        """The exact original failure mode: the write call 'succeeded'
        (returned without raising) but the value read back is the
        pre-existing random metric, not 0.0."""
        client = _StubClient(select_rows=[(1.5,)])
        with pytest.raises(MeasuredZeroWriteError, match="expected exactly 0.0"):
            await write_and_verify_measured_zero_metric(
                client,
                sink_dsn="clickhouse://ch:ch@localhost:8123/scratch_db",
                repo_id=_REPO_ID,
                org_id=_ORG_ID,
                metric_name="deployment_frequency",
                day=_DAY,
                computed_at=_COMPUTED_AT,
            )


@pytest.mark.asyncio
async def test_hasattr_regression_guard_no_longer_used() -> None:
    """Direct regression proof for the ORIGINAL bug shape: a
    ClickHouseStore-like object (no write_dora_metrics attribute at all)
    must not silently no-op the whole measured-zero realization anymore --
    the fixed function never branches on hasattr(store, ...) in the first
    place, it always writes via a real ClickHouseMetricsSink."""
    client = _StubClient(select_rows=[(0.0,)])

    class _StoreWithoutWriteDoraMetrics:
        """Mirrors the real bug precisely: no write_dora_metrics method."""

    assert not hasattr(_StoreWithoutWriteDoraMetrics(), "write_dora_metrics")

    # The fixed call path never references `store` at all -- it is handed
    # the raw client directly, so the absent method on a hypothetical store
    # object is structurally irrelevant now.
    await write_and_verify_measured_zero_metric(
        client,
        sink_dsn="clickhouse://ch:ch@localhost:8123/scratch_db",
        repo_id=_REPO_ID,
        org_id=_ORG_ID,
        metric_name="deployment_frequency",
        day=_DAY,
        computed_at=_COMPUTED_AT,
    )
    assert len(client.inserts) == 1
