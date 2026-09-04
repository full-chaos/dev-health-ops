"""CHAOS-5049: the sweep that moves dead-claim `executing` rows to `ambiguous`.

The defect these cover: every exit from `executing` is written by the process
holding the claim (`_mark_ambiguous` / `_mark_retry_authorized` / the succeeded
path all carry `WHERE state = 'executing'`). When that process dies, no writer
remains and the row stays `executing` forever — 4 rows across 4 orgs sat that way
for 7.5 days. Nothing reclaimed them because the ledger has no lease column of its
own; its liveness is derived from the partition/run tables, and nothing watches
those for expiry.

The safety line these pin: an expired lease proves the writer is GONE, not that it
wrote NOTHING. So the sweep may only reach `ambiguous`, never `retry_authorized`.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal import worker_metrics
from dev_health_ops.api.main import app

RUN_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEAD_ONE = uuid.UUID("aaaaaaaa-1111-4111-8111-111111111111")
DEAD_TWO = uuid.UUID("aaaaaaaa-2222-4222-8222-222222222222")
LIVE_ONE = uuid.UUID("bbbbbbbb-3333-4333-8333-333333333333")


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSession:
    """Returns a fixed candidate set and records commits."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.commits = 0

    async def execute(self, *_args: Any, **_kwargs: Any) -> _FakeResult:
        return _FakeResult(self._rows)

    async def commit(self) -> None:
        self.commits += 1


def _row(execution_id: uuid.UUID) -> dict[str, Any]:
    return {
        "id": execution_id,
        "run_id": RUN_ID,
        "partition_id": uuid.uuid4(),
        "claim_token": uuid.uuid4(),
        "worker_kind": "daily",
        "operation": "partition",
        "last_attempt_at": None,
    }


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    monkeypatch.setenv("WORKER_METRIC_REPAIR_TOKEN", "test-repair-token")

    async def session_override():
        yield cast(AsyncSession, object())

    app.dependency_overrides[get_postgres_session_dep] = session_override
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_postgres_session_dep, None)


@pytest.mark.asyncio
async def test_sweeps_only_rows_whose_claim_is_provably_dead() -> None:
    """A live `executing` claim is real, in-flight work — never swept."""
    session = _FakeSession([_row(DEAD_ONE), _row(LIVE_ONE), _row(DEAD_TWO)])

    async def claim_active(_session: Any, row: Any) -> bool:
        return row["id"] == LIVE_ONE

    marked: list[uuid.UUID] = []

    async def mark(_session: Any, execution_id: uuid.UUID, _detail: str) -> None:
        marked.append(execution_id)

    with (
        patch.object(worker_metrics, "_original_claim_is_active", claim_active),
        patch.object(worker_metrics, "_mark_ambiguous_by_id", mark),
    ):
        result = await worker_metrics._sweep_dead_claim_executions(
            cast(AsyncSession, session), [], 100, False
        )

    assert marked == [DEAD_ONE, DEAD_TWO]
    assert result["swept"] == 2
    assert result["skipped_claim_active"] == 1
    assert result["swept_ids"] == [str(DEAD_ONE), str(DEAD_TWO)]


@pytest.mark.asyncio
async def test_dry_run_reports_without_writing() -> None:
    """dry_run must report the same set it would sweep, and write nothing."""
    session = _FakeSession([_row(DEAD_ONE)])

    async def claim_dead(_session: Any, _row: Any) -> bool:
        return False

    mark = AsyncMock()
    with (
        patch.object(worker_metrics, "_original_claim_is_active", claim_dead),
        patch.object(worker_metrics, "_mark_ambiguous_by_id", mark),
    ):
        result = await worker_metrics._sweep_dead_claim_executions(
            cast(AsyncSession, session), [], 100, True
        )

    mark.assert_not_awaited()
    assert result["dry_run"] is True
    assert result["swept_ids"] == [str(DEAD_ONE)]


@pytest.mark.asyncio
async def test_returns_ids_because_operators_need_them_for_partition_repair() -> None:
    """Counts alone would force an operator to re-derive the list by hand.

    The swept ids are the input to the CHAOS-5042 partition repair/redrive that
    has to follow, so they are part of the contract, not a convenience.
    """
    session = _FakeSession([_row(DEAD_ONE), _row(DEAD_TWO)])

    async def claim_dead(_session: Any, _row: Any) -> bool:
        return False

    with (
        patch.object(worker_metrics, "_original_claim_is_active", claim_dead),
        patch.object(worker_metrics, "_mark_ambiguous_by_id", AsyncMock()),
    ):
        result = await worker_metrics._sweep_dead_claim_executions(
            cast(AsyncSession, session), [], 100, False
        )

    assert result["swept_ids"] == [str(DEAD_ONE), str(DEAD_TWO)]
    assert len(result["swept_ids"]) == result["swept"]


def test_sweep_can_never_authorize_retry_or_success() -> None:
    """Structural, not behavioural: the terminal states are absent from the code.

    A behavioural test would only prove the states are not reached on the paths
    it happens to exercise. This asserts they are not reachable at all, because
    neither string appears in the function's source — so widening this into an
    auto-retry path requires deliberately adding one.
    """
    import inspect

    source = inspect.getsource(worker_metrics._sweep_dead_claim_executions)
    body = source.split('"""')[2]  # drop the docstring, which discusses them
    assert "retry_authorized" not in body
    assert "succeeded" not in body


def test_endpoint_requires_the_operator_repair_token(client: TestClient) -> None:
    """Gated like /repair and /redrive, not like /execute.

    authorize_worker_bridge (what /execute uses) is the weaker gate; this verb
    advances durable ledger state, so it must sit with the repair verbs.
    """
    response = client.post(
        "/internal/worker/metric-executions/v1/sweep-dead-claims",
        json={"limit": 10, "dry_run": True},
        headers={"Authorization": "Bearer test-token"},  # bridge token, not repair
    )
    assert response.status_code in (401, 403), response.text


def test_endpoint_rejects_an_out_of_range_limit(client: TestClient) -> None:
    """The bound is part of the contract: an unbounded sweep is a long lock."""
    with patch.object(
        worker_metrics, "_sweep_dead_claim_executions", new=AsyncMock()
    ) as sweep:
        response = client.post(
            "/internal/worker/metric-executions/v1/sweep-dead-claims",
            json={"limit": 5000},
            headers={"Authorization": "Bearer test-repair-token"},
        )
    assert response.status_code == 422, response.text
    sweep.assert_not_awaited()
