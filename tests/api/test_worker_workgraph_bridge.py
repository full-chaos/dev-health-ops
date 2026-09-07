"""CHAOS-3092 (leftovers): coverage for worker_workgraph.py's surviving route.

POST /execute (and every helper that only existed to run a compatibility
subprocess for it) was deleted along with the Celery task it invoked -- see
worker_workgraph.py's module docstring. This file exists so that deletion
cannot be mistaken for removing the whole route module's test coverage:
POST /executions/{request_id}/repair stays live (dev-health-workerctl
workgraph repair is a real caller), and needs its own auth-and-response
proof now that tests/api/internal/test_worker_workgraph.py -- which was
entirely about the deleted /execute machinery -- is gone.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Generator
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.main import app

REQUEST_ID = uuid.UUID("66666666-6666-4666-8666-666666666666")


class _FakeResult:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> _FakeResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._row


def _ambiguous_row(attempt_count: int = 1) -> dict[str, Any]:
    return {
        "id": REQUEST_ID,
        "state": "ambiguous",
        "claim_token": None,
        "lease_expires_at": None,
        "ledger_state": "ambiguous",
        "attempt_count": attempt_count,
    }


class _FakeSession:
    """Scripts the exact await session.execute(...)/session.commit() sequence
    repair() issues: one SELECT ... FOR UPDATE, then an INSERT + two UPDATEs,
    then a commit -- without a real Postgres connection."""

    def __init__(self, select_row: dict[str, Any] | None) -> None:
        self._select_row = select_row
        self.executed: list[str] = []
        self.committed = False

    async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> _FakeResult:
        text = str(statement)
        self.executed.append(text)
        if "FOR UPDATE" in text:
            return _FakeResult(self._select_row)
        return _FakeResult(None)

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        pass


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-bridge-token")
    monkeypatch.setenv("WORKER_METRIC_REPAIR_TOKEN", "test-repair-token")

    async def session_override():
        yield cast(AsyncSession, object())

    app.dependency_overrides[get_postgres_session_dep] = session_override
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_postgres_session_dep, None)


def _repair_body() -> dict[str, Any]:
    return {
        "expected_attempt_count": 1,
        "resolution": "retry_safe",
        "review_evidence": "verified via worker logs, no output was ever sent",
    }


def test_workgraph_repair_requires_the_operator_repair_token(
    client: TestClient,
) -> None:
    """No Authorization header at all -- authorize_workgraph_repair fails
    closed (401) before the request body or the database is ever touched."""
    response = client.post(
        f"/internal/worker/workgraph/v1/executions/{REQUEST_ID}/repair",
        json=_repair_body(),
    )
    assert response.status_code == 401


def test_workgraph_repair_rejects_the_ordinary_bridge_token(
    client: TestClient,
) -> None:
    """The plain worker-bridge token must not double as the repair token --
    authorize_metric_repair's privilege-separation check (CHAOS-5042)."""
    response = client.post(
        f"/internal/worker/workgraph/v1/executions/{REQUEST_ID}/repair",
        headers={"Authorization": "Bearer test-bridge-token"},
        json=_repair_body(),
    )
    assert response.status_code == 401


def test_workgraph_repair_responds_and_commits_when_authorized(
    client: TestClient,
) -> None:
    """A correctly authorized repair of an unleased ambiguous row succeeds,
    proving the route still runs its real (non-mocked) SQL end to end."""
    session = _FakeSession(_ambiguous_row())

    async def session_override():
        yield session

    app.dependency_overrides[get_postgres_session_dep] = session_override
    try:
        response = client.post(
            f"/internal/worker/workgraph/v1/executions/{REQUEST_ID}/repair",
            headers={"Authorization": "Bearer test-repair-token"},
            json=_repair_body(),
        )
    finally:
        app.dependency_overrides.pop(get_postgres_session_dep, None)

    assert response.status_code == 200, response.text
    assert response.json() == {"status": "repaired", "request_id": str(REQUEST_ID)}
    assert session.committed
    # Codex r1 (P2): asserting only the COUNT let a typo'd table/column name
    # in any of the 4 statements pass silently -- a naive substring check is
    # ALSO not enough, since "work_graph_execution_repairs_typo" still
    # contains "work_graph_execution_repairs" (verified: mutating the real
    # INSERT's table name to that typo kept a substring-only assertion
    # green). \b anchors the table name on a word boundary so a suffixed
    # typo fails here, not only against real Postgres.
    assert len(session.executed) == 4
    select_sql, insert_sql, request_update_sql, ledger_update_sql = session.executed
    assert "FOR UPDATE" in select_sql
    assert re.search(r"\bwork_graph_execution_requests\b", select_sql)
    assert re.search(r"\bwork_graph_execution_ledger\b", select_sql)
    assert re.search(r"INSERT INTO\s+work_graph_execution_repairs\b", insert_sql)
    assert "SET state" in request_update_sql and re.search(
        r"UPDATE\s+work_graph_execution_requests\b", request_update_sql
    )
    assert "SET state" in ledger_update_sql and re.search(
        r"UPDATE\s+work_graph_execution_ledger\b", ledger_update_sql
    )


def test_workgraph_repair_refuses_a_row_that_is_not_unleased_ambiguous(
    client: TestClient,
) -> None:
    """A leased (claim_token set) row is not eligible -- 409, no commit."""
    row = _ambiguous_row()
    row["claim_token"] = uuid.uuid4()
    session = _FakeSession(row)

    async def session_override():
        yield session

    app.dependency_overrides[get_postgres_session_dep] = session_override
    try:
        response = client.post(
            f"/internal/worker/workgraph/v1/executions/{REQUEST_ID}/repair",
            headers={"Authorization": "Bearer test-repair-token"},
            json=_repair_body(),
        )
    finally:
        app.dependency_overrides.pop(get_postgres_session_dep, None)

    assert response.status_code == 409
    assert not session.committed
