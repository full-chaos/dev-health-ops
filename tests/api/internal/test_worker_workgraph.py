from __future__ import annotations

import uuid
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.internal import worker_workgraph
from dev_health_ops.api.internal.worker_workgraph import (
    ExecuteRequest,
    _evidence,
    _run_sync,
    _scope_arguments,
)


def test_scope_arguments_reloads_only_allowlisted_workgraph_fields() -> None:
    row = {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "model_ref": "gpt-test",
        "llm_concurrency": 2,
    }
    assert _scope_arguments(
        "workgraph.build",
        {"from_date": "2026-07-01", "heuristic_window": 7},
        row,
    ) == {
        "from_date": "2026-07-01",
        "heuristic_window": 7,
        "org_id": "00000000-0000-4000-8000-000000000009",
    }


def test_scope_arguments_rejects_callable_or_credential_injection() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _scope_arguments(
            "investment.materialize",
            {"from_date": "2026-07-01", "callable": "os.system"},
            {
                "org_id": "00000000-0000-4000-8000-000000000009",
                "model_ref": "gpt-test",
                "llm_concurrency": 1,
            },
        )


def test_evidence_is_canonical_and_bounded() -> None:
    assert _evidence({"z": 1, "a": ["evidence"]}) == {
        "a": ["evidence"],
        "z": 1,
    }
    with pytest.raises(ValueError, match="durable bound"):
        _evidence({"output": "x" * 5000})


def test_river_investment_dispatch_runs_sequentially_without_celery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import work_graph_tasks

    calls: list[tuple[str, dict[str, object]]] = []

    def record(name: str) -> Callable[..., dict[str, object]]:
        def run(**kwargs: object) -> dict[str, object]:
            calls.append((name, kwargs))
            return {"status": "success", "operation": name}

        return run

    monkeypatch.setattr(work_graph_tasks.run_work_graph_build, "run", record("build"))
    monkeypatch.setattr(
        work_graph_tasks.run_investment_materialize, "run", record("materialize")
    )
    monkeypatch.setattr(
        work_graph_tasks.run_membership_backfill, "run", record("membership")
    )
    monkeypatch.setattr(
        work_graph_tasks.dispatch_investment_materialize_partitioned,
        "run",
        lambda **_kwargs: pytest.fail("River dispatch called the Celery chord task"),
    )

    outcome = _run_sync(
        "investment.dispatch",
        {
            "org_id": "00000000-0000-4000-8000-000000000009",
            "from_date": "2026-07-01",
            "to_date": "2026-07-14",
            "llm_model": "gpt-test",
            "llm_concurrency": 2,
            "run_membership_backfill_after": True,
        },
    )

    assert [name for name, _kwargs in calls] == [
        "build",
        "materialize",
        "membership",
    ]
    assert calls[0][1] == {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "from_date": "2026-07-01",
        "to_date": "2026-07-14",
    }
    assert "llm_model" in calls[1][1]
    assert calls[2][1] == {"org_id": "00000000-0000-4000-8000-000000000009"}
    assert outcome["status"] == "success"


@pytest.mark.asyncio
async def test_execute_releases_read_transaction_before_long_running_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": uuid.UUID("00000000-0000-4000-8000-000000000101"),
        "org_id": uuid.UUID("00000000-0000-4000-8000-000000000009"),
        "kind": "investment.dispatch",
        "scope": {"run_membership_backfill_after": True},
        "model_ref": None,
        "prompt_ref": None,
        "llm_concurrency": 1,
        "spend_limit_microunits": 0,
        "claim_token": uuid.UUID("00000000-0000-4000-8000-000000000102"),
    }
    session.execute.return_value = result
    monkeypatch.setattr(worker_workgraph, "authorize_worker_bridge", lambda _auth: None)

    async def run_after_transaction_release(
        _function: object, _kind: str, _arguments: dict[str, object]
    ) -> dict[str, object]:
        session.rollback.assert_awaited_once()
        return {"status": "success"}

    monkeypatch.setattr(
        worker_workgraph.asyncio, "to_thread", run_after_transaction_release
    )

    response = await worker_workgraph.execute(
        ExecuteRequest(
            request_id=uuid.UUID("00000000-0000-4000-8000-000000000101"),
            claim_token=uuid.UUID("00000000-0000-4000-8000-000000000102"),
        ),
        session,
        "Bearer test",
    )

    assert response["status"] == "success"
    statement = str(session.execute.await_args.args[0])
    assert "FOR UPDATE" not in statement
