from __future__ import annotations

import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal import worker_metrics
from dev_health_ops.api.main import app
from dev_health_ops.metrics.remaining_scope_contract import CapacityScope

RUN_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
PARTITION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CLAIM_TOKEN = uuid.UUID("33333333-3333-4333-8333-333333333333")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")

    async def session_override():
        yield cast(AsyncSession, object())

    app.dependency_overrides[get_postgres_session_dep] = session_override
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_postgres_session_dep, None)


def _execution(*, family: str = "capacity") -> worker_metrics._Execution:
    scope = {
        "version": 1,
        "team_id": "44444444-4444-4444-8444-444444444444",
        "history_days": 90,
        "simulations": 1000,
        "all_teams": False,
    }
    digest = worker_metrics._scope_digest(scope)
    return worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="remaining",
            operation="partition",
            run_id=RUN_ID,
            partition_id=PARTITION_ID,
            family=family,
            generation="generation-v1",
            scope_digest=digest,
        ),
        worker_kind="remaining",
        operation="partition",
        run_id=RUN_ID,
        partition_id=PARTITION_ID,
        organization_id="55555555-5555-4555-8555-555555555555",
        family=family,
        generation="generation-v1",
        claim_token=CLAIM_TOKEN,
        scope=scope,
        scope_digest=digest,
        generation_seed=1234,
    )


def test_metric_bridge_requires_shared_token_before_loading_state(
    client: TestClient,
) -> None:
    with patch.object(
        worker_metrics, "_load_remaining_execution", new_callable=AsyncMock
    ) as load:
        response = client.post(
            "/internal/worker/remaining-metrics/v1/execute",
            headers={"Authorization": "Bearer wrong"},
            json={
                "operation": "partition",
                "run_id": str(RUN_ID),
                "partition_id": str(PARTITION_ID),
            },
        )
    assert response.status_code == 401
    load.assert_not_awaited()


@pytest.mark.parametrize(
    "payload",
    [
        {
            "operation": "partition",
            "run_id": str(RUN_ID),
            "partition_id": str(PARTITION_ID),
            "db_url": "clickhouse://attacker",
        },
        {
            "operation": "partition",
            "run_id": str(RUN_ID),
            "partition_id": str(PARTITION_ID),
            "family": "capacity",
        },
        {
            "operation": "command",
            "run_id": str(RUN_ID),
            "partition_id": str(PARTITION_ID),
        },
    ],
)
def test_metric_bridge_rejects_unknown_or_selectable_execution_fields(
    client: TestClient, payload: dict[str, str]
) -> None:
    response = client.post(
        "/internal/worker/remaining-metrics/v1/execute",
        headers={"Authorization": "Bearer test-token"},
        json=payload,
    )
    assert response.status_code == 422


def test_remaining_execution_rejects_unknown_persisted_family() -> None:
    with pytest.raises(ValueError, match="unknown remaining metrics family"):
        worker_metrics._execution_from_row(
            worker_kind="remaining",
            operation="partition",
            row={
                "run_id": RUN_ID,
                "org_id": "55555555-5555-4555-8555-555555555555",
                "family": "command",
                "generation": "generation-v1",
                "generation_seed": None,
                "scope": {"version": 1},
                "claim_token": CLAIM_TOKEN,
            },
            partition_id=PARTITION_ID,
        )


def test_remaining_runner_is_a_closed_eight_family_allowlist() -> None:
    assert set(worker_metrics._REMAINING_RUNNERS) == {
        "capacity",
        "complexity",
        "dora",
        "extra_metrics",
        "membership_backfill",
        "recommendations",
        "release_impact",
        "team_metrics",
    }


@pytest.mark.asyncio
async def test_capacity_adapter_passes_persisted_generation_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/default")
    run = AsyncMock(return_value=[])
    scope = CapacityScope.model_validate(_execution().scope)
    with patch("dev_health_ops.metrics.job_capacity.run_capacity_forecast", run):
        evidence = await worker_metrics._run_capacity(_execution(), scope)
    assert evidence == {"family": "capacity", "forecast_count": 0}
    assert run.await_args is not None
    assert run.await_args.kwargs["seed"] == 1234
    assert run.await_args.kwargs["db_url"] == "clickhouse://example/default"


@pytest.mark.asyncio
async def test_effect_then_exception_is_fenced_as_ambiguous_on_retry() -> None:
    execution = _execution()
    effects: list[str] = []

    async def effect_then_crash(_: worker_metrics._Execution) -> dict[str, Any]:
        effects.append("append-output")
        raise RuntimeError("killed after effect")

    with (
        patch.object(
            worker_metrics,
            "_reserve_execution",
            new=AsyncMock(return_value="execute"),
        ),
        patch.object(
            worker_metrics, "_mark_ambiguous", new_callable=AsyncMock
        ) as mark_ambiguous,
    ):
        with pytest.raises(HTTPException) as first:
            await worker_metrics._execute(
                cast(AsyncSession, object()), execution, effect_then_crash
            )
    assert first.value.status_code == 503
    mark_ambiguous.assert_awaited_once()
    assert effects == ["append-output"]

    async def must_not_repeat(_: worker_metrics._Execution) -> dict[str, Any]:
        effects.append("duplicate-output")
        return {}

    with patch.object(
        worker_metrics,
        "_reserve_execution",
        new=AsyncMock(
            side_effect=HTTPException(
                status_code=409,
                detail={"execution_id": str(execution.id), "state": "ambiguous"},
            )
        ),
    ):
        with pytest.raises(HTTPException) as retry:
            await worker_metrics._execute(
                cast(AsyncSession, object()), execution, must_not_repeat
            )
    assert retry.value.status_code == 409
    assert effects == ["append-output"]


class _Result:
    def __init__(self, *, scalar: Any = None, row: dict[str, Any] | None = None):
        self._scalar = scalar
        self._row = row

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def mappings(self) -> _Result:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._row


class _Session:
    def __init__(self, results: list[_Result]):
        self.results = iter(results)
        self.commits = 0

    async def execute(self, *_args: Any, **_kwargs: Any) -> _Result:
        return next(self.results)

    async def commit(self) -> None:
        self.commits += 1


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["executing", "ambiguous"])
async def test_ambiguous_ledger_row_never_reexecutes(state: str) -> None:
    execution = _execution()
    existing = {
        "worker_kind": execution.worker_kind,
        "operation": execution.operation,
        "run_id": execution.run_id,
        "partition_id": execution.partition_id,
        "family": execution.family,
        "generation": execution.generation,
        "scope_digest": execution.scope_digest,
        "state": state,
    }
    session = _Session([_Result(), _Result(row=existing), _Result()])
    with pytest.raises(HTTPException) as exc:
        await worker_metrics._reserve_execution(cast(AsyncSession, session), execution)
    assert exc.value.status_code == 409
    assert isinstance(exc.value.detail, dict)
    assert exc.value.detail["state"] == state
    assert session.commits == 1


def test_execution_ledger_migration_has_attempt_and_exact_output_state() -> None:
    migration = (
        Path(__file__).parents[2] / "src/dev_health_ops/alembic/versions/"
        "0059_add_metric_compatibility_execution_ledger.py"
    ).read_text()
    assert 'down_revision: str | None = "0058"' in migration
    assert "attempt_count integer NOT NULL DEFAULT 1" in migration
    assert "state IN ('executing', 'succeeded', 'ambiguous')" in migration
    assert "output_evidence jsonb NULL" in migration
    assert "claim_token uuid NOT NULL" in migration


def test_daily_expiry_query_is_a_database_time_fence() -> None:
    source = Path(worker_metrics.__file__).read_text()
    assert "p.lease_expires_at > statement_timestamp()" in source
    assert "r.finalization_lease_expires_at > statement_timestamp()" in source
