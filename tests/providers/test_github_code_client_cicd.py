from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from dev_health_ops.exceptions import PaginationException
from dev_health_ops.providers.github.budget import (
    GITHUB_USAGE_ROUTE_FAMILY_KEYS,
    GitHubBudgetEstimator,
)
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext


def test_workflow_run_result_cap_fails_when_more_pages_exist() -> None:
    asyncio.run(_test_workflow_run_result_cap_fails_when_more_pages_exist())


async def _test_workflow_run_result_cap_fails_when_more_pages_exist() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"workflow_runs": [{"id": 1}]},
            headers={"Link": f'<{request.url}&page=2>; rel="next"'},
        )

    client = GitHubCodeClient(
        auth=GitHubAuth(token="unit-test-pat"),
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(PaginationException, match="pagination incomplete"):
            await client.get_workflow_runs("acme", "widgets", max_runs=1)
    finally:
        await client.close()


def test_cicd_producer_actuals_share_tests_estimator_admission_identity() -> None:
    asyncio.run(_test_cicd_producer_actuals_share_tests_estimator_admission_identity())


async def _test_cicd_producer_actuals_share_tests_estimator_admission_identity() -> (
    None
):
    client = GitHubCodeClient(
        auth=GitHubAuth(token="unit-test-pat"),
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"workflow_runs": [{"id": 1}]},
                headers={"X-RateLimit-Remaining": "4999"},
            )
        ),
    )
    try:
        runs = await client.get_workflow_runs("acme", "widgets", max_runs=1)
        observations = client.drain_usage_observations()
    finally:
        await client.close()

    context = SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="acme/widgets",
        provider="github",
        dataset_key="cicd",
        cost_class="medium",
        mode="incremental",
        window_start=datetime(2026, 1, 10, tzinfo=timezone.utc),
        window_end=datetime(2026, 1, 10, tzinfo=timezone.utc),
        processor_flags={},
        credential_id="credential-1",
        decrypted_credentials={"token": "unit-test-pat"},
        db_url="clickhouse://localhost/default",
    )
    estimates = GitHubBudgetEstimator().estimate(context)

    assert [run.run_id for run in runs] == ["1"]
    assert len(observations) == 1
    assert observations[0]["route_family"] == "tests"
    assert observations[0]["dimension"] == BudgetDimension.REST_CORE.value
    assert observations[0]["example_operation"].startswith("tests:")
    assert any(
        estimate.route_family == observations[0]["route_family"]
        and estimate.bucket.dimension.value == observations[0]["dimension"]
        and estimate.estimated_units > 0
        for estimate in estimates
    )
    assert {estimate.route_family for estimate in estimates} == {"tests"}
    assert "cicd" not in GITHUB_USAGE_ROUTE_FAMILY_KEYS
