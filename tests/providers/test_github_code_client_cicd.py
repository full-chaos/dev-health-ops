from __future__ import annotations

import asyncio

import httpx
import pytest

from dev_health_ops.exceptions import PaginationException
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient


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
