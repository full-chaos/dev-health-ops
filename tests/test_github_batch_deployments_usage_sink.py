from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.processors import github as github_processor


def test_batch_deployments_threads_usage_sink(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_test_batch_deployments_threads_usage_sink(monkeypatch))


async def _test_batch_deployments_threads_usage_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = SimpleNamespace(
        id=1001,
        name="widgets",
        full_name="acme/widgets",
        url="https://example.com/acme/widgets",
        default_branch="main",
        language="Python",
    )
    usage_sink: list[dict[str, Any]] = []
    seen_sink: list[list[dict[str, Any]] | None] = []

    class DummyStore:
        org_id = "org-1"

        async def insert_repo(self, repo_obj: object) -> None:
            return None

        async def insert_deployments(self, deployments: object) -> None:
            return None

    class DummyConnector:
        def __init__(self, token: str) -> None:
            self.token = token

        def list_repositories(self, **kwargs: object) -> list[object]:
            return [repo]

        def close(self) -> None:
            return None

        def __enter__(self) -> DummyConnector:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.close()

    async def fake_fetch_deployments(
        connector: object,
        owner: str,
        repo_name: str,
        repo_id: object,
        max_deployments: int | None,
        since: object,
        usage_sink: list[dict[str, Any]] | None = None,
    ) -> list[object]:
        seen_sink.append(usage_sink)
        if usage_sink is not None:
            usage_sink.append({"route_family": "deployments", "request_count": 1})
        return []

    monkeypatch.setattr(github_processor, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(github_processor, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        github_processor, "_fetch_github_deployments_async", fake_fetch_deployments
    )

    await github_processor.process_github_repos_batch(
        store=DummyStore(),
        token="unit-test-pat",
        org_name="acme",
        pattern="acme/*",
        sync_git=False,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=True,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
        usage_sink=usage_sink,
    )

    assert seen_sink == [usage_sink]
    assert usage_sink == [{"route_family": "deployments", "request_count": 1}]
