"""
Unit tests for the provider contract (base, registry).

These tests verify:
1. Registry resolves providers correctly
2. The base Provider contract (capabilities, ingest/iter_ingest, batch shape)
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    Provider,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.registry import (
    get_provider,
    is_registered,
    list_providers,
    register_provider,
)


class TestProviderCapabilities:
    def test_default_capabilities(self) -> None:
        caps = ProviderCapabilities()
        assert caps.work_items is True
        assert caps.status_transitions is True
        assert caps.dependencies is False
        assert caps.interactions is False
        assert caps.sprints is False
        assert caps.reopen_events is False
        assert caps.priority is False

    def test_custom_capabilities(self) -> None:
        caps = ProviderCapabilities(
            work_items=True,
            status_transitions=True,
            dependencies=True,
            interactions=True,
            sprints=True,
            reopen_events=True,
            priority=True,
        )
        assert caps.dependencies is True
        assert caps.interactions is True
        assert caps.sprints is True


class TestIngestionContext:
    def test_empty_context(self) -> None:
        ctx = IngestionContext(window=IngestionWindow())
        assert ctx.window.updated_since is None
        assert ctx.window.active_until is None
        assert ctx.project_key is None
        assert ctx.repo is None
        assert ctx.group is None
        assert ctx.limit is None

    def test_full_context(self) -> None:
        since = datetime(2025, 1, 1, tzinfo=timezone.utc)
        until = datetime(2025, 1, 31, tzinfo=timezone.utc)
        ctx = IngestionContext(
            window=IngestionWindow(updated_since=since, active_until=until),
            project_key="TEST",
            repo="owner/repo",
            group="my-group",
            limit=100,
        )
        assert ctx.window.updated_since == since
        assert ctx.window.active_until == until
        assert ctx.project_key == "TEST"
        assert ctx.repo == "owner/repo"
        assert ctx.group == "my-group"
        assert ctx.limit == 100


class TestProviderBatch:
    def test_empty_batch(self) -> None:
        batch = ProviderBatch()
        assert batch.work_items == []
        assert batch.status_transitions == []
        assert batch.dependencies == []
        assert batch.interactions == []
        assert batch.sprints == []
        assert batch.reopen_events == []

    def test_batch_with_items(self) -> None:
        mock_item = MagicMock()
        mock_transition = MagicMock()
        batch = ProviderBatch(
            work_items=[mock_item],
            status_transitions=[mock_transition],
        )
        assert len(batch.work_items) == 1
        assert len(batch.status_transitions) == 1


class TestProviderIterIngest:
    def test_default_iter_ingest_wraps_ingest(self) -> None:
        class DummyProvider(Provider):
            name = "dummy-iter"
            capabilities = ProviderCapabilities()

            def ingest(self, ctx: IngestionContext) -> ProviderBatch:
                return expected_batch

        expected_batch = ProviderBatch(work_items=[MagicMock()])
        provider = DummyProvider()
        ctx = IngestionContext(window=IngestionWindow())

        batches = list(provider.iter_ingest(ctx))

        assert len(batches) == 1
        assert batches[0] is expected_batch


class TestProviderRegistry:
    def test_github_is_registered(self) -> None:
        assert is_registered("github")
        assert is_registered("GITHUB")  # case-insensitive

    def test_list_providers_includes_github(self) -> None:
        providers = list_providers()
        assert "github" in providers

    def test_get_unknown_provider_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown provider 'nonexistent'"):
            get_provider("nonexistent")

    def test_register_custom_provider(self) -> None:
        class DummyProvider(Provider):
            name = "dummy"
            capabilities = ProviderCapabilities()

            def ingest(self, ctx: IngestionContext) -> ProviderBatch:
                return ProviderBatch()

        register_provider("dummy", lambda: DummyProvider())

        assert is_registered("dummy")
        provider = get_provider("dummy")
        assert provider.name == "dummy"
