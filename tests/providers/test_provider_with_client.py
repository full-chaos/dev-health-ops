"""Tests for ProviderWithClient base class."""

from __future__ import annotations

from unittest.mock import MagicMock

from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    ProviderBatch,
    ProviderCapabilities,
    ProviderWithClient,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping


class _FakeClient:
    @classmethod
    def from_env(cls) -> _FakeClient:
        return cls()


class _FakeProvider(ProviderWithClient[_FakeClient]):
    name = "fake"
    capabilities = ProviderCapabilities(work_items=True)
    client_cls = _FakeClient

    def _ingest_with_client(
        self, *, client: _FakeClient, ctx: IngestionContext
    ) -> ProviderBatch:
        return ProviderBatch()


class TestProviderWithClient:
    def test_status_mapping_lazy_default(self) -> None:
        provider = _FakeProvider()
        # First access triggers load.
        mapping = provider.status_mapping
        assert isinstance(mapping, StatusMapping)
        # Repeated access returns the same instance.
        assert provider.status_mapping is mapping

    def test_status_mapping_injected(self) -> None:
        mock = MagicMock(spec=StatusMapping)
        provider = _FakeProvider(status_mapping=mock)
        assert provider.status_mapping is mock

    def test_identity_lazy_default(self) -> None:
        provider = _FakeProvider()
        resolver = provider.identity
        assert isinstance(resolver, IdentityResolver)
        assert provider.identity is resolver

    def test_identity_injected(self) -> None:
        mock = MagicMock(spec=IdentityResolver)
        provider = _FakeProvider(identity=mock)
        assert provider.identity is mock

    def test_ingest_delegates_to_subclass(self) -> None:
        provider = _FakeProvider()
        ctx = IngestionContext(window=IngestionWindow(), repo="test/repo")
        result = provider.ingest(ctx)
        assert isinstance(result, ProviderBatch)

    def test_ingest_builds_client_via_from_env(self) -> None:
        """Verify ingest() calls client_cls.from_env() to build the client."""
        call_log: list[str] = []

        class _SpyClient:
            @classmethod
            def from_env(cls) -> _SpyClient:
                call_log.append("from_env")
                return cls()

        class _SpyProvider(ProviderWithClient[_SpyClient]):
            name = "spy"
            capabilities = ProviderCapabilities(work_items=True)
            client_cls = _SpyClient

            def _ingest_with_client(
                self, *, client: _SpyClient, ctx: IngestionContext
            ) -> ProviderBatch:
                call_log.append(f"ingest:{type(client).__name__}")
                return ProviderBatch()

        provider = _SpyProvider()
        provider.ingest(IngestionContext(window=IngestionWindow()))
        assert call_log == ["from_env", "ingest:_SpyClient"]

    def test_make_client_available_to_subclasses(self) -> None:
        """Subclasses that override ingest/iter_ingest can call _make_client."""

        class _ManualProvider(ProviderWithClient[_FakeClient]):
            name = "manual"
            capabilities = ProviderCapabilities(work_items=True)
            client_cls = _FakeClient

            def ingest(self, ctx: IngestionContext) -> ProviderBatch:
                client = self._make_client()
                assert isinstance(client, _FakeClient)
                return ProviderBatch()

        provider = _ManualProvider()
        result = provider.ingest(IngestionContext(window=IngestionWindow()))
        assert isinstance(result, ProviderBatch)
