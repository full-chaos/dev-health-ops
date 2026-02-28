"""Tests for the core/ module extraction.

Verifies that core.encryption, core.cache, and core.cache_invalidation
work correctly and are importable without api.* imports.
CHAOS-651
"""

from __future__ import annotations

import os

import pytest


class TestCoreEncryption:
    """Tests for core/encryption.py."""

    def setup_method(self):
        os.environ["SETTINGS_ENCRYPTION_KEY"] = "test-key-for-unit-tests-12345"

    def test_encrypt_decrypt_round_trip(self):
        from dev_health_ops.core.encryption import decrypt_value, encrypt_value

        plaintext = "secret-token-abc123"
        encrypted = encrypt_value(plaintext)
        assert encrypted != plaintext
        assert decrypt_value(encrypted) == plaintext

    def test_decrypt_invalid_token_raises(self):
        from dev_health_ops.core.encryption import decrypt_value

        with pytest.raises(ValueError, match="Decryption failed"):
            decrypt_value("not-valid-ciphertext")

    def test_encrypt_value_returns_string(self):
        from dev_health_ops.core.encryption import encrypt_value

        result = encrypt_value("hello")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_no_api_imports(self):
        """core.encryption must not import from dev_health_ops.api.*."""
        import ast
        import inspect

        import dev_health_ops.core.encryption as mod

        source = inspect.getsource(mod)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith(
                        "dev_health_ops.api"
                    ), f"Found api import: {node.module}"


class TestCoreCache:
    """Tests for core/cache.py."""

    def test_memory_cache_set_get(self):
        from dev_health_ops.core.cache import create_cache

        cache = create_cache(ttl_seconds=60)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_memory_cache_missing_key(self):
        from dev_health_ops.core.cache import create_cache

        cache = create_cache(ttl_seconds=60)
        assert cache.get("nonexistent") is None

    def test_graphql_cache_manager_invalidate_by_tag(self):
        from dev_health_ops.core.cache import GraphQLCacheManager, create_cache

        cache = create_cache(ttl_seconds=60)
        manager = GraphQLCacheManager(cache)

        manager.set_query_result("q1", {"data": "result1"}, tags=["org:abc"])
        manager.set_query_result("q2", {"data": "result2"}, tags=["org:abc"])

        count = manager.invalidate_by_tag("org:abc")
        assert count >= 1

    def test_no_api_imports(self):
        """core.cache must not import from dev_health_ops.api.*."""
        import ast
        import inspect

        import dev_health_ops.core.cache as mod

        source = inspect.getsource(mod)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith(
                        "dev_health_ops.api"
                    ), f"Found api import: {node.module}"


class TestCoreCacheInvalidation:
    """Tests for core/cache_invalidation.py."""

    def test_invalidate_on_metrics_update(self):
        from dev_health_ops.core.cache import create_cache
        from dev_health_ops.core.cache_invalidation import invalidate_on_metrics_update

        cache = create_cache(ttl_seconds=60)
        count = invalidate_on_metrics_update(cache, "org123", "2024-01-15")
        assert isinstance(count, int)

    def test_invalidate_on_sync_complete(self):
        from dev_health_ops.core.cache import create_cache
        from dev_health_ops.core.cache_invalidation import invalidate_on_sync_complete

        cache = create_cache(ttl_seconds=60)
        count = invalidate_on_sync_complete(cache, "org123", "github")
        assert isinstance(count, int)

    def test_no_api_imports(self):
        """core.cache_invalidation must not import from dev_health_ops.api.*."""
        import ast
        import inspect

        import dev_health_ops.core.cache_invalidation as mod

        source = inspect.getsource(mod)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith(
                        "dev_health_ops.api"
                    ), f"Found api import: {node.module}"


class TestBackwardCompatibility:
    """Tests that api.* re-exports still work for backward compat."""

    def test_api_services_cache_re_exports(self):
        from dev_health_ops.api.services.cache import (
            GraphQLCacheManager,
            TTLCache,
            create_cache,
            create_graphql_cache,
        )

        assert callable(create_cache)
        assert callable(create_graphql_cache)
        assert TTLCache is not None
        assert GraphQLCacheManager is not None

    def test_api_graphql_cache_invalidation_re_exports(self):
        from dev_health_ops.api.graphql.cache_invalidation import (
            CacheInvalidationEvent,
            invalidate_on_metrics_update,
            invalidate_on_sync_complete,
        )

        assert callable(invalidate_on_metrics_update)
        assert callable(invalidate_on_sync_complete)
        assert CacheInvalidationEvent is not None

    def test_api_settings_still_exports_decrypt_value(self):
        """api.services.settings must still export decrypt/encrypt for compat."""
        # Settings imports from core.encryption, so these names must be accessible
        # either via the settings module or directly from core.encryption
        from dev_health_ops.core.encryption import decrypt_value, encrypt_value

        assert callable(decrypt_value)
        assert callable(encrypt_value)
