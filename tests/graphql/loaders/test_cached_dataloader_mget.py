"""Assert CachedDataLoader batches cache lookups via mget when available."""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.graphql.loaders.base import CachedDataLoader


class _FakeCache:
    def __init__(self, hits: dict[str, Any]):
        self._hits = hits
        self.get_calls = 0
        self.mget_calls = 0
        self.set_calls = 0

    def get(self, key):
        self.get_calls += 1
        return self._hits.get(key)

    def mget(self, keys):
        self.mget_calls += 1
        return [self._hits.get(k) for k in keys]

    def set(self, key, value, *_a, **_kw):
        self.set_calls += 1


class _Loader(CachedDataLoader[str, str]):
    def __init__(self, cache):
        super().__init__(org_id="o", cache=cache, cache_prefix="test")
        self.load_calls: list[list[str]] = []

    async def batch_load(self, keys):
        self.load_calls.append(list(keys))
        return [f"v:{k}" for k in keys]


@pytest.mark.asyncio
async def test_mget_used_when_available():
    # Two hits + one miss. Loader hashes keys via make_cache_key, so precompute
    # the expected keys.
    from dev_health_ops.api.graphql.loaders.base import make_cache_key

    expected = {
        make_cache_key("test", "o", "k1"): "cached-k1",
        make_cache_key("test", "o", "k2"): "cached-k2",
    }
    cache = _FakeCache(hits=expected)
    loader = _Loader(cache)

    out = await loader._load_with_cache(["k1", "k2", "k3"])

    assert cache.mget_calls == 1, "expected a single mget batch call"
    assert cache.get_calls == 0, "per-key get should not be used when mget is available"
    assert out[0] == "cached-k1"
    assert out[1] == "cached-k2"
    assert out[2] == "v:k3"  # miss triggered batch_load
    assert loader.load_calls == [["k3"]]


@pytest.mark.asyncio
async def test_falls_back_to_get_when_no_mget():
    class _NoMget:
        def __init__(self):
            self.get_calls = 0

        def get(self, key):
            self.get_calls += 1
            return None

        def set(self, *a, **kw):
            pass

    cache = _NoMget()
    loader = _Loader(cache)
    out = await loader._load_with_cache(["a", "b"])
    assert cache.get_calls == 2
    assert out == ["v:a", "v:b"]
