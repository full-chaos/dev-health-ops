"""Assert CacheBackend exposes mget with correct semantics."""

from __future__ import annotations

from dev_health_ops.core.cache import MemoryBackend


def test_memory_mget_returns_aligned_list():
    be = MemoryBackend()
    be.set("a", 1, ttl_seconds=60)
    be.set("c", 3, ttl_seconds=60)

    got = be.mget(["a", "b", "c"])
    assert got == [1, None, 3]


def test_memory_mget_empty_keys():
    be = MemoryBackend()
    assert be.mget([]) == []


def test_memory_mget_all_missing():
    be = MemoryBackend()
    assert be.mget(["x", "y"]) == [None, None]
