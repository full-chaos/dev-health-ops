from __future__ import annotations

from dataclasses import dataclass

import pytest

from dev_health_ops.api.graphql.caching import (
    CacheInvalidator,
    _make_cacheable,
    build_cache_key,
    cached_resolver,
)
from dev_health_ops.api.graphql.context import GraphQLContext


class _DictCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


@dataclass
class _Payload:
    value: int


def test_build_cache_key_is_deterministic_for_same_inputs():
    key1 = build_cache_key("resolver", "org-1", [1, 2], {"b": 2, "a": 1})
    key2 = build_cache_key("resolver", "org-1", [1, 2], {"a": 1, "b": 2})

    assert key1 == key2
    assert key1.startswith("gql:resolver:org-1:")


def test_make_cacheable_serializes_objects_and_lists():
    payload = _Payload(value=3)
    result = _make_cacheable({"x": payload, "list": [payload, 4]})

    assert result["x"]["__type__"] == "_Payload"
    assert result["x"]["value"] == 3
    assert result["list"][0]["value"] == 3


@pytest.mark.asyncio
async def test_cached_resolver_uses_cache_on_second_call():
    cache = _DictCache()
    context = GraphQLContext(org_id="org-1", db_url="clickhouse://localhost", cache=cache)
    calls = {"n": 0}

    @cached_resolver(ttl_seconds=60)
    async def resolve_value(ctx: GraphQLContext, x: int) -> dict:
        calls["n"] += 1
        return {"x": x, "count": calls["n"]}

    first = await resolve_value(context, 42)
    second = await resolve_value(context, 42)

    assert first == {"x": 42, "count": 1}
    assert second == first
    assert calls["n"] == 1


def test_cache_invalidator_by_tag_with_delete():
    cache = _DictCache()
    invalidator = CacheInvalidator(cache)

    invalidator.tag_key("k1", "org:o1")
    invalidator.tag_key("k2", "org:o1")
    cache.set("k1", {"a": 1})
    cache.set("k2", {"a": 2})

    count = invalidator.invalidate_tag("org:o1")

    assert count == 2
    assert cache.get("k1") is None
    assert cache.get("k2") is None
