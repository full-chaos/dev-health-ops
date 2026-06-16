"""Regression (CHAOS-2456): the explain cache must round-trip through a
JSON-serializing backend.

The bug: ``build_explain_response`` cached the raw ``ExplainResponse``
(a Pydantic ``BaseModel``) via ``cache.set(cache_key, response)``. The Redis
backend serializes with ``json.dumps(value)``, which cannot encode a
``BaseModel``, so every write threw ``Object of type ExplainResponse is not
JSON serializable``, was swallowed by the broad ``except``, and the explain
cache never populated.

The fix mirrors the established home-cache pattern: store
``response.model_dump(mode="json")`` and rebuild with
``ExplainResponse.model_validate(cached)`` on read. These tests lock that
contract and the observability improvement to the set-failure warning.
"""

from __future__ import annotations

import hashlib
import json
import logging
from unittest.mock import AsyncMock

import pytest

from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.api.models.schemas import Contributor, ExplainResponse
from dev_health_ops.api.services.explain import build_explain_response
from dev_health_ops.api.services.filtering import filter_cache_key
from dev_health_ops.core.cache import RedisBackend, TTLCache


def _make_explain_response() -> ExplainResponse:
    return ExplainResponse(
        metric="cycle_time",
        label="Cycle Time",
        unit="hours",
        value=42.5,
        delta_pct=-12.0,
        drivers=[
            Contributor(
                id="repo-1",
                label="api",
                value=10.0,
                delta_pct=5.0,
                evidence_link="/api/v1/drilldown/prs?metric=cycle_time",
                display_name="api-service",
            )
        ],
        contributors=[
            Contributor(
                id="repo-2",
                label="web",
                value=8.0,
                delta_pct=0.0,
                evidence_link="/api/v1/drilldown/issues?metric=cycle_time",
                display_name=None,
            )
        ],
        drilldown_links={
            "prs": "/api/v1/drilldown/prs?metric=cycle_time",
            "issues": "/api/v1/drilldown/issues?metric=cycle_time",
        },
    )


class _FakeRedisClient:
    """Minimal stand-in for the valkey client.

    Mirrors the real contract used by ``RedisBackend``: ``setex`` stores the
    already-serialized string, ``get`` returns it (``decode_responses=True``),
    so a value that isn't passed through ``json.dumps`` first will raise here
    exactly like production.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        assert isinstance(value, str)  # backend must serialize before storing
        self.store[key] = value

    def get(self, key: str) -> str | None:
        return self.store.get(key)


def _redis_backend_with_fake() -> tuple[RedisBackend, _FakeRedisClient]:
    backend = RedisBackend.__new__(RedisBackend)  # bypass real valkey connect
    fake = _FakeRedisClient()
    backend._client = fake
    backend._available = True
    return backend, fake


def test_explain_response_round_trips_through_redis_backend():
    """The explain.py pattern (model_dump on set, model_validate on get) must
    survive a JSON-serializing backend and reconstruct an equal model."""
    backend, _fake = _redis_backend_with_fake()
    response = _make_explain_response()
    key = "explain:cycle_time:abc"

    # Exactly what build_explain_response now does on the write side.
    backend.set(key, response.model_dump(mode="json"), ttl_seconds=120)

    cached = backend.get(key)
    assert isinstance(cached, dict)  # stored as JSON, read back as a dict

    # Exactly what build_explain_response now does on the cache-hit side.
    rebuilt = ExplainResponse.model_validate(cached)
    assert rebuilt == response


def test_raw_explain_response_is_not_json_serializable():
    """Documents the original failure: a raw ExplainResponse cannot be
    json.dumps'd, which is why the model_dump step is load-bearing."""
    response = _make_explain_response()
    with pytest.raises(TypeError):
        json.dumps(response)


def test_redis_set_failure_warning_is_sanitized_and_traceable(caplog):
    """Observability + privacy: the swallowed set-failure warning must carry the
    value type and a traceback, but must NOT leak the raw cache key, which embeds
    org_id and user-controlled filters via filter_cache_key."""
    backend, _fake = _redis_backend_with_fake()
    response = _make_explain_response()  # not JSON-serializable as-is
    # The segment after the prefix stands in for the sensitive serialized filter
    # payload (org_id, scope, repo, developer, ...).
    key = "explain:org-secret-and-filter-payload"

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.core.cache"):
        backend.set(key, response, ttl_seconds=120)  # the old, broken pattern

    assert len(caplog.records) == 1
    record = caplog.records[0]
    message = record.getMessage()

    # Value type + traceback are present (diagnosability).
    assert "ExplainResponse" in message
    assert record.exc_info is not None

    # Prefix retained for diagnosability...
    assert "explain:" in message
    # ...but the sensitive payload must never reach logs.
    assert "org-secret-and-filter-payload" not in message
    # A stable digest of the full key is logged so repeated failures correlate.
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    assert digest in message


def _patch_explain_queries(monkeypatch):
    """Patch build_explain_response's ClickHouse dependencies so the service runs
    without a real analytics backend. Returns the query mocks for call assertions."""

    class _FakeSinkCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc):
            return False

    scope_mock = AsyncMock(return_value=("", {}))
    value_mock = AsyncMock(return_value=10.0)
    empty_mock = AsyncMock(return_value=[])

    monkeypatch.setattr(
        "dev_health_ops.api.services.explain.clickhouse_client",
        lambda db_url: _FakeSinkCtx(),
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.explain.scope_filter_for_metric", scope_mock
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.explain.fetch_metric_value", value_mock
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.explain.fetch_metric_driver_delta", empty_mock
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.explain.fetch_metric_contributors", empty_mock
    )
    return scope_mock, value_mock, empty_mock


@pytest.mark.asyncio
async def test_build_explain_response_caches_json_and_serves_second_from_cache(
    monkeypatch,
):
    """Regression guard at the service boundary (CHAOS-2456): build_explain_response
    must store a JSON-serializable payload and serve the next identical request from
    cache. This fails if explain.py reverts to caching the raw model."""
    scope_mock, value_mock, empty_mock = _patch_explain_queries(monkeypatch)
    backend, fake = _redis_backend_with_fake()
    cache = TTLCache(ttl_seconds=120, backend=backend)
    filters = MetricFilter()

    first = await build_explain_response(
        db_url="clickhouse://localhost:9000/test",
        metric="cycle_time",
        filters=filters,
        cache=cache,
        org_id="org-1",
    )
    assert isinstance(first, ExplainResponse)

    # The service stored exactly one entry, and it is JSON (a dict), not a raw model.
    assert len(fake.store) == 1
    stored_raw = next(iter(fake.store.values()))
    assert isinstance(stored_raw, str)  # backend already json.dumps'd it
    assert isinstance(json.loads(stored_raw), dict)

    scope_mock.reset_mock()
    value_mock.reset_mock()
    empty_mock.reset_mock()

    second = await build_explain_response(
        db_url="clickhouse://localhost:9000/test",
        metric="cycle_time",
        filters=filters,
        cache=cache,
        org_id="org-1",
    )
    assert isinstance(second, ExplainResponse)
    assert second == first
    # Served from cache: no recompute.
    scope_mock.assert_not_called()
    value_mock.assert_not_called()
    empty_mock.assert_not_called()


@pytest.mark.asyncio
async def test_build_explain_response_recomputes_on_invalid_cache_entry(monkeypatch):
    """A stale/corrupt cache entry must degrade to a recompute, never raise out of
    the cache-hit path (which the endpoint would otherwise surface as a 503)."""
    scope_mock, value_mock, empty_mock = _patch_explain_queries(monkeypatch)
    backend, _fake = _redis_backend_with_fake()
    cache = TTLCache(ttl_seconds=120, backend=backend)
    filters = MetricFilter()

    # Pre-seed the exact key with a value that won't validate as ExplainResponse.
    cache_key = filter_cache_key(
        "explain", "org-1", filters, extra={"metric": "cycle_time"}
    )
    cache.set(cache_key, {"garbage": "not-an-explain-response"})

    result = await build_explain_response(
        db_url="clickhouse://localhost:9000/test",
        metric="cycle_time",
        filters=filters,
        cache=cache,
        org_id="org-1",
    )

    assert isinstance(result, ExplainResponse)  # recomputed, not crashed
    value_mock.assert_awaited()  # the recompute path actually ran
