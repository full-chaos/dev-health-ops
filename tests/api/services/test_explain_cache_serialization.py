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

import json
import logging

import pytest

from dev_health_ops.api.models.schemas import Contributor, ExplainResponse
from dev_health_ops.core.cache import RedisBackend


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


def test_redis_set_failure_warning_includes_key_and_value_type(caplog):
    """Observability fix: the swallowed set-failure warning must now carry the
    cache key and value type so the failure is traceable from logs alone."""
    backend, _fake = _redis_backend_with_fake()
    response = _make_explain_response()  # not JSON-serializable as-is
    key = "explain:cycle_time:bad"

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.core.cache"):
        backend.set(key, response, ttl_seconds=120)  # the old, broken pattern

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert key in record.getMessage()
    assert "ExplainResponse" in record.getMessage()
    assert record.exc_info is not None  # traceback is attached
