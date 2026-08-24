"""Home dashboard cache must react to a sync-run finalize (CHAOS-4226).

Before this change the home view's TTLCache entry (Valkey-backed in
production) was keyed only by (prefix, org, filters). A scheduled sync run's
finalize bumped ``sync_coverage_projections.invalidated_at`` in Postgres and
nothing else, so the next ``/home`` read served the pre-finalize response
until the TTL expired. The Go finalize now bumps a per-org cache epoch key in
Valkey and the Python read path folds that epoch into the cache key, so the
first read after a finalize misses and recomputes.

These tests simulate the Go side by writing the epoch key exactly as the
cross-language contract defines it (``core.cache.org_cache_epoch_key`` is the
producer of ``contracts/cache-invalidation/v1/org_cache_epoch_key.json``).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast

import fakeredis
import pytest

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
from dev_health_ops.api.models.schemas import (
    ConstraintCard,
    Coverage,
    Freshness,
    HomeResponse,
)
from dev_health_ops.api.services import home as home_service
from dev_health_ops.api.services.filtering import epoch_cache_key, filter_cache_key
from dev_health_ops.core.cache import (
    ORG_CACHE_EPOCH_TTL_SECONDS,
    RedisBackend,
    TTLCache,
    org_cache_epoch_key,
)

ORG = "org-4226"


class RecomputeAttempted(Exception):
    """Raised by the stubbed ClickHouse client: the read path went past the cache."""


def _filters() -> MetricFilter:
    return MetricFilter(
        time=TimeFilter(range_days=14, compare_days=14),
        scope=ScopeFilter(level="org", ids=[]),
    )


def _response(repos_covered_pct: float) -> HomeResponse:
    return HomeResponse(
        freshness=Freshness(
            last_ingested_at=datetime(2026, 8, 22, 0, 7, tzinfo=UTC),
            latest_successful_sync_at=None,
            sources={},
            coverage=Coverage(
                repos_covered_pct=repos_covered_pct,
                prs_linked_to_issues_pct=100,
                issues_with_cycle_states_pct=100,
            ),
        ),
        deltas=[],
        summary=[],
        tiles={},
        constraint=ConstraintCard(title="", claim="", evidence=[], experiments=[]),
        events=[],
    )


def _valkey_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TTLCache, fakeredis.FakeValkey]:
    """The REAL RedisBackend (json round-trip, INCR/EXPIRE) over fakeredis --
    the production shape (api/main.py HOME_CACHE, ttl 60s), not the memory
    backend."""
    fake = fakeredis.FakeValkey(decode_responses=True)
    monkeypatch.setattr("valkey.from_url", lambda *_a, **_k: fake, raising=False)
    backend = RedisBackend("redis://cache-invalidation-test/1")
    assert backend._available is True
    return TTLCache(ttl_seconds=60, backend=backend), fake


def _go_finalize_bump(fake: fakeredis.FakeValkey, org_id: str) -> int:
    """What internal/cacheinvalidation does after finalize commits: INCR then
    EXPIRE on the contract key. Issued as raw commands, not through the
    Python backend, so the Python read path is exercised against a value it
    did not write itself."""
    value = cast(int, fake.incr(org_cache_epoch_key(org_id)))
    fake.expire(org_cache_epoch_key(org_id), ORG_CACHE_EPOCH_TTL_SECONDS)
    return int(value)


def _stub_recompute(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def _raise(_db_url: str):
        raise RecomputeAttempted()
        yield  # pragma: no cover - unreachable, keeps the generator shape

    monkeypatch.setattr(home_service, "clickhouse_client", _raise)


@pytest.mark.asyncio
async def test_home_read_after_finalize_does_not_serve_pre_finalize_value(monkeypatch):
    """Red before CHAOS-4226: the seeded pre-finalize entry was served as-is."""
    cache, fake = _valkey_cache(monkeypatch)
    filters = _filters()
    _stub_recompute(monkeypatch)

    # A read BEFORE any finalize populates the cache under epoch 0.
    pre_key = epoch_cache_key(cache, "home", ORG, filters)
    assert pre_key is not None
    cache.set(pre_key, _response(50.0).model_dump(mode="json"))
    served = await home_service.build_home_response(
        db_url="clickhouse://unused", filters=filters, cache=cache, org_id=ORG
    )
    assert served.freshness.coverage.repos_covered_pct == 50.0

    # The Go finalize bumps the org epoch key (INCR semantics: absent -> 1).
    assert _go_finalize_bump(fake, ORG) == 1
    ttl = cast(int, fake.ttl(org_cache_epoch_key(ORG)))
    assert 0 < ttl <= ORG_CACHE_EPOCH_TTL_SECONDS

    # The very next read must NOT be served from the pre-finalize entry.
    with pytest.raises(RecomputeAttempted):
        await home_service.build_home_response(
            db_url="clickhouse://unused", filters=filters, cache=cache, org_id=ORG
        )


@pytest.mark.asyncio
async def test_home_read_after_finalize_serves_the_post_finalize_projection(
    monkeypatch,
):
    """Once recomputed under the new epoch, the fresh value is what reads see."""
    cache, fake = _valkey_cache(monkeypatch)
    filters = _filters()
    _stub_recompute(monkeypatch)

    stale_key = filter_cache_key("home", ORG, filters, extra={"_cache_epoch": 0})
    cache.set(stale_key, _response(50.0).model_dump(mode="json"))
    for _ in range(3):
        _go_finalize_bump(fake, ORG)
    fresh_key = filter_cache_key("home", ORG, filters, extra={"_cache_epoch": 3})
    cache.set(fresh_key, _response(100.0).model_dump(mode="json"))

    served = await home_service.build_home_response(
        db_url="clickhouse://unused", filters=filters, cache=cache, org_id=ORG
    )
    assert served.freshness.coverage.repos_covered_pct == 100.0


@pytest.mark.asyncio
async def test_home_read_without_any_finalize_still_hits_the_cache(monkeypatch):
    """No epoch key at all (fresh org, or Valkey flushed) reads as epoch 0."""
    cache, _fake = _valkey_cache(monkeypatch)
    filters = _filters()
    _stub_recompute(monkeypatch)
    cache.set(
        filter_cache_key("home", ORG, filters, extra={"_cache_epoch": 0}),
        _response(75.0).model_dump(mode="json"),
    )
    served = await home_service.build_home_response(
        db_url="clickhouse://unused", filters=filters, cache=cache, org_id=ORG
    )
    assert served.freshness.coverage.repos_covered_pct == 75.0


@pytest.mark.asyncio
async def test_memory_fallback_reads_the_same_key_as_epoch_zero(monkeypatch):
    """A Valkey outage at construction flips RedisBackend to its memory
    fallback; the epoch key is the same string there and an absent key is
    deterministically 0, so the two backends never split the key scheme.

    This pins the key contract only. A process that fell back to memory
    CANNOT observe a Go-side bump (nothing in-process reads shared Valkey
    again); its staleness is bounded by the entry TTL exactly as before
    CHAOS-4226 -- see the PR RISK-NOTES / follow-up on RedisBackend
    reconnect."""
    filters = _filters()
    memory_cache = TTLCache(ttl_seconds=60)
    valkey_cache, _fake = _valkey_cache(monkeypatch)
    assert memory_cache.org_epoch(ORG) == 0 == valkey_cache.org_epoch(ORG)
    assert epoch_cache_key(memory_cache, "home", ORG, filters) == epoch_cache_key(
        valkey_cache, "home", ORG, filters
    )


def test_epoch_scoped_cache_refuses_a_ttl_that_breaks_the_epoch_margin():
    """The ceiling fires once, at construction (api/main.py), not per read."""
    from dev_health_ops.core.cache import (
        EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS,
        epoch_scoped,
    )

    too_long = TTLCache(ttl_seconds=EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS + 1)
    with pytest.raises(ValueError):
        epoch_scoped(too_long)
    fits = TTLCache(ttl_seconds=EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS)
    assert epoch_scoped(fits) is fits


@pytest.mark.asyncio
async def test_unreadable_epoch_bypasses_the_cache_instead_of_guessing_zero(
    monkeypatch,
):
    """Codex R2 (CHAOS-4226): a transient failure of the epoch GET must not
    read as epoch 0 -- that would serve a still-live epoch-0 entry after the
    Go finalize bumped the epoch. Unreadable epoch => bypass the cache and
    recompute; absent epoch (no key) is still 0."""
    cache, fake = _valkey_cache(monkeypatch)
    filters = _filters()
    _stub_recompute(monkeypatch)

    stale_key = filter_cache_key("home", ORG, filters, extra={"_cache_epoch": 0})
    cache.set(stale_key, _response(50.0).model_dump(mode="json"))
    assert _go_finalize_bump(fake, ORG) == 1

    epoch_key = org_cache_epoch_key(ORG)
    real_get = fake.get

    def _flaky_get(key, *args, **kwargs):
        if key == epoch_key:
            raise ConnectionError("simulated one-command Valkey failure")
        return real_get(key, *args, **kwargs)

    monkeypatch.setattr(fake, "get", _flaky_get)
    assert cache.org_epoch(ORG) is None
    assert epoch_cache_key(cache, "home", ORG, filters) is None
    with pytest.raises(RecomputeAttempted):
        await home_service.build_home_response(
            db_url="clickhouse://unused", filters=filters, cache=cache, org_id=ORG
        )
    # The stale epoch-0 entry was neither served nor overwritten.
    assert cache.get(stale_key) is not None


class _RecordingCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, str], float]] = []

    def labels(self, **values: str) -> _RecordingCounter:
        self._pending = values
        return self

    def inc(self, amount: float = 1) -> None:
        self.calls.append((dict(self._pending), amount))


def test_epoch_bypass_is_counted_and_logged_with_org_and_error(monkeypatch, caplog):
    """Team-lead ruling on codex R2: a bypass is neither a hit nor a consumed
    invalidation, so it carries its own series
    (devhealth_cache_epoch_unreadable_total{prefix}) and a structured
    `cache.epoch_unreadable` line with prefix, org_id and the backend error."""
    import logging

    from dev_health_ops.api.services import filtering

    counter = _RecordingCounter()
    monkeypatch.setattr(filtering, "CACHE_EPOCH_UNREADABLE_TOTAL", counter)
    cache, fake = _valkey_cache(monkeypatch)
    filters = _filters()

    def _boom(*_a, **_k):
        raise TimeoutError("valkey read timed out")

    monkeypatch.setattr(fake, "get", _boom)
    with caplog.at_level(
        logging.WARNING, logger="dev_health_ops.api.services.filtering"
    ):
        assert filtering.epoch_cache_key(cache, "home", ORG, filters) is None
        assert filtering.epoch_cache_key(cache, "explain", ORG, filters) is None

    assert counter.calls == [({"prefix": "home"}, 1), ({"prefix": "explain"}, 1)]
    bypass = [r for r in caplog.records if r.getMessage() == "cache.epoch_unreadable"]
    assert len(bypass) == 2
    assert bypass[0].prefix == "home"
    assert bypass[0].org_id == ORG
    assert "TimeoutError: valkey read timed out" == bypass[0].error

    # A readable epoch neither counts nor logs.
    monkeypatch.setattr(fake, "get", type(fake).get.__get__(fake))
    caplog.clear()
    assert filtering.epoch_cache_key(cache, "home", ORG, filters) is not None
    assert counter.calls[2:] == []
    assert not [r for r in caplog.records if r.getMessage() == "cache.epoch_unreadable"]
