"""Cross-language contract for the per-org cache epoch key (CHAOS-4226).

The Go native finalize (internal/cacheinvalidation) and the Python read path
(core.cache) must agree on ONE Valkey key per org. Neither side hand-copies
the other's string: this test REGENERATES the fixture from the live Python
producer and fails on any drift, and the Go side asserts its own function
against the same fixture file (internal/cacheinvalidation/contract_test.go).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import fakeredis

from dev_health_ops.core.cache import (
    EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS,
    EPOCH_SCOPED_CACHE_TTL_MARGIN,
    ORG_CACHE_EPOCH_TTL_SECONDS,
    MemoryBackend,
    RedisBackend,
    TTLCache,
    org_cache_epoch_key,
)
from dev_health_ops.core.cache_invalidation import (
    invalidate_on_metrics_update,
    invalidate_on_sync_complete,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "contracts" / "cache-invalidation" / "v1" / "org_cache_epoch_key.json"

# The org ids the contract pins. UUIDs (the real shape), a plain slug, and
# one with characters a naive key scheme might mangle.
CONTRACT_ORG_IDS = [
    "00000000-0000-4000-8000-0000000000f1",
    "9f3c2a44-1b7e-4c1d-9a0e-3d2b5c6e7f80",
    "org-a",
    "Acme Corp/EU:1",
]


def _expected_fixture() -> dict:
    return {
        "version": 1,
        "epoch_ttl_seconds": ORG_CACHE_EPOCH_TTL_SECONDS,
        "scoped_cache_max_ttl_seconds": EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS,
        "scoped_cache_ttl_margin": EPOCH_SCOPED_CACHE_TTL_MARGIN,
        "cases": [
            {"org_id": org_id, "key": org_cache_epoch_key(org_id)}
            for org_id in CONTRACT_ORG_IDS
        ],
    }


def test_fixture_is_producer_derived_from_the_live_python_key_function():
    assert FIXTURE.is_file(), f"missing contract fixture {FIXTURE}"
    on_disk = json.loads(FIXTURE.read_text(encoding="utf-8"))
    expected = _expected_fixture()
    assert on_disk == expected, (
        "contracts/cache-invalidation/v1/org_cache_epoch_key.json drifted from "
        "core.cache.org_cache_epoch_key; regenerate it from the Python producer "
        "and re-run the Go contract test"
    )


def test_epoch_key_expiry_dwarfs_every_epoch_scoped_entry_ttl():
    """Team-lead constraint 3: an epoch key that expired while entries stamped
    N were alive would let them serve again after the next bump re-created
    the key at 1. Pin the margin, and pin that the production caches fit."""
    assert ORG_CACHE_EPOCH_TTL_SECONDS >= (
        EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS * EPOCH_SCOPED_CACHE_TTL_MARGIN
    )
    from dev_health_ops.api import main as api_main

    for cache in (api_main.HOME_CACHE, api_main.EXPLAIN_CACHE):
        assert cache.ttl_seconds <= EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS
        assert (
            ORG_CACHE_EPOCH_TTL_SECONDS
            >= cache.ttl_seconds * EPOCH_SCOPED_CACHE_TTL_MARGIN
        )


def test_epoch_key_embeds_org_id_verbatim_under_a_stable_prefix():
    assert org_cache_epoch_key("org-a") == "cache_epoch:org:org-a"
    assert org_cache_epoch_key("a") != org_cache_epoch_key("b")


def test_memory_backend_incr_starts_at_one_and_counts():
    backend = MemoryBackend()
    key = org_cache_epoch_key("org-a")
    assert backend.incr(key, ttl_seconds=60) == 1
    assert backend.incr(key, ttl_seconds=60) == 2
    assert backend.get(key) == 2


def test_redis_backend_incr_matches_go_incr_expire_semantics(monkeypatch):
    """Valkey INCR on an absent key yields 1 and the key carries the epoch TTL."""
    fake = fakeredis.FakeValkey(decode_responses=True)
    monkeypatch.setattr(
        "valkey.from_url", lambda *_args, **_kwargs: fake, raising=False
    )
    backend = RedisBackend("redis://contract-test/0")
    assert backend._available is True
    key = org_cache_epoch_key("org-a")
    assert backend.incr(key, ttl_seconds=ORG_CACHE_EPOCH_TTL_SECONDS) == 1
    assert backend.incr(key, ttl_seconds=ORG_CACHE_EPOCH_TTL_SECONDS) == 2
    # The read path json.loads the raw value: a Go INCR writes "2".
    assert backend.get(key) == 2
    ttl = cast(int, fake.ttl(key))
    assert 0 < ttl <= ORG_CACHE_EPOCH_TTL_SECONDS


def test_ttlcache_epoch_reads_zero_when_absent_and_tracks_bumps():
    cache = TTLCache(ttl_seconds=60)
    assert cache.org_epoch("org-a") == 0
    assert cache.bump_org_epoch("org-a") == 1
    assert cache.org_epoch("org-a") == 1
    # A Go-side INCR shows up as a bare integer through the same read.
    cache.set(org_cache_epoch_key("org-b"), 7)
    assert cache.org_epoch("org-b") == 7


def test_sync_complete_event_bumps_the_org_epoch():
    """Sweep (CHAOS-4226): the webhook producer now clears the home cache too."""
    cache = TTLCache(ttl_seconds=60)
    invalidate_on_sync_complete(cache, "org-a", "github")
    assert cache.org_epoch("org-a") == 1
    invalidate_on_sync_complete(cache, "org-a", "jira")
    assert cache.org_epoch("org-a") == 2


def test_metrics_update_with_empty_org_does_not_write_a_global_epoch():
    """workers/metrics_daily.py passes org_id='' -- no org, no epoch key."""
    cache = TTLCache(ttl_seconds=60)
    invalidate_on_metrics_update(cache, "", "2026-08-22")
    assert cache.get(org_cache_epoch_key("")) is None


def test_redis_backend_epoch_read_is_tri_state(monkeypatch):
    """value / 0 when absent / None when unreadable -- never a guessed 0 on
    a backend error (codex R2, CHAOS-4226)."""
    fake = fakeredis.FakeValkey(decode_responses=True)
    monkeypatch.setattr("valkey.from_url", lambda *_a, **_k: fake, raising=False)
    backend = RedisBackend("redis://contract-test/1")
    key = org_cache_epoch_key("org-a")
    assert backend.get_epoch(key) == 0
    fake.incr(key)
    fake.incr(key)
    assert backend.get_epoch(key) == 2

    def _boom(*_a, **_k):
        raise TimeoutError("simulated")

    monkeypatch.setattr(fake, "get", _boom)
    assert backend.get_epoch(key) is None
    assert TTLCache(ttl_seconds=60, backend=backend).org_epoch("org-a") is None


def test_memory_backend_epoch_read_never_reports_unreadable():
    backend = MemoryBackend()
    assert backend.get_epoch(org_cache_epoch_key("org-a")) == 0
    backend.incr(org_cache_epoch_key("org-a"), ttl_seconds=60)
    assert backend.get_epoch(org_cache_epoch_key("org-a")) == 1
