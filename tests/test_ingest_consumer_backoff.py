"""Assert consume_streams uses exponential backoff on repeated XREADGROUP failures."""

from __future__ import annotations


def test_exponential_backoff_on_repeated_failures(monkeypatch):
    from dev_health_ops.api.ingest import consumer as mod

    class BrokenRedis:
        def __init__(self):
            self.calls = 0

        def xreadgroup(self, *a, **kw):
            self.calls += 1
            raise RuntimeError("boom")

        def scan_iter(self, *a, **kw):
            return iter(["ingest:o:commits"])

        def xgroup_create(self, *a, **kw):
            pass

    broken = BrokenRedis()
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    # get_redis_client is imported lazily inside consume_streams
    from dev_health_ops.api.ingest import streams as streams_mod

    monkeypatch.setattr(streams_mod, "get_redis_client", lambda: broken)

    mod.consume_streams(stream_patterns=["ingest:*:commits"], max_iterations=5)

    # Five failed iterations -> backoff sequence starts at 1s and doubles
    # with a 30s cap. Assert strictly monotonic-non-decreasing and bounded.
    assert len(sleeps) == 5
    assert sleeps[0] == 1
    assert sleeps[-1] <= 30
    for a, b in zip(sleeps, sleeps[1:]):
        assert b >= a, f"backoff should not shrink: {sleeps}"
    assert any(b > a for a, b in zip(sleeps, sleeps[1:])), "expected growth"
