from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import datetime, timezone

import pytest

from dev_health_ops.processors.fetch_utils import (
    AsyncBatchCollector,
    SyncBatchCollector,
    coerce_datetime,
    extract_retry_after,
    safe_parse_datetime,
)


class _CompletedFuture:
    def result(self):
        return None


class _ExcWithHeaders(Exception):
    def __init__(self, headers):
        super().__init__("rate limited")
        self.headers = headers


def _flush_threadsafe(coro: Coroutine[object, object, object], _loop: object):
    asyncio.run(coro)
    return _CompletedFuture()


class TestSafeParseDateTime:
    def test_datetime_passthrough(self):
        dt = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        assert safe_parse_datetime(dt) is dt

    def test_string_iso(self):
        parsed = safe_parse_datetime("2025-01-02T03:04:05+00:00")
        assert parsed == datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_string_z_suffix(self):
        parsed = safe_parse_datetime("2025-01-02T03:04:05Z")
        assert parsed == datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_invalid_string(self):
        assert safe_parse_datetime("not-a-date") is None

    def test_none_input(self):
        assert safe_parse_datetime(None) is None

    def test_int_input(self):
        assert safe_parse_datetime(123) is None


class TestCoerceDatetime:
    def test_alias(self):
        assert coerce_datetime is safe_parse_datetime


class TestExtractRetryAfter:
    def test_from_connector_method(self):
        class _Connector:
            def _rate_limit_reset_delay_seconds(self):
                return 4.5

        exc = _ExcWithHeaders({"Retry-After": "2"})
        assert extract_retry_after(exc, connector=_Connector()) == pytest.approx(4.5)

    def test_from_retry_after_header(self):
        exc = _ExcWithHeaders({"Retry-After": "3"})
        assert extract_retry_after(exc) == pytest.approx(3.0)

    def test_from_retry_after_header_case_insensitive(self):
        exc = _ExcWithHeaders({"retry-after": "7"})
        assert extract_retry_after(exc) == pytest.approx(7.0)

    def test_from_ratelimit_reset_header(self, monkeypatch):
        monkeypatch.setattr(
            "dev_health_ops.processors.fetch_utils.time.time", lambda: 100.0
        )
        exc = _ExcWithHeaders({"x-ratelimit-reset": "110"})
        assert extract_retry_after(exc) == pytest.approx(10.0)

    def test_no_headers(self):
        exc = Exception("no headers")
        assert extract_retry_after(exc) is None

    def test_connector_method_fails(self):
        class _Connector:
            def _rate_limit_reset_delay_seconds(self):
                raise RuntimeError("broken")

        exc = _ExcWithHeaders({"Retry-After": "9"})
        assert extract_retry_after(exc, connector=_Connector()) == pytest.approx(9.0)

    def test_no_connector(self):
        exc = _ExcWithHeaders({"Retry-After": "8"})
        assert extract_retry_after(exc, connector=None) == pytest.approx(8.0)


class TestSyncBatchCollector:
    def test_basic_collection(self, monkeypatch):
        calls = []

        async def flush_fn(items):
            calls.append(items)

        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _flush_threadsafe)

        loop = asyncio.new_event_loop()
        try:
            collector = SyncBatchCollector(flush_fn, loop=loop, batch_size=2)
            collector.add(1)
            assert calls == []
            collector.add(2)
        finally:
            loop.close()

        assert calls == [[1, 2]]

    def test_context_manager_flushes_remainder(self, monkeypatch):
        calls = []

        async def flush_fn(items):
            calls.append(items)

        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _flush_threadsafe)

        loop = asyncio.new_event_loop()
        try:
            with SyncBatchCollector(flush_fn, loop=loop, batch_size=3) as collector:
                collector.add("a")
                collector.add("b")
        finally:
            loop.close()

        assert calls == [["a", "b"]]

    def test_total_count(self, monkeypatch):
        async def flush_fn(_items):
            return None

        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _flush_threadsafe)

        loop = asyncio.new_event_loop()
        try:
            collector = SyncBatchCollector(flush_fn, loop=loop, batch_size=2)
            collector.add(1)
            collector.add(2)
            collector.add(3)
            collector.flush()
            assert collector.total == 3
        finally:
            loop.close()

    def test_empty_flush_is_noop(self, monkeypatch):
        called = False

        async def flush_fn(_items):
            nonlocal called
            called = True

        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _flush_threadsafe)

        loop = asyncio.new_event_loop()
        try:
            collector = SyncBatchCollector(flush_fn, loop=loop, batch_size=2)
            collector.flush()
        finally:
            loop.close()
        assert called is False


class TestAsyncBatchCollector:
    @pytest.mark.asyncio
    async def test_basic_collection(self):
        calls = []

        async def flush_fn(items):
            calls.append(items)

        collector = AsyncBatchCollector(flush_fn, batch_size=2)
        collector.add(1)
        await collector.maybe_flush()
        assert calls == []

        collector.add(2)
        await collector.maybe_flush()
        assert calls == [[1, 2]]

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        calls = []

        async def flush_fn(items):
            calls.append(items)

        async with AsyncBatchCollector(flush_fn, batch_size=10) as collector:
            collector.add("x")
            collector.add("y")

        assert calls == [["x", "y"]]

    @pytest.mark.asyncio
    async def test_total_count(self):
        async def flush_fn(_items):
            return None

        collector = AsyncBatchCollector(flush_fn, batch_size=2)
        collector.add(1)
        collector.add(2)
        await collector.maybe_flush()
        collector.add(3)
        await collector.flush()
        assert collector.total == 3

    @pytest.mark.asyncio
    async def test_empty_flush_is_noop(self):
        called = False

        async def flush_fn(_items):
            nonlocal called
            called = True

        collector = AsyncBatchCollector(flush_fn, batch_size=2)
        await collector.flush()
        assert called is False
