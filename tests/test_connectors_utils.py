"""
Tests for connector utilities.
"""

import asyncio
import time

import pytest

from dev_health_ops.connectors.utils.pagination import (
    AsyncPaginationHandler,
    PaginationHandler,
)
from dev_health_ops.connectors.utils.retry import RateLimiter, retry_with_backoff


class TestRateLimiter:
    """Test RateLimiter class."""

    def test_initial_delay(self):
        """Test initial delay value."""
        limiter = RateLimiter(initial_delay=2.0)
        assert limiter.initial_delay == 2.0
        assert limiter.current_delay == 2.0

    def test_get_next_delay(self):
        """Test getting next delay with backoff."""
        limiter = RateLimiter(initial_delay=1.0, backoff_factor=2.0, max_delay=10.0)
        delay1 = limiter.get_next_delay()
        assert delay1 == 1.0
        delay2 = limiter.get_next_delay()
        assert delay2 == 2.0
        delay3 = limiter.get_next_delay()
        assert delay3 == 4.0

    def test_max_delay(self):
        """Test max delay limit."""
        limiter = RateLimiter(initial_delay=5.0, backoff_factor=3.0, max_delay=10.0)
        limiter.get_next_delay()  # 5.0
        limiter.get_next_delay()  # 15.0 -> limited to 10.0
        delay = limiter.get_next_delay()  # Should be max_delay
        assert delay == 10.0

    def test_reset(self):
        """Test resetting the rate limiter."""
        limiter = RateLimiter(initial_delay=1.0)
        limiter.get_next_delay()
        limiter.get_next_delay()
        limiter.reset()
        assert limiter.current_delay == limiter.initial_delay

    @pytest.mark.asyncio
    async def test_async_wait(self):
        """Test async wait."""
        limiter = RateLimiter(initial_delay=0.01, max_delay=0.1)
        start = time.time()
        await limiter.wait()
        duration = time.time() - start
        assert duration >= 0.01

    def test_sync_wait(self):
        """Test sync wait."""
        limiter = RateLimiter(initial_delay=0.01, max_delay=0.1)
        start = time.time()
        limiter.wait_sync()
        duration = time.time() - start
        assert duration >= 0.01


class TestRetryDecorator:
    """Test retry_with_backoff decorator."""

    def test_successful_function(self):
        """Test decorator with successful function."""
        call_count = [0]

        @retry_with_backoff(max_retries=3)
        def success_func():
            call_count[0] += 1
            return "success"

        result = success_func()
        assert result == "success"
        assert call_count[0] == 1

    def test_retry_on_exception(self):
        """Test retry on exception."""
        call_count = [0]

        @retry_with_backoff(max_retries=3, initial_delay=0.01, exceptions=(ValueError,))
        def failing_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("Temporary error")
            return "success"

        result = failing_func()
        assert result == "success"
        assert call_count[0] == 3

    def test_max_retries_exceeded(self):
        """Test exceeding max retries."""

        @retry_with_backoff(max_retries=2, initial_delay=0.01, exceptions=(ValueError,))
        def always_fail():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fail()

    @pytest.mark.asyncio
    async def test_async_function(self):
        """Test decorator with async function."""
        call_count = [0]

        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        async def async_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("Temporary error")
            return "success"

        result = await async_func()
        assert result == "success"
        assert call_count[0] == 2


class TestPaginationHandler:
    """Test PaginationHandler class."""

    def test_initialization(self):
        """Test pagination handler initialization."""
        handler = PaginationHandler(per_page=50, max_pages=10, max_items=100)
        assert handler.per_page == 50
        assert handler.max_pages == 10
        assert handler.max_items == 100

    def test_should_continue(self):
        """Test should_continue logic."""
        handler = PaginationHandler(per_page=10, max_pages=5, max_items=30)
        assert handler.should_continue() is True

        handler.current_page = 5
        assert handler.should_continue() is False

        handler.current_page = 0
        handler.total_items = 30
        assert handler.should_continue() is False

    def test_paginate(self):
        """Test pagination through results."""

        def fetch_func(page, per_page):
            if page > 3:
                return []
            return list(range((page - 1) * per_page, page * per_page))

        handler = PaginationHandler(per_page=10)
        results = list(handler.paginate(fetch_func))
        assert len(results) == 30
        assert results[0] == 0
        assert results[-1] == 29

    def test_paginate_with_max_items(self):
        """Test pagination with max items limit."""

        def fetch_func(page, per_page):
            return list(range((page - 1) * per_page, page * per_page))

        handler = PaginationHandler(per_page=10, max_items=25)
        results = list(handler.paginate(fetch_func))
        assert len(results) == 25

    def test_paginate_all(self):
        """Test paginate_all method."""

        def fetch_func(page, per_page):
            if page > 2:
                return []
            return list(range((page - 1) * per_page, page * per_page))

        handler = PaginationHandler(per_page=10)
        results = handler.paginate_all(fetch_func)
        assert len(results) == 20


class TestAsyncPaginationHandler:
    """Test AsyncPaginationHandler class."""

    @pytest.mark.asyncio
    async def test_async_paginate(self):
        """Test async pagination."""

        async def fetch_func(page, per_page):
            if page > 3:
                return []
            await asyncio.sleep(0.01)
            return list(range((page - 1) * per_page, page * per_page))

        handler = AsyncPaginationHandler(per_page=10)
        results = []
        async for item in handler.paginate(fetch_func):
            results.append(item)

        assert len(results) == 30

    @pytest.mark.asyncio
    async def test_async_paginate_all(self):
        """Test async paginate_all method."""

        async def fetch_func(page, per_page):
            if page > 2:
                return []
            await asyncio.sleep(0.01)
            return list(range((page - 1) * per_page, page * per_page))

        handler = AsyncPaginationHandler(per_page=10)
        results = await handler.paginate_all(fetch_func)
        assert len(results) == 20

    @pytest.mark.asyncio
    async def test_async_with_max_items(self):
        """Test async pagination with max items."""

        async def fetch_func(page, per_page):
            await asyncio.sleep(0.01)
            return list(range((page - 1) * per_page, page * per_page))

        handler = AsyncPaginationHandler(per_page=10, max_items=15)
        results = await handler.paginate_all(fetch_func)
        assert len(results) == 15
