"""
Retry and rate limiting utilities with exponential backoff.
"""

import asyncio
import inspect
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _get_retry_after_seconds(exc: Exception) -> Optional[float]:
    retry_after = getattr(exc, "retry_after_seconds", None)
    if retry_after is None:
        return None
    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        return None


class RateLimiter:
    """
    Rate limiter with exponential backoff support.

    This class tracks API calls and implements exponential backoff
    when rate limits are encountered.
    """

    def __init__(
        self,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        max_retries: int = 5,
    ):
        """
        Initialize the rate limiter.

        :param initial_delay: Initial delay in seconds before first retry.
        :param max_delay: Maximum delay in seconds between retries.
        :param backoff_factor: Factor to multiply delay by after each retry.
        :param max_retries: Maximum number of retry attempts.
        """
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries
        self.current_delay = initial_delay

    def reset(self) -> None:
        """Reset the delay to initial value."""
        self.current_delay = self.initial_delay

    def get_next_delay(self) -> float:
        """Get the next delay value and update state."""
        delay = min(self.current_delay, self.max_delay)
        self.current_delay *= self.backoff_factor
        return delay

    async def wait(self) -> None:
        """Wait for the current delay period."""
        delay = self.get_next_delay()
        logger.info(
            "Waiting %.2f seconds before retry...",
            delay,
        )
        await asyncio.sleep(delay)

    def wait_sync(self) -> None:
        """Synchronous wait for the current delay period."""
        delay = self.get_next_delay()
        logger.info(
            "Waiting %.2f seconds before retry...",
            delay,
        )
        time.sleep(delay)


def retry_with_backoff(
    max_retries: int = 5,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """
    Decorator that implements retry logic with exponential backoff.

    :param max_retries: Maximum number of retry attempts.
    :param initial_delay: Initial delay in seconds before first retry.
    :param max_delay: Maximum delay in seconds between retries.
    :param backoff_factor: Factor to multiply delay by after each retry.
    :param exceptions: Tuple of exception types to catch and retry.
    :return: Decorated function.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            rate_limiter = RateLimiter(
                initial_delay=initial_delay,
                max_delay=max_delay,
                backoff_factor=backoff_factor,
                max_retries=max_retries,
            )

            last_exception: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    result = await func(*args, **kwargs)
                    rate_limiter.reset()  # Reset on success
                    return result
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        retry_after = _get_retry_after_seconds(e)
                        logger.warning(
                            "Attempt %s/%s failed: %s. Retrying...",
                            attempt + 1,
                            max_retries,
                            e,
                        )
                        if retry_after is not None:
                            logger.info(
                                "Rate limited. Waiting %.2fs before retry...",
                                retry_after,
                            )
                            await asyncio.sleep(retry_after)
                        else:
                            await rate_limiter.wait()
                    else:
                        logger.error(
                            "All %s attempts failed. Giving up.",
                            max_retries,
                        )

            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError("Max retries exceeded with no exception captured")

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            rate_limiter = RateLimiter(
                initial_delay=initial_delay,
                max_delay=max_delay,
                backoff_factor=backoff_factor,
                max_retries=max_retries,
            )

            last_exception: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    rate_limiter.reset()  # Reset on success
                    return result
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        retry_after = _get_retry_after_seconds(e)
                        logger.warning(
                            "Attempt %s/%s failed: %s. Retrying...",
                            attempt + 1,
                            max_retries,
                            e,
                        )
                        if retry_after is not None:
                            logger.info(
                                "Rate limited. Waiting %.2fs before retry...",
                                retry_after,
                            )
                            time.sleep(retry_after)
                        else:
                            rate_limiter.wait_sync()
                    else:
                        logger.error(
                            "All %s attempts failed. Giving up.",
                            max_retries,
                        )

            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError("Max retries exceeded with no exception captured")

        # Return appropriate wrapper based on function type
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
