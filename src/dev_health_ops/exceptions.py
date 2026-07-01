from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dev_health_ops.sync.rate_limit_signal import RateLimitSignal


class ConnectorException(Exception):
    pass


class RateLimitException(ConnectorException):
    def __init__(
        self,
        message: str = "API rate limit exceeded",
        retry_after_seconds: float | None = None,
        *,
        signal: RateLimitSignal | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
        # Optional provider-neutral rate-limit signal (CHAOS-2753). Populated at
        # provider classification sites; enriched (integration_id/route_family)
        # at the worker boundary.
        self.signal = signal


class AuthenticationException(ConnectorException):
    pass


class NotFoundException(ConnectorException):
    pass


class PaginationException(ConnectorException):
    pass


class APIException(ConnectorException):
    pass
