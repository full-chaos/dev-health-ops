"""
Exception types for connector operations.
"""


class ConnectorException(Exception):
    """Base exception for all connector errors."""


class RateLimitException(ConnectorException):
    """Raised when API rate limit is exceeded."""

    def __init__(
        self,
        message: str = "API rate limit exceeded",
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class AuthenticationException(ConnectorException):
    """Raised when authentication fails."""


class NotFoundException(ConnectorException):
    """Raised when a resource is not found."""


class PaginationException(ConnectorException):
    """Raised when pagination fails."""


class APIException(ConnectorException):
    """Raised when API returns an error."""
