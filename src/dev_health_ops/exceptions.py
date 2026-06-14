class ConnectorException(Exception):
    pass


class RateLimitException(ConnectorException):
    def __init__(
        self,
        message: str = "API rate limit exceeded",
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class AuthenticationException(ConnectorException):
    pass


class NotFoundException(ConnectorException):
    pass


class PaginationException(ConnectorException):
    pass


class APIException(ConnectorException):
    pass
