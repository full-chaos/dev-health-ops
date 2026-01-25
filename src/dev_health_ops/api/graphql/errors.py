"""Typed GraphQL error classes for analytics API."""

from __future__ import annotations

from typing import Any, Dict

import strawberry


@strawberry.type
class GraphQLError:
    """Base GraphQL error type."""

    message: str
    code: str
    extensions: strawberry.Private[Dict[str, Any]] = strawberry.field(
        default_factory=dict
    )


class CostLimitExceededError(Exception):
    """Raised when a request exceeds cost limits."""

    def __init__(
        self,
        message: str,
        limit_name: str,
        limit_value: int,
        requested_value: int,
    ):
        super().__init__(message)
        self.limit_name = limit_name
        self.limit_value = limit_value
        self.requested_value = requested_value
        self.code = "COST_LIMIT_EXCEEDED"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "code": self.code,
            "extensions": {
                "limit_name": self.limit_name,
                "limit_value": self.limit_value,
                "requested_value": self.requested_value,
            },
        }


class ValidationError(Exception):
    """Raised when input validation fails."""

    def __init__(self, message: str, field: str, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value
        self.code = "VALIDATION_ERROR"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "code": self.code,
            "extensions": {
                "field": self.field,
                "value": str(self.value) if self.value is not None else None,
            },
        }


class AuthorizationError(Exception):
    """Raised when authorization fails (e.g., missing org_id)."""

    def __init__(self, message: str = "Authorization required"):
        super().__init__(message)
        self.code = "AUTHORIZATION_ERROR"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "code": self.code,
        }


class PersistedQueryError(Exception):
    """Raised for persisted query issues."""

    def __init__(
        self,
        message: str,
        query_id: str | None = None,
        schema_version: str | None = None,
    ):
        super().__init__(message)
        self.query_id = query_id
        self.schema_version = schema_version
        self.code = "PERSISTED_QUERY_ERROR"

    def to_dict(self) -> Dict[str, Any]:
        extensions: Dict[str, Any] = {}
        if self.query_id:
            extensions["query_id"] = self.query_id
        if self.schema_version:
            extensions["schema_version"] = self.schema_version
        return {
            "message": str(self),
            "code": self.code,
            "extensions": extensions if extensions else None,
        }


class QueryTimeoutError(Exception):
    """Raised when a query exceeds the timeout limit."""

    def __init__(
        self, message: str = "Query timeout exceeded", timeout_seconds: int = 0
    ):
        super().__init__(message)
        self.timeout_seconds = timeout_seconds
        self.code = "QUERY_TIMEOUT"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "code": self.code,
            "extensions": {"timeout_seconds": self.timeout_seconds},
        }
