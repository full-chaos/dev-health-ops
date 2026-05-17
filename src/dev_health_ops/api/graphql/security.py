"""Security controls for the GraphQL API.

The repository does not have a single canonical application environment helper;
GraphQL hardening therefore follows the deployment variables already used by
observability integrations (environment names default to production) while also
accepting the common APP_ENV/ENV fallbacks for local tooling.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from typing import Any

from graphql import GraphQLError
from graphql.language.ast import FieldNode
from graphql.validation import ASTValidationRule, ValidationRule
from graphql.validation.rules.custom.no_schema_introspection import (
    NoSchemaIntrospectionCustomRule,
)
from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_GRAPHQL_DEPTH = 12
MAX_GRAPHQL_ALIASES = 15
DEFAULT_GRAPHQL_MAX_QUERY_BYTES = 16 * 1024

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


def _env_flag(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    return None


def environment_name() -> str:
    """Return the current application environment name.

    ENVIRONMENT is the primary knob documented for deployments. APP_ENV and ENV
    are accepted as compatibility fallbacks because no project-wide settings
    module currently defines a stronger convention.
    """

    return (
        (
            os.getenv("ENVIRONMENT")
            or os.getenv("APP_ENV")
            or os.getenv("ENV")
            or "production"
        )
        .strip()
        .lower()
    )


def is_development_environment() -> bool:
    """Return True only for explicitly local development environments."""

    return environment_name() in {"development", "dev", "local"}


def is_graphql_hardening_enabled() -> bool:
    """Return whether production GraphQL validation/size hardening is enabled."""

    override = _env_flag("GRAPHQL_SECURITY_ENABLED")
    if override is not None:
        return override
    return not is_development_environment()


def is_graphql_ide_enabled() -> bool:
    """Return whether the in-browser GraphiQL IDE should be exposed."""

    override = _env_flag("GRAPHQL_IDE_ENABLED")
    if override is not None:
        return override
    return is_development_environment()


def is_graphql_introspection_enabled() -> bool:
    """Return whether schema introspection is allowed."""

    override = _env_flag("GRAPHQL_INTROSPECTION_ENABLED")
    if override is not None:
        return override
    return not is_graphql_hardening_enabled()


class MaxDepthLimit(ValidationRule):
    """Reject operations deeper than the documented production depth limit.

    A small in-tree validator avoids adding another dependency solely for depth
    and alias limits. The default depth of 12 allows normal analytics query
    shapes while blocking pathological nested selection sets before execution.
    """

    max_depth = MAX_GRAPHQL_DEPTH

    def __init__(self, context: Any) -> None:
        super().__init__(context)
        self._depth = 0
        self._reported = False

    def enter_field(self, node: FieldNode, *_args: Any) -> None:
        self._depth += 1
        if not self._reported and self._depth > self.max_depth:
            self._reported = True
            self.report_error(
                GraphQLError(
                    f"GraphQL query depth exceeds limit of {self.max_depth}",
                    [node],
                )
            )

    def leave_field(self, _node: FieldNode, *_args: Any) -> None:
        self._depth -= 1


class MaxAliasLimit(ValidationRule):
    """Reject operations with too many aliases.

    The default alias cap of 15 blocks alias-amplification attacks while leaving
    enough headroom for dashboard clients that fan out several related fields.
    """

    max_aliases = MAX_GRAPHQL_ALIASES

    def __init__(self, context: Any) -> None:
        super().__init__(context)
        self._aliases = 0
        self._reported = False

    def enter_field(self, node: FieldNode, *_args: Any) -> None:
        if node.alias is None:
            return
        self._aliases += 1
        if not self._reported and self._aliases > self.max_aliases:
            self._reported = True
            self.report_error(
                GraphQLError(
                    f"GraphQL alias count exceeds limit of {self.max_aliases}",
                    [node],
                )
            )


def get_graphql_validation_rules() -> list[type[ASTValidationRule]]:
    """Return validation rules for the current environment."""

    rules: list[type[ASTValidationRule]] = []
    if is_graphql_hardening_enabled():
        rules.extend([MaxDepthLimit, MaxAliasLimit])
    if not is_graphql_introspection_enabled():
        rules.append(NoSchemaIntrospectionCustomRule)
    return rules


def get_graphql_max_query_bytes() -> int:
    """Return the configured HTTP request body limit for /graphql."""

    raw = os.getenv("GRAPHQL_MAX_QUERY_BYTES")
    if raw is None:
        return DEFAULT_GRAPHQL_MAX_QUERY_BYTES
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_GRAPHQL_MAX_QUERY_BYTES
    return max(1, value)


class GraphQLQuerySizeLimitMiddleware:
    """Reject oversized /graphql request bodies before GraphQL parsing."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and scope.get("method") == "GET"
            and str(scope.get("path", "")).startswith("/graphql")
            and not is_graphql_ide_enabled()
            and _accepts_html(scope)
        ):
            await self._send_not_found(send)
            return

        if (
            scope["type"] != "http"
            or scope.get("method") not in {"POST", "PUT", "PATCH"}
            or not str(scope.get("path", "")).startswith("/graphql")
            or not is_graphql_hardening_enabled()
        ):
            await self.app(scope, receive, send)
            return

        limit = get_graphql_max_query_bytes()
        body_messages: list[Message] = []
        body_size = 0
        more_body = True

        while more_body:
            message = await receive()
            body_messages.append(message)
            if message["type"] != "http.request":
                break
            body = message.get("body", b"")
            body_size += len(body)
            more_body = bool(message.get("more_body", False))
            if body_size > limit:
                await self._send_too_large(send, limit)
                return

        replay = _ReplayReceive(body_messages)
        await self.app(scope, replay, send)

    async def _send_not_found(self, send: Send) -> None:
        payload = json.dumps({"detail": "Not Found"}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})

    async def _send_too_large(self, send: Send, limit: int) -> None:
        payload = json.dumps(
            {
                "detail": {
                    "message": "GraphQL request body exceeds size limit",
                    "limit_bytes": limit,
                }
            }
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})


class _ReplayReceive:
    def __init__(self, messages: Iterable[Message]) -> None:
        self._messages = list(messages)

    async def __call__(self) -> Message:
        if self._messages:
            return self._messages.pop(0)
        return {"type": "http.request", "body": b"", "more_body": False}


def _accepts_html(scope: Scope) -> bool:
    headers = dict(scope.get("headers", []))
    accept = headers.get(b"accept", b"").decode("latin1")
    return "text/html" in accept
