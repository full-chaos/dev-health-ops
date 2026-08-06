"""Typed ClickHouse connection URI parsing for clickhouse-connect."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit


class ClickHouseClientKwargs(TypedDict):
    """Explicit clickhouse-connect client construction options."""

    host: str
    port: int
    username: str
    password: str
    database: str
    interface: Literal["http", "https"]
    secure: bool
    settings: dict[str, int]


@dataclass(frozen=True, slots=True)
class ClickHouseConnectionUriError(ValueError):
    """A ClickHouse URI that cannot safely create a driver connection."""

    reason: str

    def __str__(self) -> str:
        return f"Invalid ClickHouse connection URI: {self.reason}"


_SUPPORTED_SCHEMES = frozenset(
    {"clickhouse", "clickhouse+http", "clickhouse+https", "clickhouse+native"}
)


def clickhouse_client_kwargs(
    dsn: str, *, settings: dict[str, int]
) -> ClickHouseClientKwargs:
    """Build explicit clickhouse-connect options from a supported URI.

    URI credentials are decoded here and never retained in error messages or logs.
    """
    parsed = _parse_clickhouse_uri(dsn)
    host = parsed.hostname
    if host is None:
        raise ClickHouseConnectionUriError(reason="host is required")

    try:
        port = parsed.port
    except ValueError as exc:
        raise ClickHouseConnectionUriError(
            reason="port must be a valid integer"
        ) from exc

    interface, secure = _interface_and_security(parsed, port)

    return {
        "host": host,
        "port": port if port is not None else 8443 if secure else 8123,
        "username": unquote(parsed.username)
        if parsed.username is not None
        else "default",
        "password": unquote(parsed.password) if parsed.password is not None else "",
        "database": _database_name(parsed),
        "interface": interface,
        "secure": secure,
        "settings": settings,
    }


def redact_clickhouse_uri(dsn: str) -> str:
    """Return a log-safe URI display that never includes userinfo or query data."""
    try:
        parsed = urlsplit(dsn)
        host = parsed.hostname
    except ValueError:
        return "clickhouse://<invalid>"

    scheme = parsed.scheme.lower()
    if scheme not in _SUPPORTED_SCHEMES or host is None:
        return "clickhouse://<invalid>"
    try:
        port = f":{parsed.port}" if parsed.port is not None else ""
    except ValueError:
        return "clickhouse://<invalid>"
    database = parsed.path.lstrip("/")
    return (
        f"{scheme}://{host}{port}/{database}"
        if database
        else f"{scheme}://{host}{port}"
    )


def _parse_clickhouse_uri(dsn: str) -> SplitResult:
    try:
        parsed = urlsplit(dsn)
    except ValueError as exc:
        raise ClickHouseConnectionUriError(reason="URI is malformed") from exc

    if parsed.scheme.lower() not in _SUPPORTED_SCHEMES:
        raise ClickHouseConnectionUriError(reason="scheme is unsupported")
    if not parsed.netloc:
        raise ClickHouseConnectionUriError(reason="host is required")
    if parsed.fragment:
        raise ClickHouseConnectionUriError(reason="fragments are unsupported")
    return parsed


def _database_name(parsed: SplitResult) -> str:
    database = unquote(parsed.path.lstrip("/"))
    if "/" in database:
        raise ClickHouseConnectionUriError(
            reason="database path must contain one segment"
        )
    return database or "default"


def _interface_and_security(
    parsed: SplitResult, port: int | None
) -> tuple[Literal["http", "https"], bool]:
    scheme = parsed.scheme.lower()
    requested_secure = _query_secure(parsed)
    if scheme == "clickhouse+https":
        if requested_secure is False:
            raise ClickHouseConnectionUriError(
                reason="secure=false conflicts with clickhouse+https"
            )
        return "https", True
    if scheme == "clickhouse+http":
        if requested_secure is True:
            raise ClickHouseConnectionUriError(
                reason="secure=true conflicts with clickhouse+http"
            )
        return "http", False
    secure = requested_secure if requested_secure is not None else port in {443, 8443}
    return ("https", True) if secure else ("http", False)


def _query_secure(parsed: SplitResult) -> bool | None:
    try:
        options = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise ClickHouseConnectionUriError(reason="query string is malformed") from exc
    if not options:
        return None
    if len(options) != 1 or options[0][0] != "secure":
        raise ClickHouseConnectionUriError(
            reason="only secure is supported in the query"
        )
    match options[0][1].lower():
        case "true":
            return True
        case "false":
            return False
        case _:
            raise ClickHouseConnectionUriError(
                reason="secure query parameter must be true or false"
            )
