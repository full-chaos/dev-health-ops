"""Strict, validation-only loader for the sync-dispatch transport contract.

This module deliberately has no dependency on the reconciler or a queue client:
loading a route contract cannot claim or publish sync work.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

MAX_TRANSPORT_ROUTES_BYTES = 16 * 1024
_MAX_JSON_DEPTH = 8

_DOCUMENT_FIELDS = frozenset({"schema_version", "routes"})
_ROUTE_FIELDS = frozenset({"kind", "delivery", "route", "rollback_route"})
_EXPECTED_KINDS = (
    "dispatch_sync_run",
    "finalize_sync_run",
    "post_sync",
    "reference_discovery",
)
_EXPECTED_DELIVERY = MappingProxyType(
    {
        "dispatch_sync_run": "at_least_once",
        "finalize_sync_run": "at_least_once",
        "post_sync": "at_least_once",
        "reference_discovery": "at_least_once",
    }
)
_ALLOWED_TRANSPORT_PAIRS = frozenset(
    {
        ("celery", "celery"),
        ("river", "celery"),
    }
)


class DispatchRouteContractError(ValueError):
    """Raised when the sync-dispatch transport artifact is malformed or drifts."""


@dataclass(frozen=True, slots=True)
class TransportRoute:
    """The delivery semantics and current/fallback transport for one wakeup kind."""

    kind: str
    delivery: str
    route: str
    rollback_route: str


@dataclass(frozen=True, slots=True)
class TransportRoutes:
    """Immutable transport routes indexed by the canonical wakeup kind."""

    routes: Mapping[str, TransportRoute]

    def __post_init__(self) -> None:
        object.__setattr__(self, "routes", MappingProxyType(dict(self.routes)))

    def by_kind(self, kind: str) -> TransportRoute:
        try:
            return self.routes[kind]
        except KeyError as error:
            raise DispatchRouteContractError("unknown sync-dispatch kind") from error


def default_transport_routes_path() -> Path:
    """Return the checked-in v1 transport contract from a source checkout."""

    return (
        Path(__file__).resolve().parents[3]
        / "contracts"
        / "sync-dispatch"
        / "v1"
        / "transport-routes.json"
    )


def load_transport_routes(path: Path | None = None) -> TransportRoutes:
    """Load exactly the frozen v1 route set without activating any transport."""

    artifact_path = path or default_transport_routes_path()
    document = _load_document(artifact_path)
    if not isinstance(document, dict) or set(document) != _DOCUMENT_FIELDS:
        raise DispatchRouteContractError("transport route document shape is invalid")
    if (
        not isinstance(document["schema_version"], int)
        or isinstance(document["schema_version"], bool)
        or document["schema_version"] != 1
    ):
        raise DispatchRouteContractError(
            "transport route schema version is unsupported"
        )
    raw_routes = document["routes"]
    if not isinstance(raw_routes, list):
        raise DispatchRouteContractError("transport routes must be an array")

    parsed: list[TransportRoute] = []
    for raw in raw_routes:
        if not isinstance(raw, dict) or set(raw) != _ROUTE_FIELDS:
            raise DispatchRouteContractError("transport route entry shape is invalid")
        kind = _required_string(raw, "kind")
        delivery = _required_string(raw, "delivery")
        route = _required_string(raw, "route")
        rollback_route = _required_string(raw, "rollback_route")
        if (route, rollback_route) not in _ALLOWED_TRANSPORT_PAIRS:
            raise DispatchRouteContractError("transport route is unsupported")
        expected_delivery = _EXPECTED_DELIVERY.get(kind)
        if expected_delivery is None or delivery != expected_delivery:
            raise DispatchRouteContractError("transport delivery is inconsistent")
        parsed.append(
            TransportRoute(
                kind=kind,
                delivery=delivery,
                route=route,
                rollback_route=rollback_route,
            )
        )

    kinds = tuple(route.kind for route in parsed)
    if len(set(kinds)) != len(kinds):
        raise DispatchRouteContractError("transport routes contain duplicate kinds")
    if kinds != tuple(sorted(kinds)):
        raise DispatchRouteContractError("transport routes are not sorted")
    if kinds != _EXPECTED_KINDS:
        raise DispatchRouteContractError(
            "transport routes are missing or unknown kinds"
        )
    return TransportRoutes(
        routes=MappingProxyType({route.kind: route for route in parsed})
    )


def _load_document(path: Path) -> Any:
    if path.is_symlink() or not path.is_file():
        raise DispatchRouteContractError(
            "transport route artifact must be a regular file"
        )
    try:
        data = path.read_bytes()
    except OSError as error:
        raise DispatchRouteContractError(
            "transport route artifact cannot be read"
        ) from error
    if not data:
        raise DispatchRouteContractError("transport route artifact is empty")
    if len(data) > MAX_TRANSPORT_ROUTES_BYTES:
        raise DispatchRouteContractError("transport route artifact exceeds size limit")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DispatchRouteContractError(
            "transport route artifact must be UTF-8"
        ) from error

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise DispatchRouteContractError(
                    "transport route artifact has duplicate JSON keys"
                )
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise DispatchRouteContractError(
            "transport route artifact has non-finite number"
        )

    try:
        document = json.loads(
            text, object_pairs_hook=object_pairs, parse_constant=reject_constant
        )
    except DispatchRouteContractError:
        raise
    except (json.JSONDecodeError, RecursionError) as error:
        raise DispatchRouteContractError(
            "transport route artifact is invalid JSON"
        ) from error
    _validate_json_depth(document, 0)
    return document


def _required_string(document: dict[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise DispatchRouteContractError("transport route string field is invalid")
    return value


def _validate_json_depth(value: Any, depth: int) -> None:
    if depth > _MAX_JSON_DEPTH:
        raise DispatchRouteContractError(
            "transport route artifact is too deeply nested"
        )
    if isinstance(value, dict):
        for child in value.values():
            _validate_json_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _validate_json_depth(child, depth + 1)
