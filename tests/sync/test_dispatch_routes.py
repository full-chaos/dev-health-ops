from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.sync import dispatch_outbox
from dev_health_ops.sync.dispatch_routes import (
    MAX_TRANSPORT_ROUTES_BYTES,
    DispatchRouteContractError,
    default_transport_routes_path,
    load_transport_routes,
)


def _write_document(path: Path, document: object) -> None:
    path.write_text(json.dumps(document), encoding="utf-8")


def _contract_document() -> dict[str, object]:
    return json.loads(default_transport_routes_path().read_text(encoding="utf-8"))


def test_v1_schema_describes_the_canonical_transport_artifact() -> None:
    artifact_path = default_transport_routes_path()
    schema_path = artifact_path.with_name("transport-routes.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    artifact = _contract_document()

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"] == (
        "https://contracts.fullchaos.dev/sync-dispatch/v1/transport-routes.schema.json"
    )
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["schema_version", "routes"]
    assert schema["properties"]["schema_version"] == {"type": "integer", "const": 1}

    routes_schema = schema["properties"]["routes"]
    assert routes_schema["type"] == "array"
    assert routes_schema["minItems"] == routes_schema["maxItems"] == 4
    assert routes_schema["items"] is False
    route_definitions = [
        route["$ref"].removeprefix("#/$defs/") for route in routes_schema["prefixItems"]
    ]
    assert route_definitions == [
        "dispatch_sync_run",
        "finalize_sync_run",
        "post_sync",
        "reference_discovery",
    ]

    definitions = schema["$defs"]
    assert definitions["transport"] == {
        "type": "string",
        "pattern": "^[a-z][a-z0-9_]*$",
        "enum": ["celery", "river"],
    }
    assert definitions["transport_pair"] == {
        "anyOf": [
            {
                "properties": {
                    "route": {"const": "celery"},
                    "rollback_route": {"const": "celery"},
                }
            },
            {
                "properties": {
                    "route": {"const": "river"},
                    "rollback_route": {"const": "celery"},
                }
            },
        ]
    }
    artifact_routes = artifact["routes"]
    assert isinstance(artifact_routes, list)
    for route, definition_name in zip(artifact_routes, route_definitions, strict=True):
        assert isinstance(route, dict)
        descriptor = definitions[definition_name]
        assert descriptor["type"] == "object"
        assert descriptor["additionalProperties"] is False
        assert descriptor["allOf"] == [{"$ref": "#/$defs/transport_pair"}]
        assert descriptor["required"] == [
            "kind",
            "delivery",
            "route",
            "rollback_route",
        ]
        properties = descriptor["properties"]
        assert route["kind"] == properties["kind"]["const"]
        assert route["delivery"] == properties["delivery"]["const"]
        assert route["route"] in definitions["transport"]["enum"]
        assert route["rollback_route"] in definitions["transport"]["enum"]

    assert load_transport_routes().by_kind("post_sync").delivery == "at_least_once"


def test_contract_covers_exactly_the_production_outbox_kinds() -> None:
    production_kinds = sorted(
        value
        for name, value in vars(dispatch_outbox).items()
        if name.startswith("OUTBOX_KIND_") and isinstance(value, str)
    )

    assert tuple(production_kinds) == tuple(sorted(load_transport_routes().routes))


def test_checked_in_transport_routes_are_celery_only_and_immutable() -> None:
    routes = load_transport_routes()

    assert tuple(routes.routes) == (
        "dispatch_sync_run",
        "finalize_sync_run",
        "post_sync",
        "reference_discovery",
    )
    assert routes.by_kind("post_sync").delivery == "at_least_once"
    assert all(
        route.route == route.rollback_route == "celery"
        for route in routes.routes.values()
    )
    with pytest.raises(TypeError):
        routes.routes["post_sync"] = routes.by_kind("post_sync")  # type: ignore[index]
    with pytest.raises(DispatchRouteContractError, match="unknown"):
        routes.by_kind("not_a_wakeup")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda document: document.update({"extra": True}),
            "shape",
        ),
        (
            lambda document: document.update({"schema_version": 2}),
            "version",
        ),
        (
            lambda document: document.update({"schema_version": True}),
            "version",
        ),
        (
            lambda document: document["routes"].append(document["routes"][0]),
            "duplicate",
        ),
        (
            lambda document: document.update(
                {"routes": list(reversed(document["routes"]))}
            ),
            "sorted",
        ),
        (
            lambda document: document.update({"routes": document["routes"][:-1]}),
            "missing",
        ),
        (
            lambda document: document["routes"][2].update(
                {"delivery": "at_most_once_mark_before"}
            ),
            "delivery",
        ),
        (
            lambda document: document["routes"][0].update({"route": "shadow"}),
            "unsupported",
        ),
        (
            lambda document: document["routes"][0].update({"extra": True}),
            "shape",
        ),
    ],
)
def test_loader_rejects_contract_drift(
    tmp_path: Path, mutate: object, message: str
) -> None:
    path = tmp_path / "transport-routes.json"
    document = _contract_document()
    mutate(document)  # type: ignore[operator]
    _write_document(path, document)

    with pytest.raises(DispatchRouteContractError, match=message):
        load_transport_routes(path)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\xff",
        b'{"schema_version": 1} {"routes": []}',
        b'{"schema_version": 1, "schema_version": 1, "routes": []}',
    ],
)
def test_loader_rejects_ambiguous_or_non_utf8_artifacts(
    tmp_path: Path, payload: bytes
) -> None:
    path = tmp_path / "transport-routes.json"
    path.write_bytes(payload)

    with pytest.raises(DispatchRouteContractError):
        load_transport_routes(path)


def test_loader_rejects_oversized_and_symlink_artifacts(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (MAX_TRANSPORT_ROUTES_BYTES + 1))
    with pytest.raises(DispatchRouteContractError, match="size"):
        load_transport_routes(oversized)

    target = tmp_path / "target.json"
    _write_document(target, _contract_document())
    symlink = tmp_path / "transport-routes.json"
    symlink.symlink_to(target)
    with pytest.raises(DispatchRouteContractError, match="regular"):
        load_transport_routes(symlink)


def test_loader_accepts_a_future_river_route_with_explicit_celery_rollback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "transport-routes.json"
    document = _contract_document()
    raw_routes = document["routes"]
    assert isinstance(raw_routes, list)
    first_route = raw_routes[0]
    assert isinstance(first_route, dict)
    first_route.update({"route": "river", "rollback_route": "celery"})
    _write_document(path, document)

    assert load_transport_routes(path).by_kind("dispatch_sync_run").route == "river"


@pytest.mark.parametrize(
    ("route", "rollback_route"),
    [
        ("celery", "river"),
        ("river", "river"),
    ],
)
def test_loader_rejects_unsafe_transport_pairs(
    tmp_path: Path, route: str, rollback_route: str
) -> None:
    path = tmp_path / "transport-routes.json"
    document = _contract_document()
    raw_routes = document["routes"]
    assert isinstance(raw_routes, list)
    first_route = raw_routes[0]
    assert isinstance(first_route, dict)
    first_route.update({"route": route, "rollback_route": rollback_route})
    _write_document(path, document)

    with pytest.raises(DispatchRouteContractError, match="unsupported"):
        load_transport_routes(path)


def test_loader_rejects_excessively_nested_json(tmp_path: Path) -> None:
    path = tmp_path / "transport-routes.json"
    nested: dict[str, object] = {}
    for _ in range(9):
        nested = {"nested": nested}
    _write_document(path, {"schema_version": 1, "routes": [], "nested": nested})

    with pytest.raises(DispatchRouteContractError, match="deeply"):
        load_transport_routes(path)
