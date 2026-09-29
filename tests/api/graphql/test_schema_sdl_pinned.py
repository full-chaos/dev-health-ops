"""Pin the Python GraphQL schema against the canonical SDL: Python must be a SUBSET.

``contracts/graphql/v1/schema.graphql`` is owned by the Go plane. query-api
generates its executable schema from that file (gqlgen, schema-first), so
the file grows when Go ports a field or type that Python never had. It is
therefore NOT a Strawberry export and must never be regenerated from one:
regenerating from Python would erase the Go-only growth.

What this test gates instead: every type, field, argument and enum value
the Python (Strawberry) schema exposes must exist in the pin, so a Python
change that the pin (and therefore Go, web codegen and the routing digest)
does not know about fails here. Extra types/fields in the pin are allowed.
The subset relation ends when CHAOS-6264 deletes the Python schema.

To change the schema: edit ``contracts/graphql/v1/schema.graphql``, run
``go run ./cmd/gqlgen-guard generate`` (gqlgen), update
``contracts/graphql/v1/schema-digest.json`` and the schema-digest history
table, and bump the checked-in ``web`` copy in a paired PR. A Python-side
addition must be mirrored into the pin in the same PR.
"""

from __future__ import annotations

from pathlib import Path

from graphql import (
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLInterfaceType,
    GraphQLObjectType,
    GraphQLUnionType,
    build_schema,
)

from dev_health_ops.api.graphql.schema import schema

_PINNED_SDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "graphql"
    / "v1"
    / "schema.graphql"
)


def python_members_missing_from_pin(python_sdl: str, pinned_sdl: str) -> list[str]:
    """Every type / field / argument / enum value / union member / implemented
    interface in ``python_sdl`` that ``pinned_sdl`` lacks, and every shared
    field, argument or input field whose declared type (nullability and list
    wrapping included) differs (empty = subset with identical signatures).

    Signatures must match exactly while Python still serves the field: a
    changed return or argument type makes the two planes accept different
    operations. A Go-side signature change to a shared member must be mirrored
    in Python, or wait for Python's deletion."""
    py = build_schema(python_sdl)
    pin = build_schema(pinned_sdl)
    missing: list[str] = []
    for name, py_type in sorted(py.type_map.items()):
        if name.startswith("__"):
            continue
        pin_type = pin.type_map.get(name)
        if pin_type is None:
            missing.append(f"type {name}")
            continue
        if type(pin_type) is not type(py_type):
            missing.append(
                f"type {name}: kind {type(py_type).__name__} in Python, "
                f"{type(pin_type).__name__} in the pin"
            )
            continue
        if isinstance(py_type, (GraphQLObjectType, GraphQLInterfaceType)):
            pin_fields = pin_type.fields  # type: ignore[union-attr]
            for fname, field in sorted(py_type.fields.items()):
                pin_field = pin_fields.get(fname)
                if pin_field is None:
                    missing.append(f"field {name}.{fname}")
                    continue
                if str(field.type) != str(pin_field.type):
                    missing.append(
                        f"field {name}.{fname}: type {field.type} in Python, "
                        f"{pin_field.type} in the pin"
                    )
                for aname, arg in sorted(field.args.items()):
                    pin_arg = pin_field.args.get(aname)
                    if pin_arg is None:
                        missing.append(f"argument {name}.{fname}({aname})")
                    elif str(arg.type) != str(pin_arg.type):
                        missing.append(
                            f"argument {name}.{fname}({aname}): type {arg.type} "
                            f"in Python, {pin_arg.type} in the pin"
                        )
            for iface in py_type.interfaces:
                if iface.name not in {
                    i.name
                    for i in pin_type.interfaces  # type: ignore[union-attr]
                }:
                    missing.append(f"type {name} implements {iface.name}")
        elif isinstance(py_type, GraphQLInputObjectType):
            for fname, in_field in sorted(py_type.fields.items()):
                pin_in_field = pin_type.fields.get(fname)  # type: ignore[union-attr]
                if pin_in_field is None:
                    missing.append(f"input field {name}.{fname}")
                elif str(in_field.type) != str(pin_in_field.type):
                    missing.append(
                        f"input field {name}.{fname}: type {in_field.type} in "
                        f"Python, {pin_in_field.type} in the pin"
                    )
        elif isinstance(py_type, GraphQLEnumType):
            for vname in sorted(py_type.values):
                if vname not in pin_type.values:  # type: ignore[union-attr]
                    missing.append(f"enum value {name}.{vname}")
        elif isinstance(py_type, GraphQLUnionType):
            pin_members = {t.name for t in pin_type.types}  # type: ignore[union-attr]
            for member in py_type.types:
                if member.name not in pin_members:
                    missing.append(f"union {name} member {member.name}")
    return missing


def test_python_schema_is_a_subset_of_the_checked_in_pin() -> None:
    """The pin is Go-owned; the Python schema may not expose anything it lacks."""
    assert _PINNED_SDL_PATH.exists(), (
        f"Canonical SDL pin missing at {_PINNED_SDL_PATH}."
    )
    missing = python_members_missing_from_pin(
        schema.as_str(), _PINNED_SDL_PATH.read_text()
    )
    assert not missing, (
        "The Python (Strawberry) schema exposes members that "
        "contracts/graphql/v1/schema.graphql does not: "
        + ", ".join(missing)
        + ". The pin is the Go plane's schema and is never regenerated from "
        "Python; add the member to the pin (then `go run ./cmd/gqlgen-guard generate`, update "
        "schema-digest.json and its history row) in the same PR."
    )


def test_subset_check_reports_a_member_the_pin_lacks() -> None:
    """Observe the guard failing: a pin missing one field is reported by name."""
    pinned = _PINNED_SDL_PATH.read_text()
    python_sdl = schema.as_str()
    mutated = pinned.replace("  home(orgId", "  homeRenamedProbe(orgId", 1)
    assert mutated != pinned
    assert python_members_missing_from_pin(python_sdl, mutated) == ["field Query.home"]


def test_a_pin_that_only_grows_still_passes() -> None:
    """Go-owned growth (an extra type and field) must not trip the gate."""
    grown = _PINNED_SDL_PATH.read_text() + "\ntype GoOnlyProbe {\n  id: String!\n}\n"
    assert python_members_missing_from_pin(schema.as_str(), grown) == []


def test_pinned_sdl_is_nonempty_and_well_formed() -> None:
    """Guard against a pin that was truncated or corrupted rather than regenerated.

    A byte-for-byte compare above would still "pass" if both the live
    export and the pin were empty/garbage for the same reason (e.g. schema
    import raised and export_schema silently produced an empty file some
    other way). Assert independently on shape.
    """
    pinned_sdl = _PINNED_SDL_PATH.read_text()
    assert len(pinned_sdl) > 1000, "Pinned SDL is suspiciously small"
    assert "type Query" in pinned_sdl
    assert "type Mutation" in pinned_sdl


def test_subset_check_reports_a_changed_field_or_argument_type() -> None:
    """Observe the guard failing on a same-name member with a different type."""
    pinned = _PINNED_SDL_PATH.read_text()
    python_sdl = schema.as_str()
    changed_return = pinned.replace(
        "  home(orgId: String!, filters: FilterInput = null): HomeResult!",
        "  home(orgId: String!, filters: FilterInput = null): String!",
        1,
    )
    assert changed_return != pinned
    assert python_members_missing_from_pin(python_sdl, changed_return) == [
        "field Query.home: type HomeResult! in Python, String! in the pin"
    ]
    changed_arg = pinned.replace(
        "  home(orgId: String!, filters: FilterInput = null): HomeResult!",
        "  home(orgId: Int!, filters: FilterInput = null): HomeResult!",
        1,
    )
    assert changed_arg != pinned
    assert python_members_missing_from_pin(python_sdl, changed_arg) == [
        "argument Query.home(orgId): type String! in Python, Int! in the pin"
    ]
