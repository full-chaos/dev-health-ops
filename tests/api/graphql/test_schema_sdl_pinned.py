"""Pin the Python GraphQL schema against the canonical SDL: Python must be a SUBSET.

``contracts/graphql/v1/schema.graphql`` is owned by the Go plane. query-api
generates its executable schema from that file (gqlgen, schema-first), so
the file grows when Go ports a field or type that Python never had. It is
therefore NOT a Strawberry export and must never be regenerated from one:
regenerating from Python would erase the Go-only growth.

What this test gates instead: every schema member the Python (Strawberry)
schema exposes must exist, printed identically, in the pin, so a Python
change the pin (and therefore Go, web codegen and the routing digest) does
not know about fails here. Extra types/members in the pin are allowed.

Exact coverage (by construction, not by a list of checks): both SDLs are
parsed to a GraphQL AST; every definition is decomposed into members --
the definition header (kind, name, implemented interfaces, applied
directives), each field, each input field, each enum value, each union
member, each directive definition -- and each member is compared as its
printed SDL text (so types, nullability, list wrapping, argument names,
types and default values, applied directives and locations are all part of
the text), plus the query/mutation/subscription root bindings. The ONE
exclusion: descriptions (docstrings) on any node. The subset relation ends
when CHAOS-6264 deletes the Python schema.

To change the schema: edit ``contracts/graphql/v1/schema.graphql``, run
``go run ./cmd/gqlgen-guard generate`` (gqlgen), update
``contracts/graphql/v1/schema-digest.json`` and the schema-digest history
table, and bump the checked-in ``web`` copy in a paired PR. A Python-side
addition must be mirrored into the pin in the same PR.
"""

from __future__ import annotations

import copy
from pathlib import Path

from graphql import build_schema, parse, print_ast
from graphql.language import ast as gql_ast

from dev_health_ops.api.graphql.schema import schema

_PINNED_SDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "graphql"
    / "v1"
    / "schema.graphql"
)


def _strip_descriptions(node: gql_ast.Node) -> None:
    """Clear ``description`` on ``node`` and every descendant, in place."""
    for key in node.keys:
        value = getattr(node, key, None)
        if key == "description":
            setattr(node, key, None)
        elif isinstance(value, gql_ast.Node):
            _strip_descriptions(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, gql_ast.Node):
                    _strip_descriptions(item)


def _printed(node: gql_ast.Node) -> str:
    return print_ast(node).strip()


def _header(node: gql_ast.Node, *children: str) -> gql_ast.Node:
    """A shallow copy of ``node`` with the named child lists emptied."""
    header = copy.copy(node)
    for attr in children:
        if hasattr(header, attr):
            setattr(header, attr, ())
    return header


def schema_members(sdl: str) -> set[str]:
    """Every member of ``sdl`` as ``<owner>: <printed member>`` text."""
    document = copy.deepcopy(parse(sdl))
    _strip_descriptions(document)
    members: set[str] = set()
    child_attrs = ("fields", "values", "types")
    for definition in document.definitions:
        if isinstance(definition, gql_ast.DirectiveDefinitionNode):
            members.add(f"directive: {_printed(definition)}")
            continue
        if isinstance(definition, gql_ast.SchemaDefinitionNode):
            # Root bindings are taken from the built schema below; what is
            # left to compare here is the directives applied to the schema.
            if definition.directives:
                header = _header(definition, "operation_types")
                members.add(f"schema: {_printed(header)}")
            continue
        name = getattr(getattr(definition, "name", None), "value", "")
        members.add(f"{name}: {_printed(_header(definition, *child_attrs))}")
        for attr in child_attrs:
            for child in getattr(definition, attr, ()) or ():
                members.add(f"{name}: {attr} {_printed(child)}")
    built = build_schema(sdl)
    for root in ("query", "mutation", "subscription"):
        root_type = getattr(built, f"{root}_type")
        if root_type is not None:
            members.add(f"schema: {root} {root_type.name}")
    return members


def python_members_missing_from_pin(python_sdl: str, pinned_sdl: str) -> list[str]:
    """Printed members of ``python_sdl`` that ``pinned_sdl`` lacks (empty =
    subset). See the module docstring for the exact coverage."""
    return sorted(schema_members(python_sdl) - schema_members(pinned_sdl))


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
        "contracts/graphql/v1/schema.graphql does not carry identically: "
        + "; ".join(missing)
        + ". The pin is the Go plane's schema and is never regenerated from "
        "Python; add or align the member in the pin (then `go run "
        "./cmd/gqlgen-guard generate`, update schema-digest.json and its "
        "history row) in the same PR."
    )


def test_a_pin_that_only_grows_still_passes() -> None:
    """Go-owned growth (an extra type and field) must not trip the gate."""
    grown = _PINNED_SDL_PATH.read_text() + "\ntype GoOnlyProbe {\n  id: String!\n}\n"
    assert python_members_missing_from_pin(schema.as_str(), grown) == []


def test_descriptions_are_the_only_exclusion() -> None:
    """A changed description passes; nothing else about the member may change."""
    base = 'type Query { "old" value(a: Int = 1): Int }'
    assert python_members_missing_from_pin(base, base.replace('"old"', '"new"')) == []


_BASE = """
directive @d(a: Int = 1) on FIELD | QUERY
directive @tag on OBJECT | SCHEMA
scalar Sc
interface Node { id: ID! }
enum Color { RED GREEN }
union U = A | B
input In { n: Int = 1 }
type A implements Node @tag { id: ID! }
type B { x: Int }
type Query { value(limit: Int = 1): Int }
type Mutation { write: Int }
type Mutation2 { write: Int }
"""

# One case per member class: (what it exercises, Python SDL, pin SDL).
_CLASSES = {
    "field (name)": (_BASE, _BASE.replace("x: Int", "y: Int")),
    "field (type/nullability)": (_BASE, _BASE.replace("x: Int", "x: Int!")),
    "argument (default)": (
        _BASE,
        _BASE.replace("limit: Int = 1", "limit: Int = 2"),
    ),
    "input field": (_BASE, _BASE.replace("n: Int = 1", "n: Int = 2")),
    "enum value": (_BASE, _BASE.replace("RED GREEN", "RED")),
    "union member": (_BASE, _BASE.replace("U = A | B", "U = A")),
    "type header (implements/directive)": (
        _BASE,
        _BASE.replace("type A implements Node @tag", "type A"),
    ),
    "scalar": (_BASE, _BASE.replace("scalar Sc\n", "")),
    "directive definition": (
        _BASE,
        _BASE.replace("FIELD | QUERY", "FIELD"),
    ),
    "schema directive": (
        _BASE + "schema @tag { query: Query }",
        _BASE + "schema { query: Query }",
    ),
    "root binding": (
        _BASE + "schema { query: Query mutation: Mutation }",
        _BASE + "schema { query: Query mutation: Mutation2 }",
    ),
}


def test_every_member_class_is_compared() -> None:
    """Each member class, changed in the pin alone, is reported."""
    assert python_members_missing_from_pin(_BASE, _BASE) == []
    for label, (python_sdl, pin_sdl) in _CLASSES.items():
        missing = python_members_missing_from_pin(python_sdl, pin_sdl)
        assert len(missing) == 1, (label, missing)


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
