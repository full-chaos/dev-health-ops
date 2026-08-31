"""Unit tests for `_shape_check` in `test_go_api_livelocal.py` -- pure
logic, no ClickHouse/Postgres/Go binary required. These exist to discharge
round-5 codex finding P3 (2026-08-30) with tests rather than a 6th codex
review round (chris's ruling: "a test that cannot fail reads as
coverage").

Finding: `_shape_check`'s walker only appended a `notes` entry for a null
on a schema-nullable field when the document ALSO selected sub-fields
under it (i.e. only for a composite/object field). A nullable SCALAR
field returning null (no sub-selection to build the "sub-fields ..."
message from -- e.g. `WorkGraphFlow.degradedReason: String`) fell through
both the violation branch (field IS nullable, so no violation) and the
note branch (no `selection.selection_set`, so the note branch's own
`elif` guard never matched), producing NO signal at all. That silently
contradicted this file's own documented ruling ("a null on a field the
schema declares NULLABLE is a spec-valid response -- reported as a note,
not a violation") for exactly the field shape (bare nullable scalar) most
common in a GraphQL schema.

`_shape_check` always builds its schema via the module-level
`_load_schema()` (real `contracts/graphql/v1/schema.graphql`, `lru_cache`d
once). These tests monkeypatch that one function to a small synthetic
schema built in-process, so the walker is exercised against controlled
nullability combinations without depending on the real schema's current
shape or reading it from disk repeatedly.
"""

from __future__ import annotations

from graphql import GraphQLSchema, build_schema

from tests.api.graphql import test_go_api_livelocal as livelocal

_TEST_SCHEMA_SDL = """
type Query {
  scalarField: String
  nonNullScalarField: String!
  compositeField: Composite
  nonNullCompositeField: Composite!
}

type Composite {
  inner: String
  nonNullInner: String!
}
"""


def _build_test_schema() -> GraphQLSchema:
    return build_schema(_TEST_SCHEMA_SDL)


def _patched_shape_check(monkeypatch, document: str, data) -> livelocal._ShapeCheck:
    monkeypatch.setattr(livelocal, "_load_schema", _build_test_schema)
    return livelocal._shape_check(document, data)


# --------------------------------------------------------------------------
# Nullable SCALAR field -- the round-5 P3 gap this PR fixes.
# --------------------------------------------------------------------------


def test_nullable_scalar_null_is_a_note_not_silently_dropped(monkeypatch):
    result = _patched_shape_check(
        monkeypatch, "query { scalarField }", {"scalarField": None}
    )
    assert result.violations == []
    assert len(result.notes) == 1
    assert "scalarField" in result.notes[0]
    assert "null" in result.notes[0]


def test_nullable_scalar_present_no_note_no_violation(monkeypatch):
    result = _patched_shape_check(
        monkeypatch, "query { scalarField }", {"scalarField": "hello"}
    )
    assert result.violations == []
    assert result.notes == []


# --------------------------------------------------------------------------
# Non-nullable SCALAR field -- unaffected by this fix, pinned as a
# regression guard so the new `else` branch cannot accidentally swallow a
# real contract violation into a note.
# --------------------------------------------------------------------------


def test_nonnullable_scalar_null_is_still_a_violation(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { nonNullScalarField }",
        {"nonNullScalarField": None},
    )
    assert result.notes == []
    assert len(result.violations) == 1
    assert "NON-nullable" in result.violations[0]


def test_nonnullable_scalar_present_no_note_no_violation(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { nonNullScalarField }",
        {"nonNullScalarField": "hello"},
    )
    assert result.violations == []
    assert result.notes == []


# --------------------------------------------------------------------------
# Nullable COMPOSITE (object) field -- the pre-existing branch, pinned
# unchanged so this fix's new `else` cannot regress the composite case it
# sits beside.
# --------------------------------------------------------------------------


def test_nullable_composite_null_is_a_note_with_subfields_listed(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { compositeField { inner } }",
        {"compositeField": None},
    )
    assert result.violations == []
    assert len(result.notes) == 1
    assert "sub-fields" in result.notes[0]
    assert "inner" in result.notes[0]


def test_nonnullable_composite_null_is_a_violation(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { nonNullCompositeField { inner } }",
        {"nonNullCompositeField": None},
    )
    assert result.notes == []
    assert len(result.violations) == 1
    assert "NON-nullable" in result.violations[0]


# --------------------------------------------------------------------------
# Nullable scalar NESTED inside a present composite -- confirms the fixed
# branch fires uniformly at every depth, not only at the top level.
# --------------------------------------------------------------------------


def test_nullable_scalar_null_inside_composite_is_a_note(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { compositeField { inner } }",
        {"compositeField": {"inner": None}},
    )
    assert result.violations == []
    assert len(result.notes) == 1
    assert "inner" in result.notes[0]


def test_nonnullable_scalar_null_inside_composite_is_a_violation(monkeypatch):
    result = _patched_shape_check(
        monkeypatch,
        "query { compositeField { nonNullInner } }",
        {"compositeField": {"nonNullInner": None}},
    )
    assert result.notes == []
    assert len(result.violations) == 1
    assert (
        "nonNullInner" in result.violations[0] or "NON-nullable" in result.violations[0]
    )


# --------------------------------------------------------------------------
# A field missing from the response entirely is still a violation,
# independent of nullability -- unaffected by this fix, pinned as a
# regression guard.
# --------------------------------------------------------------------------


def test_missing_field_is_a_violation(monkeypatch):
    result = _patched_shape_check(monkeypatch, "query { scalarField }", {})
    assert result.notes == []
    assert len(result.violations) == 1
    assert "MISSING" in result.violations[0]
