"""Unit tests for the schema discovery registry (CHAOS-2692).

Pure-Python — the registry is computed from Pydantic classes plus packaged
JSON fixtures, no ClickHouse/Postgres dependency (docs/superpowers/plans/
2026-07-01-chaos-2690-implementation/briefs/brief-2692-schemas.md test
plan).
"""

from __future__ import annotations

import re

import pytest

from dev_health_ops.api.external_ingest import schema_registry as registry
from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION

_ETAG_RE = re.compile(r'^"[0-9a-f]{64}"$')


def test_get_bundle_returns_all_kinds_and_envelope_in_defs():
    bundle = registry.get_bundle(SCHEMA_VERSION)

    assert bundle is not None
    assert bundle.schema_version == SCHEMA_VERSION
    defs = bundle.document["$defs"]
    assert "BatchEnvelope" in defs
    for kind, model in registry.iter_record_kinds():
        assert model.__name__ in defs, f"{kind} model missing from $defs"


def test_get_bundle_unsupported_version_returns_none():
    assert registry.get_bundle("external-ingest.v99") is None


def test_list_versions_has_one_entry_with_all_nine_kinds_no_duplicates():
    versions = registry.list_versions()

    assert len(versions) == 1
    entry = versions[0]
    assert entry["schemaVersion"] == SCHEMA_VERSION
    kinds = entry["recordKinds"]
    assert len(kinds) == 9
    assert len(set(kinds)) == len(kinds)


def test_etag_is_stable_and_shaped_like_a_quoted_sha256():
    bundle_a = registry.get_bundle(SCHEMA_VERSION)
    bundle_b = registry.get_bundle(SCHEMA_VERSION)

    assert bundle_a is not None
    assert bundle_b is not None
    assert bundle_a.etag == bundle_b.etag
    assert _ETAG_RE.match(bundle_a.etag), bundle_a.etag


def test_load_example_returns_dict_for_known_kind():
    example = registry.load_example("commit.v1")

    assert isinstance(example, dict)
    assert example  # non-empty


def test_load_example_unknown_kind_raises_key_error():
    with pytest.raises(KeyError):
        registry.load_example("nonexistent.v1")


def test_envelope_level_payload_is_documented_as_kind_unconstrained():
    # Adversarial-review finding: $defs.RecordEnvelope.payload is a bare
    # object (no kind-conditional constraint is derivable from schemas.py's
    # RecordEnvelope model — payload/kind are only tied together in Python
    # validation code, not in the wire model). Validating a whole batch
    # against just "envelope" can therefore pass a payload the server's
    # POST /validate rejects; a customer must validate each record against
    # recordKinds[kind].$ref instead. Lock in that this gap is documented in
    # the bundle (not silently discoverable in prod), and that each
    # record-kind schema DOES carry real field constraints (proving the
    # per-record $ref workaround is actually meaningful).
    bundle = registry.get_bundle(SCHEMA_VERSION)
    assert bundle is not None

    payload_schema = bundle.document["$defs"]["RecordEnvelope"]["properties"]["payload"]
    assert "required" not in payload_schema  # unconstrained at envelope level
    assert "recordKinds[kind]" in bundle.document["description"]

    for _kind, model in registry.iter_record_kinds():
        kind_schema = bundle.document["$defs"][model.__name__]
        assert kind_schema.get("required"), (
            f"{model.__name__} has no required fields — the per-record $ref "
            "fidelity claim in the bundle description would be false"
        )


def test_schema_version_field_is_pinned_to_supported_versions():
    # Adversarial-review finding (round 2): BatchEnvelope.schema_version is a
    # bare `str` in the frozen schemas.py (the model can't be edited here to
    # add a Literal — CC17), so the naive generated schema was just
    # {"type": "string"} and a customer's offline validator would certify a
    # batch with schemaVersion="external-ingest.v99" that the live server
    # rejects with 400 unsupported_schema_version. The registry tightens
    # this post-generation using SUPPORTED_SCHEMA_VERSIONS (data it already
    # owns), not by hand-writing a schema.
    bundle = registry.get_bundle(SCHEMA_VERSION)
    assert bundle is not None

    field = bundle.document["$defs"]["BatchEnvelope"]["properties"]["schemaVersion"]
    assert field["const"] == SCHEMA_VERSION
    assert "external-ingest.v99" != field["const"]


def test_kind_field_is_pinned_to_known_record_kinds():
    # Same fidelity gap as schema_version, for RecordEnvelope.kind: a
    # customer validating a batch against just "envelope" should not be able
    # to certify an unknown kind the server's unknown_record_kind 400 would
    # reject.
    bundle = registry.get_bundle(SCHEMA_VERSION)
    assert bundle is not None

    field = bundle.document["$defs"]["RecordEnvelope"]["properties"]["kind"]
    expected = {kind for kind, _model in registry.iter_record_kinds()}
    assert set(field["enum"]) == expected
    assert "deployment.v1" not in field["enum"]


def test_record_kind_entries_ref_into_defs_and_carry_one_example():
    bundle = registry.get_bundle(SCHEMA_VERSION)

    assert bundle is not None
    for kind, model in registry.iter_record_kinds():
        entry = bundle.document["recordKinds"][kind]
        assert entry["$ref"] == f"#/$defs/{model.__name__}"
        assert len(entry["examples"]) == 1
