"""Cheap offline guard for the CHAOS-2702 external-ingest fixture set.

Unmarked (no ``clickhouse``/live-service dependency) -- runs in the full
unit suite and ``ci/local_validate.sh``'s offline gate. Proves:

- every one of the 9 v1 record-kind fixtures parses and has ``valid``/
  ``invalid`` keys;
- each fixture's ``valid.payload`` is byte/structurally identical to the
  canonical CHAOS-2692 package example (reconciliation #3 -- no fourth,
  hand-copied duplicate);
- each fixture's ``valid`` record actually validates against the real
  Pydantic per-kind model, and its ``invalid`` record fails validation with
  the fixture's own declared ``expectedError`` code/field.

This is pure/offline: it imports ``dev_health_ops.api.external_ingest.schemas``
(Pydantic models only, no DB/network), matching the CHAOS-2697 worker's own
validation path (``validate_records``) so the fixtures this test guards are
the same ones the live e2e module drives.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dev_health_ops.api.external_ingest.schemas import RECORD_KIND_MODELS
from dev_health_ops.external_ingest.validate import validate_records
from tests._helpers_external_ingest import (
    ALL_KINDS,
    load_fixture,
    load_package_example,
)


@pytest.mark.parametrize("kind", ALL_KINDS)
def test_fixture_parses_with_valid_and_invalid_keys(kind: str) -> None:
    fixture = load_fixture(kind)
    assert fixture["kind"] == f"{kind}.v1"
    assert "valid" in fixture
    assert "invalid" in fixture
    for case in ("valid", "invalid"):
        record = fixture[case]
        assert record["kind"] == f"{kind}.v1"
        assert isinstance(record.get("externalId"), str) and record["externalId"]
        assert isinstance(record.get("payload"), dict)


@pytest.mark.parametrize("kind", ALL_KINDS)
def test_valid_payload_matches_package_example_byte_for_byte(kind: str) -> None:
    fixture = load_fixture(kind)
    package_example = load_package_example(kind)
    assert fixture["valid"]["payload"] == package_example, (
        f"tests/fixtures/external_ingest/v1/{kind}.json's valid.payload has "
        f"drifted from the canonical src/dev_health_ops/api/external_ingest/"
        f"examples/{kind}.v1.json package example (single-source-of-truth "
        "rule, CHAOS-2690 reconciliation #3)"
    )


@pytest.mark.parametrize("kind", ALL_KINDS)
def test_valid_record_validates_against_real_pydantic_model(kind: str) -> None:
    fixture = load_fixture(kind)
    model = RECORD_KIND_MODELS[f"{kind}.v1"]
    model.model_validate(fixture["valid"]["payload"])  # must not raise


@pytest.mark.parametrize("kind", ALL_KINDS)
def test_invalid_record_fails_validation_with_declared_expected_error(
    kind: str,
) -> None:
    fixture = load_fixture(kind)
    invalid = fixture["invalid"]
    expected = invalid["expectedError"]
    model = RECORD_KIND_MODELS[f"{kind}.v1"]

    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(invalid["payload"])
    fields_in_error = {err["loc"][0] for err in exc_info.value.errors() if err["loc"]}
    assert expected["field"] in fields_in_error

    # Cross-check against the real POST /validate error-item shape
    # (validate_records) -- the same function the live e2e module exercises
    # over HTTP, so this fixture's declared code matches production reality.
    from dev_health_ops.api.external_ingest.schemas import RecordEnvelope

    record = RecordEnvelope.model_validate(
        {
            "kind": invalid["kind"],
            "externalId": invalid["externalId"],
            "payload": invalid["payload"],
        }
    )
    errors = validate_records([record])
    assert any(
        e.code == expected["code"]
        and e.path == f"records[0].payload.{expected['field']}"
        for e in errors
    ), [e.__dict__ for e in errors]
