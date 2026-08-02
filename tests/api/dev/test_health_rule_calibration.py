"""Calibration inventory completeness (CHAOS-3302 deliverable):

"calibration record and owner decision for each launch rule". Every rule
in the registry must have a calibration record, every record must point
at a real registered rule, and no provisional record may carry an
evidence_ref it hasn't earned.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationRecord,
    CalibrationState,
)
from dev_health_ops.api.dev.health_rule_calibration_inventory import CALIBRATION_RECORDS
from dev_health_ops.api.dev.health_rule_registry import HEALTH_RULE_REGISTRY


def test_every_registered_rule_has_a_calibration_record() -> None:
    recorded_rule_ids = {record.rule_id for record in CALIBRATION_RECORDS}
    assert recorded_rule_ids == set(HEALTH_RULE_REGISTRY)


def test_every_calibration_record_points_at_a_registered_rule() -> None:
    for record in CALIBRATION_RECORDS:
        assert record.rule_id in HEALTH_RULE_REGISTRY


def test_calibration_record_state_matches_the_rule_definition() -> None:
    for record in CALIBRATION_RECORDS:
        rule = HEALTH_RULE_REGISTRY.rule(record.rule_id)
        assert record.calibration_state == rule.calibration_state


def test_provisional_records_carry_no_evidence_ref() -> None:
    for record in CALIBRATION_RECORDS:
        if record.calibration_state == CalibrationState.PROVISIONAL:
            assert record.evidence_ref is None


def test_reviewed_records_require_evidence_ref() -> None:
    """Kill site: a reviewed calibration_state without evidence_ref must reject."""

    with pytest.raises(ValidationError):
        CalibrationRecord(
            schema_version="health_rule_calibration.v1",
            calibration_id="test.missing-evidence",
            rule_id="health_rule.completion_stalled.v1",
            rule_version="health_rule.completion_stalled.v1",
            calibration_state=CalibrationState.PRODUCT_APPROVED,
            sample_size=10,
            distribution_summary="n/a",
            false_positive_review="n/a",
            false_negative_review="n/a",
            small_cohort_behavior="n/a",
            owner="test",
            decided_at="2026-08-01",
            evidence_ref=None,
        )
