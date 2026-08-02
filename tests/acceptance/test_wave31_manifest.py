from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance.wave31_manifest import (
    MANIFEST,
    MANIFEST_SCHEMA_VERSION,
    ManifestIntegrityError,
    ManifestItem,
    build_report,
    validate_manifest,
)

_ROOT = Path(__file__).resolve().parents[2]


def test_the_landed_manifest_has_no_integrity_errors() -> None:
    """Every evidence path the manifest cites must actually exist.

    This is the anti-fabrication check: a status of proven_unit/proven_e2e
    that names a test file which was renamed or deleted must fail here, not
    quietly keep reporting green.
    """

    assert validate_manifest(_ROOT) == []


def test_every_item_has_a_unique_id() -> None:
    ids = [item.id for item in MANIFEST]
    assert len(ids) == len(set(ids)), "duplicate manifest ids"


def test_every_blocking_matrix_category_is_represented() -> None:
    categories = {item.category for item in MANIFEST}
    assert categories == {
        "original_defect_reproduction",
        "real_project_positive_control",
        "attack",
        "blocking_matrix",
        "gate",
        "mutation_proof",
    }


def test_original_defect_reproductions_are_all_at_least_unit_proven() -> None:
    defects = [
        item for item in MANIFEST if item.category == "original_defect_reproduction"
    ]
    assert len(defects) == 3
    assert all(item.status in ("proven_unit", "proven_e2e") for item in defects)


def test_health_workload_deficiency_portfolio_items_are_honestly_blocked() -> None:
    """Locks in the CHAOS-3303/3304/3305 plan-wiring gap.

    These ids must stay ``blocked`` with the plan-wiring root cause until
    someone actually registers DevInvestigationPlan documents + StepRegistry
    steps for PROJECT_HEALTH/TEAM_HEALTH/PORTFOLIO_STATUS/
    TEAM_WORKLOAD_BALANCE/OPERATIONAL_DEFICIENCY_INVENTORY and threads the
    five services into production_runtime.py. A future change that flips one
    of these to proven_* without that wiring landing would be a false-green
    regression this test exists to catch.
    """

    blocked_ids = {
        "matrix.legitimate-org-wide-status",
        "matrix.organization-portfolio-status",
        "matrix.project-health-mixed-dimensions",
        "matrix.project-health-unknown-not-applicable",
        "matrix.team-health-complete-attribution",
        "matrix.team-health-unattributable-signals",
        "matrix.struggling-teams-positive",
        "matrix.struggling-teams-insufficient-sample",
        "matrix.overburdened-teams-with-denominators",
        "matrix.pressure-without-denominator",
        "matrix.light-on-feature-work",
        "matrix.light-on-feature-work-unclassified",
        "matrix.operational-deficiencies-mixed",
    }
    by_id = {item.id: item for item in MANIFEST}
    assert blocked_ids <= by_id.keys()
    for item_id in blocked_ids:
        item = by_id[item_id]
        assert item.status == "blocked", f"{item_id} is not blocked: {item.status}"
        assert item.blocked_reason is not None
        assert "CORE_PLANS_BY_INTENT" in item.blocked_reason
        assert "production_runtime.py" in item.blocked_reason


def test_migration_coexistence_gate_reflects_chaos_3306_decision() -> None:
    by_id = {item.id: item for item in MANIFEST}
    item = by_id["gate.migration-coexistence"]
    assert item.status == "blocked"
    assert item.blocked_reason is not None
    assert "CHAOS-3306" in item.blocked_reason
    assert "no implementation or decommission work is authorized" in (
        item.blocked_reason
    )


def test_repeated_provider_gate_is_deferred_not_silently_green() -> None:
    by_id = {item.id: item for item in MANIFEST}
    item = by_id["gate.repeated-certified-provider"]
    assert item.status == "deferred"
    assert item.blocked_reason is not None
    assert "credentials" in item.blocked_reason


# --- guard behavior: validate_manifest must actually catch every defect it
# claims to catch (RED-then-GREEN proof, not "no exception was raised") ---


def test_validate_manifest_catches_duplicate_ids() -> None:
    dup = ManifestItem(
        id="matrix.exact-project-complete",  # collides with a real MANIFEST id
        category="blocking_matrix",
        description="duplicate",
        status="proven_e2e",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
    )
    errors = validate_manifest(_ROOT, MANIFEST + (dup,))
    assert any("duplicate id" in error for error in errors)


def test_validate_manifest_catches_missing_evidence_file() -> None:
    fabricated = ManifestItem(
        id="test.fabricated-evidence",
        category="blocking_matrix",
        description="a claim resting on a file that does not exist",
        status="proven_unit",
        evidence=("tests/acceptance/this_file_does_not_exist.json",),
    )
    errors = validate_manifest(_ROOT, (fabricated,))
    assert any("evidence file does not exist" in error for error in errors)


def test_validate_manifest_catches_proven_item_with_no_evidence() -> None:
    unsupported = ManifestItem(
        id="test.unsupported-proven-claim",
        category="blocking_matrix",
        description="claims proven with nothing backing it",
        status="proven_unit",
        evidence=(),
    )
    errors = validate_manifest(_ROOT, (unsupported,))
    assert any("claims no evidence" in error for error in errors)


def test_validate_manifest_catches_blocked_without_reason() -> None:
    silent_skip = ManifestItem(
        id="test.silent-skip",
        category="blocking_matrix",
        description="blocked with no stated reason",
        status="blocked",
    )
    errors = validate_manifest(_ROOT, (silent_skip,))
    assert any("blocked without a blocked_reason" in error for error in errors)


def test_validate_manifest_catches_missing_content_marker() -> None:
    wrong_marker = ManifestItem(
        id="test.wrong-marker",
        category="blocking_matrix",
        description="evidence exists but does not say what is claimed",
        status="proven_unit",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
        content_markers=("this-string-is-not-in-the-oracle-file",),
    )
    errors = validate_manifest(_ROOT, (wrong_marker,))
    assert any("content marker" in error for error in errors)


def test_validate_manifest_catches_unknown_status() -> None:
    bad_status = ManifestItem(
        id="test.bad-status",
        category="blocking_matrix",
        description="an impossible status",
        status="totally_green",  # type: ignore[arg-type]
    )
    errors = validate_manifest(_ROOT, (bad_status,))
    assert any("unknown status" in error for error in errors)


def test_validate_manifest_is_clean_over_a_single_well_formed_item() -> None:
    clean = ManifestItem(
        id="test.well-formed",
        category="blocking_matrix",
        description="a correctly evidenced item",
        status="proven_e2e",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
        content_markers=("ask_dev_acceptance_oracle.v1",),
    )
    assert validate_manifest(_ROOT, (clean,)) == []


# --- report generation ---


def test_build_report_raises_on_a_broken_manifest_rather_than_reporting_green() -> None:
    fabricated = ManifestItem(
        id="test.fabricated-for-report",
        category="blocking_matrix",
        description="a claim resting on a file that does not exist",
        status="proven_unit",
        evidence=("tests/acceptance/this_file_does_not_exist.json",),
    )
    with pytest.raises(ManifestIntegrityError):
        build_report(_ROOT, MANIFEST + (fabricated,))


def test_build_report_shape_over_the_real_manifest() -> None:
    report = build_report(_ROOT)
    assert report["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert report["item_count"] == len(MANIFEST)
    status_counts = report["status_counts"]
    assert isinstance(status_counts, dict)
    assert sum(status_counts.values()) == len(MANIFEST)
    items = report["items"]
    assert isinstance(items, list)
    assert len(items) == len(MANIFEST)
    for entry in items:
        assert set(entry) == {
            "id",
            "category",
            "description",
            "status",
            "evidence",
            "blocked_reason",
        }
    # JSON-round-trippable -- a report a downstream tool cannot parse is not
    # a machine-readable deliverable.
    json.dumps(report)


def test_report_is_deterministically_ordered() -> None:
    report = build_report(_ROOT)
    items = report["items"]
    keys = [(entry["category"], entry["id"]) for entry in items]
    assert keys == sorted(keys)


def test_no_blocked_item_is_missing_its_reason_in_the_real_manifest() -> None:
    for item in MANIFEST:
        if item.status == "blocked":
            assert item.blocked_reason, f"{item.id} is blocked with no reason"


def test_no_proven_item_lacks_evidence_in_the_real_manifest() -> None:
    for item in MANIFEST:
        if item.status in ("proven_e2e", "proven_unit"):
            assert item.evidence, f"{item.id} claims {item.status} with no evidence"
