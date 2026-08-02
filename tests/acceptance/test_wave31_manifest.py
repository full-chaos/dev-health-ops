from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from scripts.acceptance.wave31_manifest import (
    MANIFEST,
    MANIFEST_SCHEMA_VERSION,
    ManifestIntegrityError,
    ManifestItem,
    build_report,
    execute_manifest,
    run_evidence_tests,
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
    assert len(defects) == 5
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
        # Must read as sequenced-to-stack-3, not silently dropped: pointer to
        # the ratified CHAOS-3303 deferral comment.
        assert "SEQUENCED" in item.blocked_reason
        assert "d0985e79-051d-4b6f-8833-6137e8511aec" in item.blocked_reason
        assert "stack-3" in item.blocked_reason


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


def test_team_status_live_defect_is_pinned_not_silently_folded_into_the_wiring_gap() -> (
    None
):
    """Locks in the 2026-08-02 live-discovered team-attribution blocker.

    This is a DIFFERENT root cause from the 13 stack-3-sequenced items:
    TEAM is a supported_subject_kind on status.entity.v2, a WIRED plan, so
    this defect must never get silently merged into or explained away by
    the CORE_PLANS_BY_INTENT wiring-gap reason -- doing so would hide a
    live, 100%-reproducible bug behind an unrelated, already-tracked one.
    """

    by_id = {item.id: item for item in MANIFEST}
    item = by_id["attack.team-attribution.e2e-blocked-by-live-defect"]
    assert item.status == "blocked"
    assert item.blocked_reason is not None
    assert "internal_error" in item.blocked_reason
    assert "proceeded_committed_subject" in item.blocked_reason
    assert "tool_call_count=0" in item.blocked_reason
    # Routed by team-lead 2026-08-02: filed as CHAOS-3332. The manifest must
    # point at the tracker, not just the raw repro.
    assert "CHAOS-3332" in item.blocked_reason
    # Explicitly distinguishes itself from the stack-3 wiring gap rather
    # than silently reusing that shared reason string.
    assert "NOT the CORE_PLANS_BY_INTENT gap" in item.blocked_reason
    stack3_blocked_ids = {
        "matrix.legitimate-org-wide-status",
        "matrix.organization-portfolio-status",
    }
    stack3_reasons = {
        by_id[stack3_id].blocked_reason for stack3_id in stack3_blocked_ids
    }
    assert item.blocked_reason not in stack3_reasons


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
        requires_live_infra=True,
    )
    assert validate_manifest(_ROOT, (clean,)) == []


def test_validate_manifest_catches_proven_unit_with_no_test_nodeids() -> None:
    file_only_claim = ManifestItem(
        id="test.file-only-claim",
        category="blocking_matrix",
        description="a file exists but no specific test is cited",
        status="proven_unit",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
    )
    errors = validate_manifest(_ROOT, (file_only_claim,))
    assert any("no test_nodeids" in error for error in errors)


def test_validate_manifest_catches_proven_e2e_without_requires_live_infra() -> None:
    unmarked_e2e = ManifestItem(
        id="test.unmarked-e2e",
        category="blocking_matrix",
        description="claims e2e proof without flagging it needs live infra",
        status="proven_e2e",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
    )
    errors = validate_manifest(_ROOT, (unmarked_e2e,))
    assert any("requires_live_infra" in error for error in errors)


def test_validate_manifest_catches_a_test_nodeid_outside_its_own_evidence() -> None:
    mismatched = ManifestItem(
        id="test.mismatched-nodeid",
        category="blocking_matrix",
        description="cites a test in a file that isn't listed as evidence",
        status="proven_unit",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
        test_nodeids=(
            "tests/api/dev/test_tool_registry.py::"
            "test_manifest_is_the_exact_nine_tool_server_allowlist",
        ),
    )
    errors = validate_manifest(_ROOT, (mismatched,))
    assert any(
        "is not under any of this item's own evidence" in error for error in errors
    )


# --- execution proof: a cited test must actually run and pass, not merely
# exist under a plausible name (team-lead emphasis 2026-08-02: "a scenario
# that doesn't run must FAIL the manifest, never read as pass") ---


def test_run_evidence_tests_marks_a_passing_test_as_passed(tmp_path: Path) -> None:
    (tmp_path / "test_throwaway_pass.py").write_text(
        "def test_it_passes() -> None:\n    assert True\n"
    )
    outcomes = run_evidence_tests(tmp_path, ("test_throwaway_pass.py::test_it_passes",))
    assert outcomes == {"test_throwaway_pass.py::test_it_passes": "passed"}


def test_run_evidence_tests_marks_a_failing_test_as_failed(tmp_path: Path) -> None:
    (tmp_path / "test_throwaway_fail.py").write_text(
        "def test_it_fails() -> None:\n    assert False, 'deliberate RED'\n"
    )
    outcomes = run_evidence_tests(tmp_path, ("test_throwaway_fail.py::test_it_fails",))
    assert outcomes == {"test_throwaway_fail.py::test_it_fails": "failed"}


def test_run_evidence_tests_marks_a_nonexistent_test_as_not_collected(
    tmp_path: Path,
) -> None:
    (tmp_path / "test_throwaway_real.py").write_text(
        "def test_real() -> None:\n    assert True\n"
    )
    outcomes = run_evidence_tests(
        tmp_path, ("test_throwaway_real.py::test_this_function_does_not_exist",)
    )
    assert outcomes == {
        "test_throwaway_real.py::test_this_function_does_not_exist": "not_collected"
    }


def test_run_evidence_tests_over_a_mixed_pass_fail_error_file(tmp_path: Path) -> None:
    (tmp_path / "test_throwaway_mixed.py").write_text(
        textwrap.dedent(
            """\
            import pytest

            def test_passes() -> None:
                assert True

            def test_fails() -> None:
                assert False

            def test_errors() -> None:
                raise RuntimeError("setup blew up")
            """
        )
    )
    outcomes = run_evidence_tests(
        tmp_path,
        (
            "test_throwaway_mixed.py::test_passes",
            "test_throwaway_mixed.py::test_fails",
            "test_throwaway_mixed.py::test_errors",
        ),
    )
    assert outcomes == {
        "test_throwaway_mixed.py::test_passes": "passed",
        "test_throwaway_mixed.py::test_fails": "failed",
        "test_throwaway_mixed.py::test_errors": "failed",
    }


def test_execute_manifest_reports_a_failing_cited_test(tmp_path: Path) -> None:
    (tmp_path / "test_throwaway_broken_claim.py").write_text(
        "def test_the_claim() -> None:\n    assert False\n"
    )
    broken_claim = ManifestItem(
        id="test.broken-claim",
        category="blocking_matrix",
        description="cites a test that does not actually pass",
        status="proven_unit",
        evidence=("test_throwaway_broken_claim.py",),
        test_nodeids=("test_throwaway_broken_claim.py::test_the_claim",),
    )
    errors = execute_manifest(tmp_path, (broken_claim,))
    assert any(
        "test_throwaway_broken_claim.py::test_the_claim -> failed" in error
        for error in errors
    )


def test_execute_manifest_reports_a_test_that_never_ran() -> None:
    """A node id that fails to collect must fail loudly, not be skipped."""

    vanished = ManifestItem(
        id="test.vanished-evidence",
        category="blocking_matrix",
        description="cites a test function that does not exist",
        status="proven_unit",
        evidence=("tests/acceptance/test_wave31_manifest.py",),
        test_nodeids=(
            "tests/acceptance/test_wave31_manifest.py::"
            "test_this_function_was_never_written",
        ),
    )
    errors = execute_manifest(_ROOT, (vanished,))
    assert any("not_collected" in error for error in errors)


def test_execute_manifest_over_the_real_manifest_all_cited_tests_pass() -> None:
    """The big claim: every test_nodeids entry any MANIFEST item cites
    actually passes right now. This is what makes a proven_unit status a
    verified fact rather than an assertion about a file's existence.

    Deliberately unmarked (no ``slow``/``clickhouse``-style marker): this is
    exactly the check that must run in every normal gate, never filtered out
    of a default CI pass -- a manifest whose own proof-of-proof is opt-out
    is the false-green failure mode this module exists to prevent.
    """

    errors = execute_manifest(_ROOT)
    assert errors == [], "\n".join(errors)


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
            "test_nodeids",
            "requires_live_infra",
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


def test_no_proven_unit_item_lacks_test_nodeids_in_the_real_manifest() -> None:
    for item in MANIFEST:
        if item.status == "proven_unit" and not item.requires_live_infra:
            assert item.test_nodeids, (
                f"{item.id} claims proven_unit with no test_nodeids -- a "
                "plausible file name is not proof"
            )


def test_every_test_nodeid_file_half_is_one_of_its_item_evidence() -> None:
    for item in MANIFEST:
        for node_id in item.test_nodeids:
            file_part = node_id.split("::", 1)[0]
            assert file_part in item.evidence, (
                f"{item.id}: {node_id} is not under its own evidence {item.evidence}"
            )
