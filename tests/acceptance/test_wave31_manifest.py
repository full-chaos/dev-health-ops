from __future__ import annotations

import hashlib
import json
import subprocess
import textwrap
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from scripts.acceptance.acceptance_artifact import (
    RUNTIME_DEPENDENCY_PATHS,
    AcceptanceFailure,
    aggregate_runtime_digest,
    runtime_dependency_hashes,
)
from scripts.acceptance.wave31_manifest import (
    MANIFEST,
    MANIFEST_SCHEMA_VERSION,
    MIGRATION_COEXISTENCE_REASON,
    STACK3_PERSISTENCE_GAP_REASON,
    STACK3_WIRING_GAP_REASON,
    TEAM_ATTRIBUTION_LIVE_DEFECT_REASON,
    ManifestIntegrityError,
    ManifestItem,
    build_report,
    execute_manifest,
    run_evidence_tests,
    validate_blocked_execution_artifact,
    validate_execution_artifact,
    validate_manifest,
)

_ROOT = Path(__file__).resolve().parents[2]

#: Independent literal snapshot of the golden reason constants -- see
#: golden_reason_snapshots.json's own "_note" for why this file exists and
#: is deliberately never imported by scripts/acceptance/wave31_manifest.py.
#: Values are either a plain reason string (the constant-keyed entries) or
#: a nested {item_id: reason} dict (BLOCKED_REASON_BY_ITEM_ID) -- see the
#: two typed accessors below rather than indexing this dict directly with
#: a `str` expectation.
_GOLDEN_REASON_SNAPSHOT: dict[str, Any] = json.loads(
    (Path(__file__).parent / "golden_reason_snapshots.json").read_text(encoding="utf-8")
)


def _golden_reason(key: str) -> str:
    value = _GOLDEN_REASON_SNAPSHOT[key]
    assert isinstance(value, str)
    return value


def _golden_blocked_reasons_by_item_id() -> dict[str, str]:
    value = _GOLDEN_REASON_SNAPSHOT["BLOCKED_REASON_BY_ITEM_ID"]
    assert isinstance(value, dict)
    return value


def _init_throwaway_git_repo(root: Path) -> str:
    """A minimal git repo in ``root`` with one commit, returning its SHA --
    ``validate_execution_artifact``'s ancestry check needs a real repo."""

    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
    (root / "placeholder.txt").write_text("x")
    # validate_execution_artifact digests the shared fixture surface, so a
    # throwaway root needs those paths present for an artifact to validate
    # at all -- committed, so the tree still starts clean.
    for relative in RUNTIME_DEPENDENCY_PATHS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("throwaway runtime dependency\n")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "initial"], cwd=root, check=True)
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _runtime_hashes_or_placeholder(root: Path) -> dict[str, str]:
    """Most guard tests build a throwaway root with no fixture surface at
    all; those cases are not about the digest, so they get values that are
    simply never compared against a real tree."""

    try:
        return runtime_dependency_hashes(root)
    except AcceptanceFailure:
        return {relative: "0" * 64 for relative in RUNTIME_DEPENDENCY_PATHS}


def _write_valid_artifact(
    root: Path,
    *,
    scenario_id: str,
    script_relative: str,
    commit_sha: str,
    tree_clean: bool = True,
    assertion_names: tuple[str, ...] = ("it_worked",),
    runtime_hashes: dict[str, str] | None = None,
) -> Path:
    script_path = root / script_relative
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# a throwaway smoke script\n")
    script_sha256 = hashlib.sha256(script_path.read_bytes()).hexdigest()
    now = datetime.now(UTC).isoformat()
    artifact = {
        "schema_version": "ask_dev_acceptance_artifact.v1",
        "scenario_id": scenario_id,
        "tree_clean": tree_clean,
        "tree_digest": hashlib.sha256(b"" if tree_clean else b"dirty").hexdigest(),
        "script": script_relative,
        "script_sha256": script_sha256,
        "runtime_digest": aggregate_runtime_digest(
            runtime_hashes
            if runtime_hashes is not None
            else _runtime_hashes_or_placeholder(root)
        ),
        "runtime_dependencies": dict(
            runtime_hashes
            if runtime_hashes is not None
            else _runtime_hashes_or_placeholder(root)
        ),
        "commit_sha": commit_sha,
        "command": "python throwaway.py",
        "started_at": now,
        "finished_at": now,
        "status": "passed",
        "error": None,
        "assertions": [
            {"name": name, "passed": True, "detail": "yes"} for name in assertion_names
        ],
    }
    artifact_path = root / "artifacts" / f"{scenario_id}.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    return artifact_path


def _write_failed_artifact(
    root: Path,
    *,
    scenario_id: str,
    script_relative: str,
    commit_sha: str,
    failing_assertion_names: tuple[str, ...],
    tree_clean: bool = True,
) -> Path:
    """A blocked row's supporting evidence: a real live attempt that
    genuinely failed on the named assertions -- mirrors _write_valid_
    artifact's shape but with status="failed" and specific passed=False
    entries, matching what smoke_ask_dev_stack3_intents.py's pre-CHAOS-3337
    artifacts actually looked like."""

    script_path = root / script_relative
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# a throwaway smoke script\n")
    script_sha256 = hashlib.sha256(script_path.read_bytes()).hexdigest()
    now = datetime.now(UTC).isoformat()
    artifact = {
        "schema_version": "ask_dev_acceptance_artifact.v1",
        "scenario_id": scenario_id,
        "tree_clean": tree_clean,
        "tree_digest": hashlib.sha256(b"" if tree_clean else b"dirty").hexdigest(),
        "script": script_relative,
        "script_sha256": script_sha256,
        "commit_sha": commit_sha,
        "command": "python throwaway.py",
        "started_at": now,
        "finished_at": now,
        "status": "failed",
        "error": "the live attempt failed as expected",
        "assertions": [
            {"name": "login_response_is_object", "passed": True, "detail": "ok"},
            *(
                {"name": name, "passed": False, "detail": "expected failure"}
                for name in failing_assertion_names
            ),
        ],
    }
    artifact_path = root / "artifacts" / f"{scenario_id}.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    return artifact_path


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
        # CHAOS-3408/3409/3421: the later "org-pinned wrong-subject +
        # 0-of-0 readiness answer" investigation's own defect set --
        # deliberately a distinct category from original_defect_
        # reproduction (CHAOS-3292's), not folded into it.
        "readiness_and_resolution_safety_defect_reproduction",
    }


def test_original_defect_reproductions_are_all_at_least_unit_proven() -> None:
    defects = [
        item for item in MANIFEST if item.category == "original_defect_reproduction"
    ]
    assert len(defects) == 5
    assert all(item.status in ("proven_unit", "proven_e2e") for item in defects)


def test_health_workload_deficiency_portfolio_flip_after_stack3_wiring() -> None:
    """Locks in the 2026-08-03 stack-3 wiring flip (ops #1383/#1387).

    STACK3_WIRING_GAP_REASON's root cause is resolved for PROJECT_HEALTH/
    TEAM_HEALTH/TEAM_WORKLOAD_BALANCE/OPERATIONAL_DEFICIENCY_INVENTORY --
    most of the 13 originally-blocked rows now flip to ``proven_unit``,
    backed by existing CHAOS-3302/3303/3304/3305 service-layer tests. A
    regression back to ``blocked`` (or a silent switch to some other
    status) without a corresponding new reason must fail here.
    """

    proven_ids = {
        "matrix.legitimate-org-wide-status",
        "matrix.project-health-mixed-dimensions",
        "matrix.team-health-complete-attribution",
        "matrix.team-health-unattributable-signals",
        "matrix.struggling-teams-positive",
        "matrix.struggling-teams-insufficient-sample",
        "matrix.overburdened-teams-with-denominators",
        "matrix.pressure-without-denominator",
        "matrix.light-on-feature-work-unclassified",
        "matrix.operational-deficiencies-mixed",
    }
    by_id = {item.id: item for item in MANIFEST}
    assert proven_ids <= by_id.keys()
    for item_id in proven_ids:
        item = by_id[item_id]
        assert item.status == "proven_unit", (
            f"{item_id} is not proven_unit: {item.status}"
        )
        assert item.blocked_reason is None
        assert item.test_nodeids, f"{item_id}: proven_unit with no test_nodeids"


def test_health_workload_deficiency_portfolio_items_still_honestly_blocked() -> None:
    """The rows the 2026-08-03 map found no adequate citation for must stay
    ``blocked`` rather than force-fit to a weak analogue. A future change
    that flips one of these without a genuinely matching test would be a
    false-green regression this test exists to catch.

    CHAOS-3393: ``matrix.organization-portfolio-status`` is no longer one
    of these -- it flipped to ``proven_unit`` (a real StepContext-widening
    fix landed, with matching orchestrator-level tests); see
    ``test_plan_registry_gap_gates_are_proven_at_unit_and_e2e_level``'s
    neighbors for its own coverage.
    """

    blocked_ids = {
        "matrix.project-health-unknown-not-applicable",
        "matrix.light-on-feature-work",
    }
    by_id = {item.id: item for item in MANIFEST}
    assert blocked_ids <= by_id.keys()
    for item_id in blocked_ids:
        item = by_id[item_id]
        assert item.status == "blocked", f"{item_id} is not blocked: {item.status}"
        assert item.blocked_reason, f"{item_id} is blocked with no reason"
        # None of these three share a single golden reason anymore (each
        # was blocked for a genuinely different cause -- deliberately
        # unwired, no matching test, or a weak analogue), so this pins
        # distinguishing content per row rather than one shared constant.
        assert item.blocked_reason != STACK3_WIRING_GAP_REASON
        assert item.blocked_reason != STACK3_PERSISTENCE_GAP_REASON


def test_the_two_rows_that_were_briefly_flipped_name_their_unreachable_input() -> None:
    """codex finding (HIGH, 2026-08-03). Both rows were flipped to
    ``proven_unit`` earlier that day on newly written tests, and both flips
    were wrong in the SAME way the rows were originally blocked for: the
    new test reached the asserted state only through an input production
    cannot produce -- a hand-set ``change_failure_rate_not_applicable``
    for a PROJECT subject, and a hand-set ``attribution_present=True`` for
    a rule whose only adapter reports ``False`` unconditionally.

    Reverting is not enough on its own: a future reader needs the reason to
    say WHY a plausible-looking test does not close the row, or the same
    flip happens again. This pins that the reason names the specific
    production guard, not just "no test exists".
    """

    by_id = {item.id: item for item in MANIFEST}

    project_health = by_id["matrix.project-health-unknown-not-applicable"]
    assert project_health.status == "blocked"
    assert project_health.test_nodeids == ()
    assert "CHANGE_FAILURE_RATE_SUPPORTED_SCOPES" in (
        project_health.blocked_reason or ""
    )

    feature_work = by_id["matrix.light-on-feature-work"]
    assert feature_work.status == "blocked"
    assert feature_work.test_nodeids == ()
    reason = feature_work.blocked_reason or ""
    assert "attribution_present=False" in reason
    assert "CHAOS-3331" in reason


def test_migration_coexistence_gate_reflects_chaos_3306_decision() -> None:
    by_id = {item.id: item for item in MANIFEST}
    item = by_id["gate.migration-coexistence"]
    assert item.status == "blocked"
    assert item.blocked_reason == MIGRATION_COEXISTENCE_REASON


def test_repeated_provider_gate_is_deferred_not_silently_green() -> None:
    by_id = {item.id: item for item in MANIFEST}
    item = by_id["gate.repeated-certified-provider"]
    assert item.status == "deferred"
    assert item.blocked_reason is not None
    assert "credentials" in item.blocked_reason


def test_team_attribution_flipped_to_proven_after_chaos_3332_fix() -> None:
    """Locks in the 2026-08-02 team-attribution flip.

    The item started ``blocked`` behind a newly discovered, 100%-
    reproducible live defect (CHAOS-3332) -- a DIFFERENT root cause from the
    13 stack-3-sequenced items, since TEAM is a supported_subject_kind on
    status.entity.v2, a WIRED plan. Once ops #1382 fixed the crash and this
    lane re-ran the exact-commit pattern against a named team subject on the
    fixed code, it flipped to ``proven_e2e`` with a real execution artifact.
    This test locks the flip in: a regression back to ``blocked`` without a
    corresponding new defect reason, or a silent switch to some other
    status, must fail here.
    """

    by_id = {item.id: item for item in MANIFEST}
    item = by_id["attack.team-attribution.e2e-blocked-by-live-defect"]
    assert item.status == "proven_e2e"
    assert item.blocked_reason is None
    assert item.requires_live_infra is True
    assert item.execution_artifact == "tests/acceptance/artifacts/team_attribution.json"
    assert "scripts/acceptance/smoke_ask_dev_team_attribution.py" in item.evidence
    assert "CHAOS-3332" in item.description
    assert "#1382" in item.description
    # Codex finding (MED, 2026-08-02, round 2): the row's claim is exactly
    # the CHAOS-3333 characterization (degraded, metrics empty, limitation
    # named) -- asserted directly by the smoke script and bound here so a
    # future change cannot silently widen or narrow what this row proves.
    assert item.required_assertion_names == (
        "named_team_committed",
        "no_internal_error_event",
        "answer_completed_event_present",
        "answer_status_is_degraded",
        "metrics_empty_for_team_scope",
        "limitation_names_unavailable_metric_source",
    )
    # The original repro is preserved as a historical record, not deleted --
    # this proves the constant still exists and still documents what was
    # found, even though it is no longer this item's blocked_reason.
    assert "CHAOS-3332" in TEAM_ATTRIBUTION_LIVE_DEFECT_REASON
    assert "NOT the CORE_PLANS_BY_INTENT gap" in TEAM_ATTRIBUTION_LIVE_DEFECT_REASON
    assert TEAM_ATTRIBUTION_LIVE_DEFECT_REASON != STACK3_WIRING_GAP_REASON


def test_golden_reason_constants_match_independent_snapshot() -> None:
    """Codex finding (MED, 2026-08-02, round 2): the tests above import
    STACK3_WIRING_GAP_REASON etc. from the SAME module that constructs the
    manifest, so a mutation to one of these constants moves both the
    production value and this test's comparison value together -- codex
    demonstrated live that appending "SILENTLY REWORDED" to a constant and
    re-running the suite stays green, because ``item.blocked_reason ==
    CONSTANT`` is trivially true when both sides are literally the same
    mutated object. ``golden_reason_snapshots.json`` is a manually
    maintained, independent literal copy that
    ``scripts/acceptance/wave31_manifest.py`` never reads -- comparing the
    live constant against it is the only check with the SILENTLY-REWORDED
    property: a wording change here without a matching edit to the JSON
    snapshot must fail.
    """

    assert STACK3_WIRING_GAP_REASON == _golden_reason("STACK3_WIRING_GAP_REASON")
    assert MIGRATION_COEXISTENCE_REASON == _golden_reason(
        "MIGRATION_COEXISTENCE_REASON"
    )
    assert TEAM_ATTRIBUTION_LIVE_DEFECT_REASON == _golden_reason(
        "TEAM_ATTRIBUTION_LIVE_DEFECT_REASON"
    )
    assert STACK3_PERSISTENCE_GAP_REASON == _golden_reason(
        "STACK3_PERSISTENCE_GAP_REASON"
    )


def test_golden_reason_mutation_is_actually_caught_not_just_theoretically() -> None:
    """RED-then-GREEN proof for the fix above, not just an assertion that
    could pass by construction. Simulates codex's exact demonstrated bypass
    (append "SILENTLY REWORDED" to a golden constant) against a COPY of the
    live value, and proves that copy no longer matches the independent
    snapshot -- the specific failure mode
    test_golden_reason_constants_match_independent_snapshot exists to catch.
    """

    mutated = STACK3_WIRING_GAP_REASON + " SILENTLY REWORDED"
    assert mutated != _golden_reason("STACK3_WIRING_GAP_REASON")


def test_stack3_persistence_gap_reason_is_preserved_as_history() -> None:
    """CHAOS-3337 (persistence-layer _SOURCE_CLASSES allowlist gap) shipped
    2026-08-03 (ops #1402) -- nothing in the current MANIFEST is blocked by
    it anymore, but the constant documenting the original repro is kept,
    still exported and tested, as the historical record."""

    assert "CHAOS-3337" in STACK3_PERSISTENCE_GAP_REASON
    assert "invalid source_class" in STACK3_PERSISTENCE_GAP_REASON
    assert STACK3_PERSISTENCE_GAP_REASON != STACK3_WIRING_GAP_REASON
    by_id = {item.id: item for item in MANIFEST}
    assert "matrix.stack3-intents.e2e-blocked-by-live-defect" not in by_id, (
        "superseded by the three per-intent proven_e2e rows below -- a "
        "single row cannot bind three distinct execution artifacts"
    )


def test_stack3_team_scoped_intents_flip_to_proven_e2e_after_chaos_3337() -> None:
    """Locks in the 2026-08-03 CHAOS-3337 flip: all three TEAM-subject
    stack-3 intents now complete live, each with its own execution
    artifact. PROJECT_HEALTH stays unverified live (zero PROJECT-kind
    fixture rows), so it has no row here.

    Codex finding (HIGH, 2026-08-03, round 4): scope+commit+non-error
    alone does not distinguish the claimed plan running from a legacy-loop
    fallback silently absorbing a missing plan-registry entry (portfolio_
    status_gap's own proof is exactly that fallback). required_assertion_
    names must include the dev_runs-backed plan-execution proof
    (preflight_proceeded_committed_subject + claimed_plan_step_completed),
    not just the reachability checks alone.
    """

    by_id = {item.id: item for item in MANIFEST}
    expected = {
        "matrix.team-health.e2e-live-validated": (
            "tests/acceptance/artifacts/team_health.json"
        ),
        "matrix.team-workload-balance.e2e-live-validated": (
            "tests/acceptance/artifacts/team_workload_balance.json"
        ),
        "matrix.operational-deficiency.e2e-live-validated": (
            "tests/acceptance/artifacts/operational_deficiency_team.json"
        ),
    }
    assert expected.keys() <= by_id.keys()
    for item_id, artifact_path in expected.items():
        item = by_id[item_id]
        assert item.status == "proven_e2e", f"{item_id}: {item.status}"
        assert item.requires_live_infra is True
        assert item.execution_artifact == artifact_path
        assert item.required_assertion_names == (
            "scope_resolved_event_present",
            "named_team_committed",
            "answer_completed_event_present",
            "answer_status_not_hard_error",
            "preflight_proceeded_committed_subject",
            "claimed_plan_step_completed",
            "stream_terminated_as_answer",
        )
        assert "CHAOS-3337" in item.description


def test_every_blocked_item_reason_matches_independent_snapshot() -> None:
    """Codex finding (MED, 2026-08-03, round 3): a blocked_reason with no
    independent binding validates clean however it is reworded --
    validate_manifest's only check is "non-empty". BLOCKED_REASON_BY_
    ITEM_ID snapshots every currently-blocked item by id; a blocked item
    missing from it, or present with drifted text, both fail here.
    """

    blocked_snapshot = _golden_blocked_reasons_by_item_id()
    by_id = {item.id: item for item in MANIFEST}
    blocked_ids = {item.id for item in MANIFEST if item.status == "blocked"}
    missing = blocked_ids - set(blocked_snapshot.keys())
    assert not missing, f"blocked items with no golden-snapshot entry: {missing}"
    stale = set(blocked_snapshot.keys()) - blocked_ids
    assert not stale, f"golden snapshot entries for items no longer blocked: {stale}"
    for item_id in blocked_ids:
        assert by_id[item_id].blocked_reason == blocked_snapshot[item_id], item_id


def test_blocked_reason_mutation_is_actually_caught() -> None:
    """RED-then-GREEN proof for the fix above: plant codex's exact
    demonstrated bypass (a reason that is 'fabricated but nonempty') on a
    real blocked item and confirm the independent-snapshot check -- not
    validate_manifest's non-empty check -- is what catches it.
    """

    # CHAOS-3393: matrix.organization-portfolio-status flipped to
    # proven_unit (status.portfolio.v1 is wired now), so this RED-then-
    # GREEN proof uses a different item that is still genuinely blocked.
    fabricated = ManifestItem(
        id="matrix.project-health-unknown-not-applicable",
        category="blocking_matrix",
        description="mutated",
        status="blocked",
        blocked_reason="fabricated but nonempty",
    )
    # validate_manifest's own check (non-empty blocked_reason) does NOT
    # catch this -- proving the independent snapshot is doing real work,
    # not restating an existing guard.
    assert validate_manifest(_ROOT, (fabricated,)) == []
    blocked_snapshot = _golden_blocked_reasons_by_item_id()
    assert fabricated.blocked_reason != blocked_snapshot[fabricated.id]


def test_plan_registry_gap_gates_are_proven_at_unit_and_e2e_level() -> None:
    by_id = {item.id: item for item in MANIFEST}
    unit_item = by_id["gate.plan-registry-gap-is-loud"]
    assert unit_item.status == "proven_unit"
    assert unit_item.test_nodeids == (
        "tests/api/dev/test_chaos_3300_plan_registry_gap.py::"
        "test_plan_registry_gap_is_loud_for_a_normally_plan_governed_intent",
        "tests/api/dev/test_chaos_3300_plan_registry_gap.py::"
        "test_bounded_investigation_never_triggers_the_gap_signal",
    )

    e2e_item = by_id["gate.plan-registry-gap-is-loud.e2e-live-validated"]
    assert e2e_item.status == "proven_e2e"
    assert e2e_item.requires_live_infra is True
    assert (
        e2e_item.execution_artifact
        == "tests/acceptance/artifacts/portfolio_status_gap.json"
    )
    assert e2e_item.required_assertion_names == (
        "scope_resolved_event_present",
        "answer_completed_event_present",
        "answer_status_is_exactly_partial",
        "stream_terminated_as_answer",
        "warnings_present_but_not_a_plan_registry_gap_signal",
    )


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


def test_validate_manifest_is_clean_over_a_single_well_formed_item(
    tmp_path: Path,
) -> None:
    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="well_formed",
        script_relative="smoke_well_formed.py",
        commit_sha=commit_sha,
    )
    clean = ManifestItem(
        id="test.well-formed",
        category="blocking_matrix",
        description="a correctly evidenced item",
        status="proven_e2e",
        evidence=("smoke_well_formed.py",),
        requires_live_infra=True,
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
    )
    assert validate_manifest(tmp_path, (clean,)) == []


# --- validate_execution_artifact: codex finding (HIGH, 2026-08-02) --
# a proven_e2e claim previously needed no machine-verifiable execution at
# all; every check below closes one specific way that could still be true ---


def _proven_e2e_item(
    *,
    execution_artifact: str | None,
    evidence: str,
    required_assertion_names: tuple[str, ...] = (),
) -> ManifestItem:
    return ManifestItem(
        id="test.e2e-claim",
        category="blocking_matrix",
        description="a proven_e2e claim under test",
        status="proven_e2e",
        evidence=(evidence,),
        requires_live_infra=True,
        execution_artifact=execution_artifact,
        required_assertion_names=required_assertion_names,
    )


def test_validate_execution_artifact_accepts_a_real_passing_artifact(
    tmp_path: Path,
) -> None:
    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path, scenario_id="ok", script_relative="smoke_ok.py", commit_sha=commit_sha
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_ok.py",
    )
    assert validate_execution_artifact(tmp_path, item) == []


def test_validate_execution_artifact_rejects_a_fabricated_row_with_no_artifact(
    tmp_path: Path,
) -> None:
    """The exact codex repro: an evidence file exists and
    requires_live_infra=True, but the row was never actually run."""

    (tmp_path / "smoke_fabricated.py").write_text("# never run\n")
    item = _proven_e2e_item(execution_artifact=None, evidence="smoke_fabricated.py")
    errors = validate_execution_artifact(tmp_path, item)
    assert any("no execution_artifact" in error for error in errors)


def test_validate_execution_artifact_rejects_a_missing_artifact_file(
    tmp_path: Path,
) -> None:
    item = _proven_e2e_item(
        execution_artifact="artifacts/does_not_exist.json",
        evidence="smoke_missing.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("does not exist" in error for error in errors)


def test_validate_execution_artifact_rejects_malformed_json(tmp_path: Path) -> None:
    artifact_path = tmp_path / "artifacts" / "broken.json"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text("{not valid json")
    item = _proven_e2e_item(
        execution_artifact="artifacts/broken.json", evidence="smoke_broken.py"
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("not valid JSON" in error for error in errors)


def test_validate_execution_artifact_rejects_a_failing_assertion(
    tmp_path: Path,
) -> None:
    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="has_failure",
        script_relative="smoke_has_failure.py",
        commit_sha=commit_sha,
    )
    artifact = json.loads(artifact_path.read_text())
    artifact["assertions"].append(
        {"name": "it_actually_broke", "passed": False, "detail": "nope"}
    )
    artifact["status"] = "failed"
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_has_failure.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("failing assertion" in error for error in errors)
    assert any("status is 'failed'" in error for error in errors)


@pytest.mark.parametrize(
    "commit_ish",
    ["HEAD", "HEAD^{commit}", "HEAD~0", "main", "v1.0.0", "e395de5", ""],
)
def test_validate_execution_artifact_rejects_a_non_immutable_commit_ish(
    tmp_path: Path, commit_ish: str
) -> None:
    """The commit id is metadata now, not a binding, but it still has to be
    an immutable one. codex finding (HIGH, 2026-08-03): it went straight to
    `git merge-base`, which accepts any revision expression, so an artifact
    recording the literal "HEAD" resolved at read time to whatever HEAD is
    now and said nothing about what ran. Every mutable or abbreviated
    spelling is rejected."""

    _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="mutable_ref",
        script_relative="smoke_mutable_ref.py",
        commit_sha=commit_ish,
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_mutable_ref.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert errors, f"{commit_ish!r} was accepted as an immutable commit id"
    assert any(
        "canonical 40-character hexadecimal commit id" in error
        or "no commit_sha" in error
        for error in errors
    )


def test_validate_execution_artifact_rejects_an_artifact_with_no_runtime_digest(
    tmp_path: Path,
) -> None:
    """An artifact predating the field is not grandfathered: from here,
    "minted before runtime_digest existed" and "minted by something that
    skipped it" are indistinguishable, and neither is proof."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    for relative in RUNTIME_DEPENDENCY_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original\n")
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="legacy_digest",
        script_relative="smoke_legacy_digest.py",
        commit_sha=commit_sha,
        runtime_hashes=runtime_dependency_hashes(tmp_path),
    )
    artifact = json.loads(artifact_path.read_text())
    del artifact["runtime_dependencies"]
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_legacy_digest.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("records no runtime_dependencies" in error for error in errors)


def test_validate_execution_artifact_still_accepts_a_real_full_sha(
    tmp_path: Path,
) -> None:
    """The control for the check above: tightening the input format must
    not reject the genuine, correctly-recorded case."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    assert len(commit_sha) == 40
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="real_sha",
        script_relative="smoke_real_sha.py",
        commit_sha=commit_sha,
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_real_sha.py",
    )
    assert validate_execution_artifact(tmp_path, item) == []


def test_validate_execution_artifact_rejects_a_script_edited_since_it_ran(
    tmp_path: Path,
) -> None:
    """The scenario the row cites was validated against different code than
    what exists now -- codex's "stale" case."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="edited",
        script_relative="smoke_edited.py",
        commit_sha=commit_sha,
    )
    (tmp_path / "smoke_edited.py").write_text(
        "# edited after the artifact was minted\n"
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_edited.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("script_sha256 does not match" in error for error in errors)


# --- artifact-integrity round 2: codex finding (HIGH, 2026-08-02, round 2)
# -- "artifact exists and all-passes" did not bind the artifact to the row
# citing it, nor prove the tree that produced it was clean. Each test below
# plants exactly the mutation codex named and proves it fails now. ---


def test_validate_execution_artifact_rejects_an_artifact_swap(tmp_path: Path) -> None:
    """codex mutation: swap scenario_a's artifact content onto scenario_b's
    row (e.g. a copy-paste or a stale symlink) -- both artifacts are
    individually all-passing, but this row expects ITS OWN scenario."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    other_artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="scenario_a",
        script_relative="smoke_scenario_a.py",
        commit_sha=commit_sha,
    )
    # This row's execution_artifact filename says "scenario_b", but the
    # file's own recorded scenario_id (copied wholesale from scenario_a)
    # says "scenario_a" -- exactly what an artifact swap looks like.
    swapped_path = tmp_path / "artifacts" / "scenario_b.json"
    swapped_path.write_text(other_artifact_path.read_text(encoding="utf-8"))
    item = _proven_e2e_item(
        execution_artifact="artifacts/scenario_b.json",
        evidence="smoke_scenario_a.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("scenario_id" in error and "does not match" in error for error in errors)


def test_validate_execution_artifact_rejects_an_ancient_but_dirty_tree(
    tmp_path: Path,
) -> None:
    """codex mutation: the recorded commit IS a genuine ancestor of HEAD (the
    ancestor check alone would pass), but the tree was dirty when the
    artifact was minted -- commit_sha being a true ancestor does not prove
    commit_sha actually describes what ran."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="dirty",
        script_relative="smoke_dirty.py",
        commit_sha=commit_sha,
        tree_clean=False,
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_dirty.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("tree_clean" in error for error in errors)
    # Confirms the ancestor check by itself is NOT what is failing here --
    # tree_clean is a genuinely independent gate, not a restatement.
    assert not any("is not an ancestor of" in error for error in errors)


def test_validate_execution_artifact_rejects_an_unrecorded_tree_state(
    tmp_path: Path,
) -> None:
    """A legacy artifact minted before tree_clean existed must be treated as
    unrecorded, not silently accepted as clean by default."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="legacy",
        script_relative="smoke_legacy.py",
        commit_sha=commit_sha,
    )
    artifact = json.loads(artifact_path.read_text())
    del artifact["tree_clean"]
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_legacy.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("tree_clean" in error for error in errors)


def test_validate_execution_artifact_rejects_a_fabricated_assertions_list(
    tmp_path: Path,
) -> None:
    """codex mutation: assertions is a list of bare strings, not dicts. The
    original ``isinstance(entry, dict)`` filter silently excluded these from
    the "failing" check, so ``["fabricated"]`` validated clean as long as
    status=="passed" -- this must now be rejected outright."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="fabricated",
        script_relative="smoke_fabricated_list.py",
        commit_sha=commit_sha,
    )
    artifact = json.loads(artifact_path.read_text())
    artifact["assertions"] = ["fabricated"]
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_fabricated_list.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("non-dict assertion" in error for error in errors)


def test_validate_execution_artifact_rejects_a_missing_required_assertion(
    tmp_path: Path,
) -> None:
    """An artifact can be real, current, and all-passing, and still not
    prove THIS row's specific claim if the load-bearing assertion this row
    names never ran (e.g. only boilerplate auth/SSE-plumbing checks fired)."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="incomplete",
        script_relative="smoke_incomplete.py",
        commit_sha=commit_sha,
        assertion_names=("login_response_is_object",),
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_incomplete.py",
        required_assertion_names=("the_actual_claim_this_row_makes",),
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("missing required" in error for error in errors)


def test_validate_execution_artifact_rejects_a_leaked_jwt(tmp_path: Path) -> None:
    """Codex finding (HIGH, 2026-08-02): six committed artifacts leaked live
    JWTs via ``str(response)`` assertion details. Redaction at the recorder
    is the primary fix; this is the independent backstop -- an artifact
    whose raw bytes still contain a JWT-shaped token must fail the manifest
    even if every assertion otherwise passes."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="leaky",
        script_relative="smoke_leaky.py",
        commit_sha=commit_sha,
    )
    artifact = json.loads(artifact_path.read_text())
    fake_jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiJ0ZXN0In0"
        ".c2lnbmF0dXJlLXBsYWNlaG9sZGVyLWJ5dGVz"
    )
    artifact["assertions"][0]["detail"] = f"leaked token: {fake_jwt}"
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_leaky.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("JWT-shaped token" in error for error in errors)


def test_validate_execution_artifact_rejects_a_script_outside_its_own_evidence(
    tmp_path: Path,
) -> None:
    """The script that produced the artifact must be one of THIS row's cited
    evidence paths -- otherwise a row could bind to any script's artifact by
    filename alone, regardless of what evidence it actually names."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="wrong_script",
        script_relative="smoke_actually_ran.py",
        commit_sha=commit_sha,
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_something_else_entirely.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any(
        "is not among this item's own evidence paths" in error for error in errors
    )


# --- blocked-artifact binding: codex finding (MED, 2026-08-03, round 3) --
# a blocked row citing a failed live attempt had NOTHING checking that
# evidence -- a nonexistent artifact, or one recording status="passed",
# validated exactly the same as a real one. Every check below closes one
# specific way that could still be true, and confirms the mechanism can
# NEVER be used to promote a row's status. ---


def _blocked_item(
    *,
    blocked_execution_artifact: str | None,
    evidence: tuple[str, ...] = (),
    blocked_expected_failing_assertions: tuple[str, ...] = (),
) -> ManifestItem:
    return ManifestItem(
        id="test.blocked-claim",
        category="blocking_matrix",
        description="a blocked claim with supporting live evidence",
        status="blocked",
        blocked_reason="a real, checkable reason",
        evidence=evidence,
        blocked_execution_artifact=blocked_execution_artifact,
        blocked_expected_failing_assertions=blocked_expected_failing_assertions,
    )


def test_validate_blocked_execution_artifact_accepts_a_real_failed_artifact(
    tmp_path: Path,
) -> None:
    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_failed_artifact(
        tmp_path,
        scenario_id="genuinely_failed",
        script_relative="smoke_genuinely_failed.py",
        commit_sha=commit_sha,
        failing_assertion_names=("scope_resolved_event_present",),
    )
    item = _blocked_item(
        blocked_execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence=("smoke_genuinely_failed.py",),
        blocked_expected_failing_assertions=("scope_resolved_event_present",),
    )
    assert validate_blocked_execution_artifact(tmp_path, item) == []


def test_validate_blocked_execution_artifact_rejects_a_missing_artifact(
    tmp_path: Path,
) -> None:
    """codex mutation: a nonexistent artifact reference must fail, not
    validate clean the way it did before this mechanism existed."""

    item = _blocked_item(
        blocked_execution_artifact="artifacts/does_not_exist.json",
    )
    errors = validate_blocked_execution_artifact(tmp_path, item)
    assert any("does not exist" in error for error in errors)


def test_validate_blocked_execution_artifact_rejects_a_passed_status(
    tmp_path: Path,
) -> None:
    """codex mutation: a 'passed' artifact backing a blocked claim is
    self-contradictory -- the exact scenario a proven row citing failed
    evidence via the wrong field mirrors from the other direction."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="secretly_passed",
        script_relative="smoke_secretly_passed.py",
        commit_sha=commit_sha,
    )
    item = _blocked_item(
        blocked_execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence=("smoke_secretly_passed.py",),
    )
    errors = validate_blocked_execution_artifact(tmp_path, item)
    assert any("expected 'failed'" in error for error in errors)


def test_validate_blocked_execution_artifact_rejects_when_set_on_a_non_blocked_item() -> (
    None
):
    """codex mutation: this field must never appear on a non-blocked
    item -- it exists only to back a staying-blocked claim, never as an
    alternate path to "proven"."""

    proven_with_blocked_artifact = ManifestItem(
        id="test.proven-with-blocked-artifact",
        category="blocking_matrix",
        description="a proven claim that also sets blocked_execution_artifact",
        status="proven_unit",
        evidence=("tests/acceptance/ask-dev-oracle.v1.json",),
        test_nodeids=(
            "tests/api/dev/test_tool_registry.py::"
            "test_manifest_is_the_exact_nine_tool_server_allowlist",
        ),
        blocked_execution_artifact="artifacts/whatever.json",
    )
    errors = validate_blocked_execution_artifact(_ROOT, proven_with_blocked_artifact)
    assert any(
        "blocked_execution_artifact set on a non-blocked item" in error
        for error in errors
    )


def test_validate_blocked_execution_artifact_rejects_a_missing_expected_failure(
    tmp_path: Path,
) -> None:
    """An artifact can be real, current, and genuinely status='failed', and
    still not prove THIS row's specific expected failure if the assertion
    it names never actually failed (e.g. it failed on something else
    entirely)."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_failed_artifact(
        tmp_path,
        scenario_id="wrong_failure",
        script_relative="smoke_wrong_failure.py",
        commit_sha=commit_sha,
        failing_assertion_names=("some_unrelated_assertion",),
    )
    item = _blocked_item(
        blocked_execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence=("smoke_wrong_failure.py",),
        blocked_expected_failing_assertions=("scope_resolved_event_present",),
    )
    errors = validate_blocked_execution_artifact(tmp_path, item)
    assert any(
        "does not record the expected failing assertion" in error for error in errors
    )


def test_validate_execution_artifact_rejects_a_failed_artifact_on_a_proven_row(
    tmp_path: Path,
) -> None:
    """codex mutation, explicitly planted: a genuinely-failed live-attempt
    artifact (the same shape a blocked row's supporting evidence uses)
    cited via execution_artifact -- the proven_e2e field -- on a proven
    row. This is the mirror image of a 'passed' artifact backing a blocked
    claim, and must be rejected just as loudly."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_failed_artifact(
        tmp_path,
        scenario_id="failed_but_cited_as_proof",
        script_relative="smoke_failed_but_cited_as_proof.py",
        commit_sha=commit_sha,
        failing_assertion_names=("scope_resolved_event_present",),
    )
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_failed_but_cited_as_proof.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("expected 'passed'" in error for error in errors)
    assert any("failing assertion" in error for error in errors)


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


def test_run_evidence_tests_marks_a_skipped_test_as_skipped_not_passed(
    tmp_path: Path,
) -> None:
    """Codex finding (MED, 2026-08-02): a measurement that never ran must
    never read as passed. A skip is exactly that -- the assertion body
    never executed."""

    (tmp_path / "test_throwaway_skip.py").write_text(
        textwrap.dedent(
            """\
            import pytest

            @pytest.mark.skip(reason="deliberately never measured")
            def test_it_skips() -> None:
                assert True
            """
        )
    )
    outcomes = run_evidence_tests(tmp_path, ("test_throwaway_skip.py::test_it_skips",))
    assert outcomes == {"test_throwaway_skip.py::test_it_skips": "skipped"}


def test_run_evidence_tests_marks_an_xfail_test_as_xfail_not_passed(
    tmp_path: Path,
) -> None:
    """Codex finding (MED, 2026-08-02): the original code mapped XFAIL to
    "passed". An xfail means the behavior the manifest cites as proof is
    documented as EXPECTED TO FAIL -- the opposite of proof it works."""

    (tmp_path / "test_throwaway_xfail.py").write_text(
        textwrap.dedent(
            """\
            import pytest

            @pytest.mark.xfail(reason="known broken, not fixed yet")
            def test_it_xfails() -> None:
                assert False
            """
        )
    )
    outcomes = run_evidence_tests(
        tmp_path, ("test_throwaway_xfail.py::test_it_xfails",)
    )
    assert outcomes == {"test_throwaway_xfail.py::test_it_xfails": "xfail"}


def test_run_evidence_tests_marks_an_xpass_test_as_xpass_not_passed(
    tmp_path: Path,
) -> None:
    """An xpass (an xfail-marked test that unexpectedly passed) is a claim
    that something is STILL documented as broken, not live proof it works --
    must not be coerced into "passed" either."""

    (tmp_path / "test_throwaway_xpass.py").write_text(
        textwrap.dedent(
            """\
            import pytest

            @pytest.mark.xfail(reason="thought this was broken")
            def test_it_xpasses() -> None:
                assert True
            """
        )
    )
    outcomes = run_evidence_tests(
        tmp_path, ("test_throwaway_xpass.py::test_it_xpasses",)
    )
    assert outcomes == {"test_throwaway_xpass.py::test_it_xpasses": "xpass"}


def test_execute_manifest_rejects_skipped_xfail_and_xpass_cited_tests(
    tmp_path: Path,
) -> None:
    (tmp_path / "test_throwaway_nonpassing.py").write_text(
        textwrap.dedent(
            """\
            import pytest

            @pytest.mark.skip(reason="never measured")
            def test_skipped() -> None:
                assert True

            @pytest.mark.xfail(reason="known broken")
            def test_xfailed() -> None:
                assert False

            @pytest.mark.xfail(reason="thought broken")
            def test_xpassed() -> None:
                assert True
            """
        )
    )
    claims = tuple(
        ManifestItem(
            id=f"test.nonpassing-{name}",
            category="blocking_matrix",
            description="a claim resting on a non-passing pytest outcome",
            status="proven_unit",
            evidence=("test_throwaway_nonpassing.py",),
            test_nodeids=(f"test_throwaway_nonpassing.py::{name}",),
        )
        for name in ("test_skipped", "test_xfailed", "test_xpassed")
    )
    errors = execute_manifest(tmp_path, claims)
    assert any("test_skipped -> skipped" in error for error in errors)
    assert any("test_xfailed -> xfail" in error for error in errors)
    assert any("test_xpassed -> xpass" in error for error in errors)


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
            "execution_artifact",
            "required_assertion_names",
        }
    # CHAOS-3479: the stale_dependencies / stale_row_count / stale_rows
    # fields are GONE with the dependency-drift mechanism. Asserting the
    # item key set is exact (== not <=) is what keeps this honest: if a
    # future change re-adds a staleness field, this fails rather than
    # silently tolerating it.
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


def test_an_artifact_with_extra_dependency_keys_is_rejected(tmp_path: Path) -> None:
    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="extra_keys",
        script_relative="smoke_extra_keys.py",
        commit_sha=commit_sha,
        runtime_hashes=runtime_dependency_hashes(tmp_path),
    )
    artifact = json.loads(artifact_path.read_text())
    artifact["runtime_dependencies"]["src/not/covered.py"] = "0" * 64
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_extra_keys.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("not covered dependencies" in e for e in errors)


@pytest.mark.parametrize("bad_value", [None, "", "not-a-digest", 12345])
def test_an_artifact_with_a_malformed_dependency_hash_is_rejected(
    tmp_path: Path, bad_value: object
) -> None:
    """A None or empty hash used to read as ordinary drift, which a
    declaration could then absorb -- an absent measurement laundered into
    an acknowledged one."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="bad_hash",
        script_relative="smoke_bad_hash.py",
        commit_sha=commit_sha,
        runtime_hashes=runtime_dependency_hashes(tmp_path),
    )
    artifact = json.loads(artifact_path.read_text())
    artifact["runtime_dependencies"]["pyproject.toml"] = bad_value
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_bad_hash.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("malformed" in e for e in errors)


def test_an_artifact_whose_aggregate_disagrees_with_its_parts_is_rejected(
    tmp_path: Path,
) -> None:
    """The recorder writes both together, so a disagreement means the file
    was hand-edited -- exactly the shape of a doctored artifact."""

    commit_sha = _init_throwaway_git_repo(tmp_path)
    artifact_path = _write_valid_artifact(
        tmp_path,
        scenario_id="aggregate_mismatch",
        script_relative="smoke_aggregate.py",
        commit_sha=commit_sha,
        runtime_hashes=runtime_dependency_hashes(tmp_path),
    )
    artifact = json.loads(artifact_path.read_text())
    artifact["runtime_digest"] = "0" * 64
    artifact_path.write_text(json.dumps(artifact))
    item = _proven_e2e_item(
        execution_artifact=str(artifact_path.relative_to(tmp_path)),
        evidence="smoke_aggregate.py",
    )
    errors = validate_execution_artifact(tmp_path, item)
    assert any("does not agree with its own per-path digests" in e for e in errors)


def test_the_report_states_what_proven_e2e_does_not_assert() -> None:
    """codex finding (HIGH, 2026-08-03): nine row descriptions say a live
    run is "proven", while a hand-written artifact with correct hashes
    validates clean. The rows keep their wording; the report carries one
    authoritative statement of the limits, so no consumer reads the rows
    without it. This pins the load-bearing admissions, not the prose."""

    limits = build_report(_ROOT)["proven_e2e_claim_limits"]
    assert "fabricated artifact validates clean" in limits
    assert "metadata only" in limits
    assert "historical records" in limits
    assert "does NOT mean" in limits


def test_committed_report_matches_what_build_report_produces_now() -> None:
    """The committed report is the machine-readable deliverable; the module is
    the source of truth. Nothing tied them together, so a declaration added to
    the source (e.g. a newly-acknowledged runtime dependency drift) could ship
    while the committed report still described the old, smaller changed
    surface -- consumers reading the report would understate it.

    Codex adversarial review (MEDIUM, confirmed) during CHAOS-3463, whose
    launcher change added exactly such a declaration.
    """

    root = Path(__file__).resolve().parents[2]
    committed = json.loads(
        (root / "tests" / "acceptance" / "wave31-manifest-report.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert committed == build_report(root), (
        "tests/acceptance/wave31-manifest-report.v1.json is out of date with "
        "wave31_manifest.build_report(). Regenerate it in the same change that "
        "altered the manifest: python scripts/acceptance/wave31_manifest.py "
        "--skip-execution"
    )
