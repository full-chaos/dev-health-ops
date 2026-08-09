"""CHAOS-3616: the coverage matrix is total, and its gaps are visible.

A coverage table's dangerous failure is not a red cell — it is a blank one. A
reader who sees nothing in a cell reads "fine"; a reader who sees `--` reads
"nobody measures this". These tests exist to keep the second reading the only
possible one.

The guard-injection script is checked from here too. Its case table must
account for every scoring dimension — injected or explicitly excused — because
a dimension whose expectation nobody has watched fail is a coverage claim with
nothing behind it, whatever the matrix says.
"""

from __future__ import annotations

import dataclasses
import importlib.util
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ALL_QUESTION_FAMILY_IDS,
    ALL_SCORING_DIMENSION_IDS,
    QUESTION_FAMILY_REGISTRY,
)
from dev_health_ops.api.dev.investigation_corpus import cases as cases_module
from dev_health_ops.api.dev.investigation_corpus import coverage as coverage_module
from dev_health_ops.api.dev.investigation_corpus.cases import (
    CASE_REGISTRY,
    CaseDisposition,
)
from dev_health_ops.api.dev.investigation_corpus.coverage import (
    CellStatus,
    coverage_matrix,
    dispositions_table,
    render_dispositions,
    render_matrix,
    validate_coverage,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
_GUARD_SCRIPT = _REPOSITORY_ROOT / "scripts" / "verify_chaos_3616_oracle_guards.py"


def _load_guard_script() -> Any:
    spec = importlib.util.spec_from_file_location(
        "chaos_3616_guard_script", _GUARD_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# Totality
# --------------------------------------------------------------------------


def test_matrix_validates() -> None:
    validate_coverage()


def test_the_matrix_has_a_cell_for_every_family_and_dimension() -> None:
    matrix = coverage_matrix()
    assert len(matrix) == len(ALL_QUESTION_FAMILY_IDS) * len(ALL_SCORING_DIMENSION_IDS)


def test_every_dimension_the_frozen_registry_declares_is_exercised() -> None:
    """The registry's per-family list is a promise the corpus has to keep."""

    matrix = coverage_matrix()
    unmet: list[str] = []
    for family_id in ALL_QUESTION_FAMILY_IDS:
        for dimension_id in QUESTION_FAMILY_REGISTRY[family_id].scoring_dimension_ids:
            if matrix[(family_id, dimension_id)].status is not CellStatus.COVERED:
                unmet.append(f"{family_id.value}/{dimension_id.value}")
    assert not unmet, (
        f"the frozen registry declares these family/dimension pairs and no "
        f"authored case exercises them: {unmet}"
    )


def test_every_dimension_is_covered_somewhere() -> None:
    matrix = coverage_matrix()
    orphans = [
        dimension_id.value
        for dimension_id in ALL_SCORING_DIMENSION_IDS
        if not any(
            matrix[(family_id, dimension_id)].status is CellStatus.COVERED
            for family_id in ALL_QUESTION_FAMILY_IDS
        )
    ]
    assert not orphans, f"dimensions no authored case scores anywhere: {orphans}"


# --------------------------------------------------------------------------
# Gaps are visible
# --------------------------------------------------------------------------


def test_unmapped_cells_render_as_an_explicit_gap() -> None:
    rendered = render_matrix()
    assert "--" in rendered, (
        "the rendered matrix contains no explicit gap marker, which means "
        "either every cell is covered (check the counts) or gaps are being "
        "rendered as blanks"
    )
    assert "no case scores it there" in rendered, "the legend does not explain `--`"


def test_a_skipped_only_cell_is_visibly_distinct_from_a_covered_one() -> None:
    """The X01 disposition must show through as `skip`, not as a count.

    This is the cell-level statement of the issue's rule. A skipped case is
    not a failure; it is also not coverage, and the two must not render the
    same way.
    """

    matrix = coverage_matrix()
    skipped = [
        cell for cell in matrix.values() if cell.status is CellStatus.SKIPPED_ONLY
    ]
    assert skipped, (
        "no skipped-only cell exists, so the SKIPPED_ONLY rendering is "
        "untested and the first real skip will render as a gap or a count"
    )
    for cell in skipped:
        assert cell.symbol == "skip"
        assert not cell.authored_case_ids
        assert cell.skipped_case_ids
    assert "skip" in render_matrix()


def test_the_disposition_table_lists_every_skip_with_its_reason() -> None:
    rendered = render_dispositions()
    skipped = dispositions_table()
    assert skipped
    for case in skipped:
        assert case.case_id in rendered
        assert case.disposition_reason.split(".")[0].strip() in rendered


def test_no_authored_case_appears_in_the_disposition_table() -> None:
    authored = {
        case.case_id
        for case in CASE_REGISTRY.values()
        if case.disposition is CaseDisposition.AUTHORED
    }
    assert not authored & {case.case_id for case in dispositions_table()}


# --------------------------------------------------------------------------
# The guards reject what they claim to
# --------------------------------------------------------------------------


def test_a_dimension_nothing_exercises_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strip a dimension from every case and require the matrix to complain."""

    target = next(
        dimension
        for dimension in QUESTION_FAMILY_REGISTRY[
            ALL_QUESTION_FAMILY_IDS[0]
        ].scoring_dimension_ids
    )
    registry = {
        case_id: dataclasses.replace(
            case,
            scoring_dimension_ids=tuple(
                item for item in case.scoring_dimension_ids if item is not target
            ),
        )
        for case_id, case in CASE_REGISTRY.items()
    }
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    monkeypatch.setattr(coverage_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="no authored corpus case exercises it"):
        validate_coverage()


def test_a_cell_filled_only_by_a_skipped_case_is_not_counted_as_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Turning an authored case into a skip must change the cell, not hide it."""

    case_id = "T01_clearly_struggling_team"
    case = CASE_REGISTRY[case_id]
    registry = dict(CASE_REGISTRY)
    registry[case_id] = dataclasses.replace(
        case,
        disposition=CaseDisposition.UNMEASURABLE,
        disposition_reason="Skipped for the purposes of this test only.",
    )
    monkeypatch.setattr(coverage_module, "CASE_REGISTRY", registry)
    matrix = coverage_matrix()
    cell = matrix[(case.question_family, case.scoring_dimension_ids[0])]
    assert case_id not in cell.authored_case_ids
    if cell.status is CellStatus.SKIPPED_ONLY:
        assert cell.symbol == "skip"


# --------------------------------------------------------------------------
# The injection table is total
# --------------------------------------------------------------------------


def test_the_guard_injection_script_accounts_for_every_dimension() -> None:
    """An uninjected dimension is a coverage claim nobody has watched fail."""

    module = _load_guard_script()
    injected = {case.dimension for case in module.CASES}
    excused = set(module.UNINJECTED)
    unaccounted = sorted(
        item.value for item in set(ALL_SCORING_DIMENSION_IDS) - injected - excused
    )
    assert not unaccounted, (
        "these dimensions have neither an injection case nor a stated reason "
        f"for having none: {unaccounted}"
    )
    assert not (injected & excused)


def test_every_excused_dimension_states_a_substantive_reason() -> None:
    module = _load_guard_script()
    for dimension, reason in module.UNINJECTED.items():
        assert len(reason.strip()) >= 60, dimension


def test_every_injection_case_names_a_real_case_and_mutation() -> None:
    module = _load_guard_script()
    for case in module.CASES:
        assert case.case_id in CASE_REGISTRY, case.dimension
        assert case.mutation in module.MUTATIONS, case.mutation
        assert (case.oracle_field is None) != (case.world_neutralizer is None), (
            f"{case.dimension}: an injection case must neutralize exactly one "
            "of an oracle field or a world fact"
        )
        if case.world_neutralizer is not None:
            assert case.world_neutralizer in module.NEUTRALIZERS


def test_every_injection_case_targets_a_dimension_its_case_scores() -> None:
    """A case that does not score the dimension cannot prove anything about it."""

    module = _load_guard_script()
    for case in module.CASES:
        scored = CASE_REGISTRY[case.case_id].scoring_dimension_ids
        assert case.dimension in scored, (
            f"{case.dimension.value} is injected on {case.case_id}, which does "
            "not score it"
        )


# --------------------------------------------------------------------------
# The published matrix is the real one
# --------------------------------------------------------------------------


_ARCHITECTURE_DOC = (
    _REPOSITORY_ROOT
    / "docs"
    / "contribute"
    / "architecture"
    / "ask-dev-investigation-corpus.md"
)


def test_the_published_matrix_matches_the_generated_one() -> None:
    """Docs and code must not disagree about what is covered.

    A stale table in the architecture document is worse than no table: a
    reader trusts it and stops checking, which is the inaccurate-coverage
    claim this whole package is built to avoid.
    """

    published = _ARCHITECTURE_DOC.read_text(encoding="utf-8")
    assert published.strip(), "the architecture document is empty"
    for line in render_matrix().splitlines():
        if not line.startswith("|"):
            continue
        assert line in published, (
            f"the published coverage matrix is stale; missing row: {line}"
        )


def test_the_published_disposition_table_matches_the_generated_one() -> None:
    published = _ARCHITECTURE_DOC.read_text(encoding="utf-8")
    for line in render_dispositions().splitlines():
        if not line.startswith("| `"):
            continue
        assert line in published, (
            f"the published disposition table is stale; missing row: {line}"
        )
