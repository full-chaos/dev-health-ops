"""The coverage matrix: question family × evaluation dimension.

The corrected trial reports per question family and per evaluation dimension,
and never as one number. This module builds that grid — machine-readable via
:func:`coverage_matrix`, rendered via :func:`render_matrix` — from the case
registry and the frozen scoring registry, so it cannot drift from either.

**Unmapped cells are explicit.** A family/dimension pair that no authored case
exercises renders as ``UNMAPPED``, not as a blank. A blank cell in a coverage
table reads as "fine"; that is the whole failure mode this module exists to
prevent, and it is the cell-level version of the rule the case registry
applies to skipped rows.

**A skipped case never fills a cell.** :data:`CellStatus.SKIPPED_ONLY` exists
for the pair whose only would-be case carries a non-authored disposition. The
issue's rule is that no skipped case counts as a failure — a statement about
blame. It is not a licence for an unmeasured thing to read as measured, so a
skipped-only cell is visibly distinct from a covered one and from an unmapped
one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from ..investigation_contract.question_families import (
    ALL_QUESTION_FAMILY_IDS,
    QUESTION_FAMILY_REGISTRY,
    QuestionFamilyID,
)
from ..investigation_contract.scoring import (
    ALL_SCORING_DIMENSION_IDS,
    SCORING_DIMENSION_REGISTRY,
    ScoringDimensionID,
)
from .cases import CASE_REGISTRY, CaseDisposition, CorpusCase

__all__ = [
    "CellStatus",
    "CoverageCell",
    "coverage_matrix",
    "dispositions_table",
    "render_dispositions",
    "render_matrix",
    "validate_coverage",
]


class CellStatus(StrEnum):
    #: At least one authored case scores this dimension for this family.
    COVERED = "covered"
    #: The only cases that would have scored it are skipped, with a reason.
    SKIPPED_ONLY = "skipped_only"
    #: No case, authored or skipped, scores it here.
    UNMAPPED = "unmapped"


@dataclass(frozen=True)
class CoverageCell:
    family_id: QuestionFamilyID
    dimension_id: ScoringDimensionID
    status: CellStatus
    authored_case_ids: tuple[str, ...]
    skipped_case_ids: tuple[str, ...]

    @property
    def symbol(self) -> str:
        return {
            CellStatus.COVERED: str(len(self.authored_case_ids)),
            CellStatus.SKIPPED_ONLY: "skip",
            CellStatus.UNMAPPED: "--",
        }[self.status]


def _cases_for(family_id: QuestionFamilyID) -> Sequence[CorpusCase]:
    return [
        case for case in CASE_REGISTRY.values() if case.question_family is family_id
    ]


def coverage_matrix() -> Mapping[
    tuple[QuestionFamilyID, ScoringDimensionID], CoverageCell
]:
    """The full grid. Every family × dimension pair is present, always."""

    matrix: dict[tuple[QuestionFamilyID, ScoringDimensionID], CoverageCell] = {}
    for family_id in ALL_QUESTION_FAMILY_IDS:
        family_cases = _cases_for(family_id)
        for dimension_id in ALL_SCORING_DIMENSION_IDS:
            authored = tuple(
                case.case_id
                for case in family_cases
                if dimension_id in case.scoring_dimension_ids
                and case.disposition is CaseDisposition.AUTHORED
            )
            skipped = tuple(
                case.case_id
                for case in family_cases
                if dimension_id in case.scoring_dimension_ids
                and case.disposition is not CaseDisposition.AUTHORED
            )
            if authored:
                status = CellStatus.COVERED
            elif skipped:
                status = CellStatus.SKIPPED_ONLY
            else:
                status = CellStatus.UNMAPPED
            matrix[(family_id, dimension_id)] = CoverageCell(
                family_id=family_id,
                dimension_id=dimension_id,
                status=status,
                authored_case_ids=authored,
                skipped_case_ids=skipped,
            )
    return matrix


def dispositions_table() -> tuple[CorpusCase, ...]:
    """Every case that is not authored, in registry order."""

    return tuple(
        case
        for case in CASE_REGISTRY.values()
        if case.disposition is not CaseDisposition.AUTHORED
    )


def render_matrix() -> str:
    """The matrix as Markdown, for the architecture document."""

    matrix = coverage_matrix()
    families = list(ALL_QUESTION_FAMILY_IDS)
    header = (
        "| Evaluation dimension | "
        + " | ".join(family_id.value for family_id in families)
        + " |"
    )
    divider = "| --- | " + " | ".join("---" for _ in families) + " |"
    rows = [header, divider]
    for dimension_id in ALL_SCORING_DIMENSION_IDS:
        cells = [matrix[(family_id, dimension_id)].symbol for family_id in families]
        rows.append(f"| `{dimension_id.value}` | " + " | ".join(cells) + " |")
    legend = (
        "\nA number is the count of authored cases scoring that dimension for "
        "that family. `skip` means the only case that would have scored it "
        "carries a stated non-authored disposition. `--` means no case scores "
        "it there — an explicit gap, never a blank."
    )
    return "\n".join(rows) + "\n" + legend


def render_dispositions() -> str:
    """The explicit disposition table, for the architecture document."""

    rows = [
        "| Case | Disposition | Reason |",
        "| --- | --- | --- |",
    ]
    for case in dispositions_table():
        reason = case.disposition_reason.replace("\n", " ").strip()
        rows.append(f"| `{case.case_id}` | {case.disposition.value} | {reason} |")
    if len(rows) == 2:
        rows.append("| _(none)_ | | |")
    return "\n".join(rows)


def validate_coverage() -> None:
    """Raise unless the matrix is total and its gaps are the ones we accept.

    The totality check is the point: a matrix missing a cell is a matrix a
    reader cannot tell is missing a cell.
    """

    matrix = coverage_matrix()
    expected = {
        (family_id, dimension_id)
        for family_id in ALL_QUESTION_FAMILY_IDS
        for dimension_id in ALL_SCORING_DIMENSION_IDS
    }
    if set(matrix) != expected:
        missing = sorted(
            f"{family}/{dimension}" for family, dimension in expected - set(matrix)
        )
        raise RuntimeError(f"the coverage matrix is not total; missing={missing}")

    for family_id in ALL_QUESTION_FAMILY_IDS:
        family = QUESTION_FAMILY_REGISTRY[family_id]
        for dimension_id in family.scoring_dimension_ids:
            cell = matrix[(family_id, dimension_id)]
            if cell.status is CellStatus.UNMAPPED:
                raise RuntimeError(
                    f"the frozen registry says family {family_id} is scored on "
                    f"{dimension_id}, and no authored corpus case exercises it. "
                    "A declared dimension nothing measures is the unmeasured "
                    "coverage claim this matrix exists to expose."
                )

    for dimension_id in ALL_SCORING_DIMENSION_IDS:
        covered_anywhere = any(
            matrix[(family_id, dimension_id)].status is CellStatus.COVERED
            for family_id in ALL_QUESTION_FAMILY_IDS
        )
        if not covered_anywhere:
            raise RuntimeError(
                f"scoring dimension {dimension_id} is exercised by no authored "
                "case in any family; the trial would report an empty row that "
                "reads like a clean sheet"
            )
        if dimension_id not in SCORING_DIMENSION_REGISTRY:
            raise RuntimeError(f"unknown scoring dimension {dimension_id}")


validate_coverage()
