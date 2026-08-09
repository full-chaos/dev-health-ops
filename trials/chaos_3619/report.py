"""Render the trial report FROM the raw records. Never from a live run.

Every number in the generated markdown is derived here from
``trial-results.records.json``. The renderer never touches an arm, a store or
the corpus, which is what makes the document checkable: a committed test
re-derives the report's load-bearing claims from the same artifact, so the
prose cannot drift away from the sweep it describes. That is the
``trials/chaos_3499`` precedent, and it exists because manual sweeps kept
"fixing" numbers that moved again the next run.

**No aggregate, including in the prose.** The correction addendum forbids a
single score, and the ban is not only about a field in a dataclass -- a
sentence saying "the graph arm won 31 of 39" is the same failure in a form
that is easier to quote. So this module has no total, and
:func:`render_report` emits per-family and per-dimension tables plus explicit
non-run accounting. Cell contents are counts of verdicts, never ratios,
because a ratio is one division away from being read as a score.

**A blank is never a pass.** Three things look alike in a sparse matrix and
must not: a dimension a case does not exercise (the corpus says so),
a dimension nothing measured (the arm never ran), and a dimension that
passed. They render as distinct tokens, and the legend is emitted with the
table rather than living in someone's head.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .binding import RunClass
from .dispositions import CaseDisposition

__all__ = ["render_report"]

#: What one matrix cell can say. Deliberately short and deliberately
#: distinct: a reader scanning a wide table separates these by shape.
_PASS = "P"
_FAIL = "F"
_NOT_APPLICABLE = "-"
_NOT_MEASURED = "x"
_EMPTY = "."

_LEGEND = (
    f"`{_PASS}n` n dimension verdicts PASS | "
    f"`{_FAIL}n` n FAIL | "
    f"`{_NOT_APPLICABLE}n` n NOT APPLICABLE (the case does not exercise it; "
    "the corpus says so, this is not a pass) | "
    f"`{_NOT_MEASURED}n` n cases where the arm produced no scored packet, so "
    "nothing was measured (NOT a zero) | "
    f"`{_EMPTY}` no case in this family declares this dimension at all"
)


def _cell(counts: dict[str, int]) -> str:
    """One matrix cell: every verdict kind that occurred, or an empty mark.

    Concatenated counts rather than a single winner, because a cell holding
    two passes and one failure is not a passing cell and a summary token
    would say it was.
    """

    parts = [
        f"{token}{counts[token]}"
        for token in (_PASS, _FAIL, _NOT_APPLICABLE, _NOT_MEASURED)
        if counts.get(token)
    ]
    return "".join(parts) if parts else _EMPTY


def _arm_ids(payload: dict[str, Any]) -> list[str]:
    seen: list[str] = []
    for case in payload["cases"]:
        for arm in case["arms"]:
            if arm["arm_id"] not in seen:
                seen.append(arm["arm_id"])
    return sorted(seen)


def _family_dimension_matrix(
    payload: dict[str, Any], arm_id: str
) -> tuple[dict[tuple[str, str], dict[str, int]], list[str], list[str]]:
    """Counts per (question family, dimension) for one arm."""

    matrix: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    families: list[str] = []
    dimensions: list[str] = []
    for case in payload["cases"]:
        family = case["question_family"]
        if family not in families:
            families.append(family)
        declared = case["declared_dimension_ids"]
        for dimension in declared:
            if dimension not in dimensions:
                dimensions.append(dimension)
        arm = next((a for a in case["arms"] if a["arm_id"] == arm_id), None)
        if arm is None:
            continue
        if arm["disposition"] != CaseDisposition.SCORED.value:
            # Every dimension the case DECLARES is unmeasured for this arm.
            # Counted against the declared set rather than skipped, so an
            # arm that never ran leaves a visible hole of the right size
            # instead of an absence indistinguishable from a narrow case.
            for dimension in declared:
                matrix[(family, dimension)][_NOT_MEASURED] += 1
            continue
        for outcome in arm["dimension_outcomes"]:
            token = {
                "pass": _PASS,
                "fail": _FAIL,
                "not_applicable": _NOT_APPLICABLE,
                "contract_invalid": _FAIL,
            }.get(outcome["verdict"], _FAIL)
            matrix[(family, outcome["dimension_id"])][token] += 1
    return matrix, sorted(families), sorted(dimensions)


def _disposition_table(payload: dict[str, Any], arms: list[str]) -> list[str]:
    counts: dict[str, dict[str, int]] = {arm: defaultdict(int) for arm in arms}
    for case in payload["cases"]:
        for arm in case["arms"]:
            counts[arm["arm_id"]][arm["disposition"]] += 1
    lines = ["| disposition | " + " | ".join(arms) + " |"]
    lines.append("|---" * (len(arms) + 1) + "|")
    for disposition in CaseDisposition:
        row = [counts[arm].get(disposition.value, 0) for arm in arms]
        if not any(row):
            continue
        lines.append(
            f"| `{disposition.value}` | " + " | ".join(str(n) for n in row) + " |"
        )
    return lines


def render_report(payload: dict[str, Any]) -> str:
    """The whole document, derived from one loaded record set."""

    binding = payload["binding"]
    arms = _arm_ids(payload)
    lines: list[str] = []

    lines.append("# CHAOS-3619 — graph-assisted Ask Dev product-value trial")
    lines.append("")
    lines.append(
        "Generated from `trial-results.records.json` by `trials/chaos_3619/"
        "report.py`. Do not edit: every figure below is re-derived from the "
        "raw records, and a committed test fails if this document and those "
        "records disagree."
    )
    lines.append("")

    if binding.get("run_class") != RunClass.MEASURED.value:
        lines.append(
            f"> **THIS IS NOT A TRIAL RESULT.** `run_class = "
            f"{binding.get('run_class')!r}`. This record set is a pipeline "
            "exercise. Its arm packets may carry known-defective output and "
            "no figure in it may be cited as a measurement of either arm."
        )
        lines.append("")

    lines.append("## What produced this")
    lines.append("")
    lines.append("| binding | value |")
    lines.append("|---|---|")
    for key in (
        "run_class",
        "commit",
        "feature_tip_commit",
        "tree_clean",
        "execution_mode",
        "corpus_version",
        "corpus_manifest_sha256",
        "contract_manifest_sha256",
        "packet_schema_version",
        "shadow_record_schema_version",
        "native_arm_id",
        "native_projection_version",
        "graph_arm_id",
        "graph_projection_version",
        "graph_query_version",
        "graph_embedder_model_id",
        "graph_attachment_encoding",
        "trial_store_backend",
        "per_case_timeout_seconds",
    ):
        lines.append(f"| `{key}` | `{binding.get(key)}` |")
    for name, version in sorted(binding.get("dependency_versions", {}).items()):
        lines.append(f"| `dependency:{name}` | `{version}` |")
    if not binding.get("tree_clean", False):
        lines.append("")
        lines.append(
            "> The working tree was **not clean** when this ran, so the "
            "commit above does not fully describe the code that produced "
            "these records. Treat the run as exploratory."
        )
    lines.append("")

    lines.append("## Case dispositions")
    lines.append("")
    lines.append(
        "What happened to each (case, arm) pair. `arm_declared_gap` is a "
        "RESULT, not a failure -- the native baseline reports several kinds "
        "of run as unprojectable by design, and how often it must is one of "
        "the numbers this comparison turns on."
    )
    lines.append("")
    lines.extend(_disposition_table(payload, arms))
    lines.append("")

    lines.append("## Per question family x per evaluation dimension")
    lines.append("")
    lines.append(
        "One table per arm. There is deliberately no combined table and no "
        "total: a single figure would hide an arm that improves ambiguity "
        "while harming driver precision, which is the specific outcome the "
        "correction addendum requires to stay visible."
    )
    lines.append("")
    lines.append(f"Legend: {_LEGEND}")
    lines.append("")

    for arm_id in arms:
        matrix, families, dimensions = _family_dimension_matrix(payload, arm_id)
        lines.append(f"### Arm `{arm_id}`")
        lines.append("")
        header = "| family | " + " | ".join(dimensions) + " |"
        lines.append(header)
        lines.append("|---" * (len(dimensions) + 1) + "|")
        for family in families:
            cells = [_cell(matrix.get((family, d), {})) for d in dimensions]
            lines.append(f"| `{family}` | " + " | ".join(cells) + " |")
        lines.append("")

    non_authored = payload.get("non_authored", ())
    lines.append("## Cases the corpus itself does not score")
    lines.append("")
    if non_authored:
        lines.append("| case | disposition | reason |")
        lines.append("|---|---|---|")
        for entry in non_authored:
            lines.append(
                f"| `{entry.get('case_id')}` | `{entry.get('disposition')}` | "
                f"{entry.get('disposition_reason')} |"
            )
    else:
        lines.append("None recorded.")
    lines.append("")
    lines.append(
        "Carried rather than dropped: once this artifact is the only "
        "evidence, a missing row and a deliberately unscored row look "
        "identical."
    )
    lines.append("")

    return "\n".join(lines) + "\n"
