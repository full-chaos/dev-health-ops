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
from .legs import LEG_B_NATIVE_LABEL, LegId, reading_rule
from .unsound import UNSOUND_DIMENSIONS, unsound_for

__all__ = ["confound_section", "render_report"]

#: Families where an intent-recognition miss is a RESULT rather than a
#: confound, because resolving the reference is the capability under trial.
#:
#: The distinction is the ADR's, not a presentational choice: on the
#: ambiguity families, "the thing we used to call Northstar" is precisely
#: what the graph's alias and previous-name capability exists to resolve, so
#: the native arm failing there is a measured product difference. On the
#: other families an unrecognised intent is a pure recognition gap that a
#: better classifier could close with no graph involved, and reading it as
#: evidence for a graph would be adopting a dependency to fix a parser.
_AMBIGUITY_FAMILIES = frozenset(
    {
        "ambiguous_identity",
        "colloquial_follow_up",
        "clarification_and_no_match",
    }
)


def confound_section(payload: dict[str, Any]) -> list[str]:
    """The interpretation confound, per family and in both directions.

    Rendered from the per-case interpretation dispositions in the records
    rather than from a remembered figure, so the prose cannot outlive the
    measurement it describes.
    """

    below: dict[str, int] = defaultdict(int)
    total: dict[str, int] = defaultdict(int)
    unmapped: dict[str, int] = defaultdict(int)
    for case in payload["cases"]:
        if case.get("leg") != LegId.AS_DEPLOYED.value:
            continue
        family = case["question_family"]
        total[family] += 1
        for arm in case["arms"]:
            disposition = arm.get("interpretation")
            if not disposition:
                continue
            if disposition.get("below_fallback_floor"):
                below[family] += 1
            if disposition.get("derived_question_family") is None:
                unmapped[family] += 1

    lines = [
        "## The question-interpretation confound",
        "",
        "Stated in both directions, because either alone is misleading.",
        "",
        "* It is **not a starved baseline.** Production wires no classifier "
        "either, so the native arm in this trial behaves exactly as the "
        "deployed one does.",
        "* It **is a hard limit** on what the native arm can do with most "
        "corpus questions, and any per-family reading of the native column "
        "has to be read against this table rather than as a "
        "graph-versus-native difference alone.",
        "",
        "**The counterfactual, named honestly.** The constrained-model "
        "fallback seam EXISTS in production code and is deliberately unwired "
        "(`production_runtime.py:2468`). Leg A therefore measures "
        "graph-versus-native **as deployed**, and native-with-classifier is "
        "UNMEASURED NATIVE HEADROOM in that leg. Leg B measures an upper "
        "bound on that headroom by supplying the classification perfectly. "
        "The classifier was NOT wired for this trial: doing so would break "
        "deployed parity and re-import the model-tier substitution the "
        "correction plan bans.",
        "",
        "| family | cases (Leg A) | below fallback floor | no native family "
        "-> unprojectable | miss is |",
        "|---|---|---|---|---|",
    ]
    for family in sorted(total):
        kind = (
            "a RESULT (reference resolution is the capability under trial)"
            if family in _AMBIGUITY_FAMILIES
            else "a CONFOUND (a recognition gap a classifier could close "
            "without any graph)"
        )
        lines.append(
            f"| `{family}` | {total[family]} | {below.get(family, 0)} | "
            f"{unmapped.get(family, 0)} | {kind} |"
        )
    lines.append("")
    return lines


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
            # A computable verdict is not a publishable one. When the
            # dimension's INPUTS are known-defective the cell renders NOT
            # MEASURED, because a value in a MUST_BE_ZERO cell invites being
            # quoted and a provisional tag does not travel with a quotation.
            if unsound_for(outcome["dimension_id"], arm_id) is not None:
                matrix[(family, outcome["dimension_id"])][_NOT_MEASURED] += 1
                continue
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

    lines.append("## How to read the two legs")
    lines.append("")
    lines.append(reading_rule())
    lines.append("")

    lines.extend(confound_section(payload))

    lines.append("## Per question family x per evaluation dimension")
    lines.append("")
    lines.append(
        "One table per arm, per leg. There is deliberately no combined table "
        "and no total: a single figure would hide an arm that improves "
        "ambiguity while harming driver precision, which is the specific "
        "outcome the correction addendum requires to stay visible. The legs "
        "are never aggregated together."
    )
    lines.append("")
    lines.append(f"Legend: {_LEGEND}")
    lines.append("")

    legs_present = [
        leg
        for leg in (LegId.AS_DEPLOYED.value, LegId.JOB_HELD_CONSTANT.value)
        if any(case.get("leg") == leg for case in payload["cases"])
    ]
    for leg in legs_present:
        leg_payload = {
            **payload,
            "cases": [c for c in payload["cases"] if c.get("leg") == leg],
        }
        lines.append(f"### Leg `{leg}`")
        lines.append("")
        if leg == LegId.JOB_HELD_CONSTANT.value:
            lines.append(
                f"> Every native figure in this leg is **{LEG_B_NATIVE_LABEL}**. "
                "The native arm here receives a question-family classification "
                "it cannot derive, so these are an upper bound on what a "
                "perfect classifier could deliver -- not a forecast, and not "
                "the product's behaviour."
            )
            lines.append("")
        for arm_id in arms:
            matrix, families, dimensions = _family_dimension_matrix(leg_payload, arm_id)
            if not families:
                continue
            label = (
                f" — {LEG_B_NATIVE_LABEL}"
                if leg == LegId.JOB_HELD_CONSTANT.value and arm_id == "native"
                else ""
            )
            lines.append(f"#### Arm `{arm_id}`{label}")
            lines.append("")
            lines.append("| family | " + " | ".join(dimensions) + " |")
            lines.append("|---" * (len(dimensions) + 1) + "|")
            for family in families:
                cells = [_cell(matrix.get((family, d), {})) for d in dimensions]
                lines.append(f"| `{family}` | " + " | ".join(cells) + " |")
            lines.append("")

    lines.extend(_unsound_section())
    lines.extend(_safety_column_callout(payload))

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


def _safety_column_callout(payload: dict[str, Any]) -> list[str]:
    """Why Leg A's native safety column must not be read as clean.

    All nine adversarial-safety cases go unmeasured for the native arm in
    Leg A -- the interpreter does not recognise them, so the arm reports them
    unprojectable and never emits a packet to be scored. An unmeasured safety
    column and a clean safety column look identical in a sparse matrix, and
    the difference is the whole point of the safety family.
    """

    unmeasured: dict[str, list[str]] = defaultdict(list)
    measured: dict[str, list[str]] = defaultdict(list)
    for case in payload["cases"]:
        if case.get("corpus_family") != "adversarial_safety":
            continue
        leg = case.get("leg", "")
        for arm in case["arms"]:
            if arm["arm_id"] != "native":
                continue
            bucket = (
                measured
                if arm["disposition"] == CaseDisposition.SCORED.value
                else unmeasured
            )
            bucket[leg].append(case["case_id"])

    lines = ["## Safety column: unmeasured is not clean", ""]
    if not unmeasured and not measured:
        lines.append("No adversarial-safety cases in this record set.")
        lines.append("")
        return lines
    lines.append(
        "An unmeasured safety cell and a passing safety cell look alike in a "
        "sparse matrix. They are not alike, and this is where that "
        "distinction is spelled out for the native arm."
    )
    lines.append("")
    lines.append("| leg | native adversarial-safety scored | unmeasured |")
    lines.append("|---|---|---|")
    for leg in sorted(set(unmeasured) | set(measured)):
        lines.append(
            f"| `{leg}` | {len(measured.get(leg, []))} | "
            f"{len(unmeasured.get(leg, []))} |"
        )
    lines.append("")
    for leg, case_ids in sorted(unmeasured.items()):
        lines.append(
            f"Unmeasured for native in `{leg}`: "
            + ", ".join(f"`{cid}`" for cid in sorted(case_ids))
            + ". These produced no packet, so nothing about their safety "
            "behaviour was observed. **Do not read this leg's native safety "
            "column as clean.**"
        )
        lines.append("")
    return lines


def _unsound_section() -> list[str]:
    """Dimensions suppressed because their inputs are known-defective.

    Rendered even when empty, and the empty case says so explicitly. A
    section that vanished when the registry emptied would leave a reader
    unable to tell "nothing is suppressed" from "this report predates the
    idea of suppressing anything".
    """

    lines = ["## Dimensions NOT MEASURED because their inputs are defective", ""]
    if not UNSOUND_DIMENSIONS:
        lines.append(
            "None. Every dimension's inputs are sound, so every cell above "
            "renders the verdict the oracles actually returned."
        )
        lines.append("")
        return lines
    lines.append(
        "These cells render as NOT MEASURED rather than as a verdict. The "
        "oracle would produce one, but it would be a function of a known "
        "defect rather than of the arm's behaviour -- and a value in a "
        "MUST_BE_ZERO cell invites being quoted, which a provisional tag "
        "cannot prevent once the number travels."
    )
    lines.append("")
    lines.append("| dimension | arm(s) | owner | why the verdict would be unsound |")
    lines.append("|---|---|---|---|")
    for entry in UNSOUND_DIMENSIONS:
        arms = ", ".join(f"`{arm}`" for arm in sorted(entry.arm_ids))
        lines.append(
            f"| `{entry.dimension_id.value}` | {arms} | **{entry.owner}** | "
            f"{entry.reason} |"
        )
    lines.append("")
    return lines
