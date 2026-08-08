#!/usr/bin/env python3
"""Fail on drift between the published Ask Dev answer copy and the server-owned constants.

``docs/use/ai-workflows/ask-dev-answers.md`` prints, as the product's own
words, three classes of string the runtime owns:

* the ``dev_answer.v2`` outcome **display labels**
  (``contracts_v2/answer.py::_OUTCOME_DISPLAY_LABELS``);
* the canonical no-answer **sentence** and **remediation** for every
  no-answer outcome (``contracts_v2/no_answer_policy.py``);
* the **withheld-content** sentence (``no_match_terminal.py::WITHHELD_COPY``).

Every one of those is a single server-owned string that a user reads
verbatim on screen. If the page and the runtime disagree, the page is a
confident, plausible lie about what the product says -- the worst failure
mode a documentation page has, because a reader who sees an exact quotation
stops checking. Reviewer vigilance does not scale to catching a one-word
edit on either side, so this is a guard.

Scope, stated so the exclusions are not silent:

* Only the columns that CLAIM to quote the product are compared. The
  outcome table's "What it means" column is authored explanation, is not
  presented as the product's words, and is deliberately not checked.
* The comparison is **verbatim**, including punctuation and apostrophes.
  A paraphrase in a checked column is a failure, not a near-miss.

Following ``check_investment_docs_drift.py``: constants are read out of the
source with ``ast`` rather than imported, so the guard runs in the docs
environment without importing the application or its dependencies.

Fails loudly rather than vacuously. A missing page, a missing marker pair, a
region that yields zero rows, or a constant that cannot be located in the
source is an ERROR -- never a silent pass. A check that measured nothing must
not report success.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
DEV = ROOT / "src" / "dev_health_ops" / "api" / "dev"

ANSWERS_DOC = DOCS / "use" / "ai-workflows" / "ask-dev-answers.md"
BASE_PATH = DEV / "contracts_v2" / "base.py"
ANSWER_PATH = DEV / "contracts_v2" / "answer.py"
NO_ANSWER_PATH = DEV / "contracts_v2" / "no_answer_policy.py"
NO_MATCH_PATH = DEV / "no_match_terminal.py"

OUTCOME_LABELS_REGION = "ASK-DEV OUTCOME LABELS"
REFUSAL_COPY_REGION = "ASK-DEV REFUSAL COPY"
NO_ANSWER_COPY_REGION = "ASK-DEV NO-ANSWER COPY"
WITHHELD_COPY_REGION = "ASK-DEV WITHHELD COPY"

_CODE_CELL_RE = re.compile(r"^`([a-z_]+)`$")
_SEPARATOR_ROW_RE = re.compile(r"^[\s|:-]+$")


class SourceLookupError(RuntimeError):
    """A constant this guard compares against is not where it expects it."""


# ---------------------------------------------------------------------------
# Source-side extraction (ast, never import)
# ---------------------------------------------------------------------------


def _assigned_value(path: Path, name: str) -> ast.expr:
    """The value node assigned to a module-level ``name``."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            ):
                return node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            if isinstance(node.target, ast.Name) and node.target.id == name:
                return node.value
    raise SourceLookupError(f"{name} not found at module level in {path}")


def _string(node: ast.expr, context: str) -> str:
    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
        raise SourceLookupError(f"{context} is not a string literal")
    return node.value


def _str_enum_values(path: Path, class_name: str) -> dict[str, str]:
    """``{MEMBER_NAME: "wire_value"}`` for a ``StrEnum`` class."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        members: dict[str, str] = {}
        for statement in node.body:
            if not isinstance(statement, ast.Assign):
                continue
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    members[target.id] = _string(
                        statement.value, f"{class_name}.{target.id}"
                    )
        if not members:
            raise SourceLookupError(f"{class_name} in {path} has no string members")
        return members
    raise SourceLookupError(f"class {class_name} not found in {path}")


def _str_to_str_mapping(path: Path, name: str) -> dict[str, str]:
    """A ``{"key": "value"}`` dict literal with string keys and values."""

    value = _assigned_value(path, name)
    if not isinstance(value, ast.Dict):
        raise SourceLookupError(f"{name} in {path} is not a dict literal")
    mapping: dict[str, str] = {}
    for key_node, value_node in zip(value.keys, value.values, strict=True):
        if key_node is None:
            raise SourceLookupError(f"{name} in {path} uses dict unpacking")
        key = _string(key_node, f"{name} key")
        mapping[key] = _string(value_node, f"{name}[{key!r}]")
    if not mapping:
        raise SourceLookupError(f"{name} in {path} is empty")
    return mapping


def _str_to_first_of_tuple(path: Path, name: str) -> dict[str, str]:
    """A ``{"key": ("value",)}`` dict literal, reduced to its first element."""

    value = _assigned_value(path, name)
    if not isinstance(value, ast.Dict):
        raise SourceLookupError(f"{name} in {path} is not a dict literal")
    mapping: dict[str, str] = {}
    for key_node, value_node in zip(value.keys, value.values, strict=True):
        if key_node is None:
            raise SourceLookupError(f"{name} in {path} uses dict unpacking")
        key = _string(key_node, f"{name} key")
        if not isinstance(value_node, ast.Tuple) or not value_node.elts:
            raise SourceLookupError(f"{name}[{key!r}] is not a non-empty tuple")
        mapping[key] = _string(value_node.elts[0], f"{name}[{key!r}][0]")
    if not mapping:
        raise SourceLookupError(f"{name} in {path} is empty")
    return mapping


def _enum_keyed_labels(path: Path, name: str, enum_name: str) -> dict[str, str]:
    """``{PublicOutcome.X: "Label"}`` resolved to ``{"wire_value": "Label"}``."""

    members = _str_enum_values(BASE_PATH, enum_name)
    value = _assigned_value(path, name)
    if not isinstance(value, ast.Dict):
        raise SourceLookupError(f"{name} in {path} is not a dict literal")
    mapping: dict[str, str] = {}
    for key_node, value_node in zip(value.keys, value.values, strict=True):
        if (
            not isinstance(key_node, ast.Attribute)
            or not isinstance(key_node.value, ast.Name)
            or key_node.value.id != enum_name
        ):
            raise SourceLookupError(f"{name} in {path} has a non-{enum_name} key")
        member = key_node.attr
        if member not in members:
            raise SourceLookupError(f"{enum_name}.{member} is not a member")
        mapping[members[member]] = _string(value_node, f"{name}[{member}]")
    if not mapping:
        raise SourceLookupError(f"{name} in {path} is empty")
    return mapping


# ---------------------------------------------------------------------------
# Doc-side extraction
# ---------------------------------------------------------------------------


def _region(document: str, name: str, source: str) -> str:
    begin = f"<!-- BEGIN {name} -->"
    end = f"<!-- END {name} -->"
    start = document.find(begin)
    stop = document.find(end)
    if start == -1 or stop == -1 or stop < start:
        raise SourceLookupError(f"{source}: marker pair for {name!r} is missing")
    return document[start + len(begin) : stop]


def _table_rows(block: str, name: str, source: str) -> list[list[str]]:
    """Body cells of a pipe table, header and separator dropped."""

    rows: list[list[str]] = []
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line.startswith("|"):
            continue
        if _SEPARATOR_ROW_RE.match(line):
            rows = []  # everything before the separator is the header
            continue
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    if not rows:
        raise SourceLookupError(f"{source}: region {name!r} contains no table rows")
    return rows


def _code_cell(cell: str, name: str, source: str) -> str:
    match = _CODE_CELL_RE.match(cell)
    if match is None:
        raise SourceLookupError(
            f"{source}: region {name!r} expected a `code` outcome cell, got {cell!r}"
        )
    return match.group(1)


def _quoted_cell(cell: str, name: str, source: str) -> str:
    """A cell that claims to quote the product: italicised, stripped verbatim."""

    if not (cell.startswith("*") and cell.endswith("*") and len(cell) > 2):
        raise SourceLookupError(
            f"{source}: region {name!r} expected an italicised product string, "
            f"got {cell!r}"
        )
    return cell[1:-1]


# ---------------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------------


def _compare(
    kind: str, outcome: str, published: str, canonical: str, errors: list[str]
) -> None:
    if published != canonical:
        errors.append(
            f"{ANSWERS_DOC.relative_to(ROOT)}: {kind} for {outcome!r} drifted -- "
            f"page says {published!r}, source says {canonical!r}"
        )


def check_outcome_labels(document: str, errors: list[str]) -> None:
    canonical = _enum_keyed_labels(
        ANSWER_PATH, "_OUTCOME_DISPLAY_LABELS", "PublicOutcome"
    )
    block = _region(document, OUTCOME_LABELS_REGION, "ask-dev-answers.md")
    published: dict[str, str] = {}
    for row in _table_rows(block, OUTCOME_LABELS_REGION, "ask-dev-answers.md"):
        if len(row) < 2:
            errors.append(f"outcome-label row has too few columns: {row!r}")
            continue
        outcome = _code_cell(row[0], OUTCOME_LABELS_REGION, "ask-dev-answers.md")
        published[outcome] = row[1]

    missing = sorted(set(canonical) - set(published))
    unknown = sorted(set(published) - set(canonical))
    if missing:
        errors.append(f"outcome table does not document these outcomes: {missing}")
    if unknown:
        errors.append(f"outcome table documents unknown outcomes: {unknown}")
    for outcome in sorted(set(canonical) & set(published)):
        _compare(
            "display label", outcome, published[outcome], canonical[outcome], errors
        )


def check_no_answer_copy(document: str, errors: list[str]) -> None:
    canonical_copy = _str_to_str_mapping(NO_ANSWER_PATH, "CANONICAL_NO_ANSWER_COPY")
    canonical_remediation = _str_to_first_of_tuple(
        NO_ANSWER_PATH, "CANONICAL_NO_ANSWER_REMEDIATION"
    )

    published_copy: dict[str, str] = {}
    published_remediation: dict[str, str] = {}

    # The refusal table carries a leading "You asked it to" column; the
    # no-answer table does not. Both quote copy then remediation.
    for name, outcome_col in (
        (REFUSAL_COPY_REGION, 1),
        (NO_ANSWER_COPY_REGION, 0),
    ):
        block = _region(document, name, "ask-dev-answers.md")
        for row in _table_rows(block, name, "ask-dev-answers.md"):
            if len(row) < outcome_col + 3:
                errors.append(f"{name} row has too few columns: {row!r}")
                continue
            outcome = _code_cell(row[outcome_col], name, "ask-dev-answers.md")
            if outcome in published_copy:
                errors.append(f"outcome {outcome!r} is quoted in more than one table")
            published_copy[outcome] = _quoted_cell(
                row[outcome_col + 1], name, "ask-dev-answers.md"
            )
            published_remediation[outcome] = _quoted_cell(
                row[outcome_col + 2], name, "ask-dev-answers.md"
            )

    missing = sorted(set(canonical_copy) - set(published_copy))
    unknown = sorted(set(published_copy) - set(canonical_copy))
    if missing:
        errors.append(f"no-answer copy is not published for these outcomes: {missing}")
    if unknown:
        errors.append(f"copy published for unknown outcomes: {unknown}")
    for outcome in sorted(set(canonical_copy) & set(published_copy)):
        _compare(
            "canonical copy",
            outcome,
            published_copy[outcome],
            canonical_copy[outcome],
            errors,
        )
        if outcome in canonical_remediation:
            _compare(
                "remediation",
                outcome,
                published_remediation[outcome],
                canonical_remediation[outcome],
                errors,
            )
        else:
            errors.append(f"no canonical remediation exists for outcome {outcome!r}")


def check_withheld_copy(document: str, errors: list[str]) -> None:
    canonical = _string(
        _assigned_value(NO_MATCH_PATH, "WITHHELD_COPY"), "WITHHELD_COPY"
    )
    block = _region(document, WITHHELD_COPY_REGION, "ask-dev-answers.md").strip()
    published = _quoted_cell(block, WITHHELD_COPY_REGION, "ask-dev-answers.md")
    _compare("withheld-content copy", "withheld", published, canonical, errors)


def main() -> int:
    if not ANSWERS_DOC.is_file():
        print(f"ERROR: missing page {ANSWERS_DOC.relative_to(ROOT)}")
        return 1

    document = ANSWERS_DOC.read_text(encoding="utf-8")
    errors: list[str] = []
    try:
        check_outcome_labels(document, errors)
        check_no_answer_copy(document, errors)
        check_withheld_copy(document, errors)
    except SourceLookupError as exc:
        # A guard that cannot find what it compares has NOT passed.
        print(f"ERROR: {exc}")
        return 1

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Ask Dev published-copy drift check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
