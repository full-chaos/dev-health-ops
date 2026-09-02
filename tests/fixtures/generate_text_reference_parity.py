"""Freeze what the DEPLOYED text reference extractors actually do.

This corpus exists because the Go port of `work_graph/extractors/text_parser.py`
cannot be a transcription. Python's `re` and Go's RE2 are different languages,
and they disagree in two independent ways on patterns this module actually uses.

# THE TWO DISAGREEMENTS, MEASURED NOT ASSUMED

**1. RE2 has no lookaround.** Two of the module's patterns use it:

    text_parser.py:53   GITHUB_PLAIN_REF_PATTERN  (?<!\\w)#(\\d+)\\b
    text_parser.py:465  the flag-key pattern      (?<!B)key(?!B), built per key

Both are lookaround over a single character class, which is exactly "match, then
inspect the neighbouring rune". The Go side writes that as code. The equivalence
is a theorem, not a hope, and it is stated as one in the Go doc -- but a theorem
about a rewrite is worth exactly as much as the corpus that could falsify it,
which is why the position axis below is enumerated rather than sampled.

**2. Python's character classes are Unicode-aware; RE2's are ASCII-only.**
This is the bigger trap, and it is invisible in any ASCII corpus. `re.ASCII` is
set NOWHERE in this module (checked), so every `\\w`, `\\d`, `\\b` -- and `\\s`,
which is easy to forget because it looks like formatting rather than semantics --
matches by Unicode property in Python and by ASCII range in Go.

Measured against CPython 3.14.7, five distinct divergence classes:

    class                     input              Python            Go RE2
    ----------------------------------------------------------------------
    \\s vs NBSP U+00A0         'closes\\xa0#42'    matches           does not
    \\s vs U+2028, U+3000      same shape         matches           does not
    \\d vs Arabic-Indic        '#٣٤'            matches, int()=34  does not match
    \\d vs fullwidth/Devanagari same shape        matches           does not
    \\b after non-ASCII letter 'éPROJ-1'          NO match          WOULD match
    \\w inside a captured span 'éx/repo#7'        group='éx/repo'   group='x/repo'

The last row is the one that decides how this corpus asserts. Both planes
"match" that input; they disagree about the SPAN. An assertion on presence, or
on the count of matches, passes while the extracted repository slug is wrong.
So every expectation here is a full tuple -- start, end, and every group -- and
never a boolean.

The `\\b` row is the one that decides which direction to worry about. It is the
only divergence where GO matches and PYTHON does not; every other class has
Python matching a superset. A corpus built only from "things Python finds" would
never contain it, so the inputs below are constructed from the character classes
outward rather than from the reference shapes outward.

# WHY THIS IS GENERATED RATHER THAN WRITTEN

The reference is IMPORTED and CALLED, so the expectations are its output rather
than the author's transcription of its output. A mistyped expectation is
indistinguishable from a correct one, and it never goes stale loudly.

    python tests/fixtures/generate_text_reference_parity.py --stdout
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Run from a repo checkout or from the container image, without editing either.
for _candidate in ("/app/src", str(Path(__file__).resolve().parents[2] / "src")):
    if _candidate not in sys.path and Path(_candidate).is_dir():
        sys.path.insert(0, _candidate)

from dev_health_ops.work_graph.extractors import text_parser as tp  # noqa: E402

SCHEMA = "text_reference_parity.v1"

# --------------------------------------------------------------------------
# The character-class alphabet. Every input below is built from these, so the
# axes are explicit rather than implied by whatever examples came to mind.
#
# Each entry is (label, character). The label lands in the corpus so a failing
# case names the property it was chosen for, not just the codepoint.
# --------------------------------------------------------------------------
NEIGHBOURS = [
    ("ascii_letter", "x"),
    ("ascii_digit", "7"),
    ("underscore", "_"),
    ("ascii_punct", "."),
    ("ascii_space", " "),
    ("latin1_letter", "é"),  # e-acute: \w in Python, not in RE2
    ("cyrillic_letter", "д"),  # de
    ("cjk_letter", "中"),
    ("combining_mark", "é"),  # e + COMBINING ACUTE: \w in Python
    ("arabic_indic_digit", "٣"),  # \d in Python, not in RE2
    ("fullwidth_digit", "１"),  # \d in Python, not in RE2
    ("devanagari_digit", "१"),  # \d in Python, not in RE2
    ("emoji", "\U0001f600"),  # not \w on either plane
    ("nbsp", " "),  # \s in Python, not in RE2
    ("line_separator", " "),  # \s in Python, not in RE2
    ("ideographic_space", "　"),  # \s in Python, not in RE2
    ("empty", ""),  # the string-start case
]

# The reference shapes the module recognises. `{}` marks where a neighbour is
# spliced in on the left; the position axis walks it through each shape.
SHAPES = [
    ("github_plain", "{}#42"),
    ("github_closing", "{}closes #42"),
    ("github_closing_nbsp", "{}closes #42"),
    ("github_merge_pr", "{}merge pull request #42"),
    ("github_squash", "{}fix the thing (#42)"),
    ("gitlab_cross_project", "{}group/proj#42"),
    ("gitlab_merge_mr", "{}see merge request group/proj!42"),
    ("jira_key", "{}PROJ-1"),
    ("revert_subject", '{}revert "a change"'),
]

# Digits inside the reference itself, which is a different axis from the
# neighbour: Python's \d accepts these and int() converts them, so a non-ASCII
# digit does not merely fail to match -- it can yield a REAL PR NUMBER that Go
# never produces.
DIGIT_BODIES = [
    ("ascii", "42"),
    ("arabic_indic", "٣٤"),
    ("fullwidth", "１２"),
    ("devanagari", "१२"),
    ("mixed_ascii_arabic", "4٤"),
]

# Cardinality and overlap: a corpus of single-match strings cannot see an
# extractor that returns matches in the wrong order, drops duplicates it should
# keep, or keeps ones it should drop.
MULTI = [
    ("two_plain", "#1 and #2"),
    ("three_plain", "#1 #2 #3"),
    ("duplicate_plain", "#7 and #7 again"),
    ("adjacent_no_space", "#1#2"),
    ("overlapping_closing", "closes #1 fixes #2"),
    ("plain_inside_closing", "closes #1"),
    ("jira_twice", "PROJ-1 and PROJ-2"),
    ("jira_and_plain", "PROJ-1 fixes #9"),
    ("nested_parens", "((#5))"),
    ("url_fragment", "https://example.test/x#42"),
    ("markdown_heading", "# 42"),
    ("empty", ""),
    ("whitespace_only", "   "),
]

# The module-level compiled patterns, recorded per input so a failure names the
# pattern that diverged rather than only the extractor that surfaced it.
PATTERNS = {
    name: value for name, value in vars(tp).items() if isinstance(value, re.Pattern)
}

EXTRACTORS = {
    "extract_pr_refs": tp.extract_pr_refs,
    "extract_squash_pr_refs": tp.extract_squash_pr_refs,
    "extract_jira_keys": tp.extract_jira_keys,
    "extract_github_issue_refs": tp.extract_github_issue_refs,
    "extract_gitlab_issue_refs": tp.extract_gitlab_issue_refs,
}


def _encode(value):
    """Serialise an extractor result without asserting its shape."""
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, (int, str)) or value is None:
        return value
    # ParsedIssueRef and friends are dataclasses; record every field so a change
    # to the dataclass surfaces here rather than being silently dropped.
    if hasattr(value, "__dataclass_fields__"):
        return {
            field: _encode(getattr(value, field))
            for field in value.__dataclass_fields__
        }
    return repr(value)


def _pattern_spans(text: str) -> dict:
    """Full match tuples per pattern: start, end, and every group.

    Presence is deliberately not recorded on its own. The `éx/repo#7` case
    matches on both planes and disagrees only about the span, so a boolean here
    would make that divergence invisible.
    """
    out = {}
    for name, pattern in sorted(PATTERNS.items()):
        matches = [
            {"start": m.start(), "end": m.end(), "groups": list(m.groups())}
            for m in pattern.finditer(text)
        ]
        if matches:
            out[name] = matches
    return out


def _case(case_id: str, axis: str, text: str) -> dict:
    return {
        "id": case_id,
        "axis": axis,
        "text": text,
        # Codepoints so a reviewer can see what the string IS without trusting
        # their terminal's rendering of combining marks and bidi characters.
        "codepoints": [f"U+{ord(ch):04X}" for ch in text],
        "extractors": {
            name: _encode(fn(text)) for name, fn in sorted(EXTRACTORS.items())
        },
        "patterns": _pattern_spans(text),
    }


def build_corpus() -> dict:
    cases: list[dict] = []

    # Axis 1: position — every neighbour class immediately left of every shape.
    # This is the axis that falsifies the lookaround rewrite, and it is a full
    # cross product rather than a sample because lookbehind is definitionally
    # about the preceding rune.
    for shape_name, template in SHAPES:
        for neighbour_name, neighbour in NEIGHBOURS:
            cases.append(
                _case(
                    f"position/{shape_name}/{neighbour_name}",
                    "position",
                    template.format(neighbour),
                )
            )

    # Axis 2: the digits inside the reference, not beside it.
    for shape_name, template in SHAPES:
        if "42" not in template:
            continue
        for digit_name, digits in DIGIT_BODIES:
            cases.append(
                _case(
                    f"digits/{shape_name}/{digit_name}",
                    "digit_class",
                    template.format("").replace("42", digits),
                )
            )

    # Axis 3: cardinality and overlap.
    for label, text in MULTI:
        cases.append(_case(f"cardinality/{label}", "cardinality", text))

    # Axis 4: the reference at the very end of the string, and followed by each
    # neighbour class. `\b` and `(?<!\w)` are asymmetric -- a corpus that only
    # varies the LEFT neighbour cannot see a right-hand boundary divergence.
    for shape_name, template in SHAPES:
        base = template.format("")
        for neighbour_name, neighbour in NEIGHBOURS:
            cases.append(
                _case(
                    f"trailing/{shape_name}/{neighbour_name}",
                    "trailing_position",
                    base + neighbour,
                )
            )

    return {
        "schema": SCHEMA,
        "generator": "tests/fixtures/generate_text_reference_parity.py",
        "python_version": sys.version.split()[0],
        # Recorded because Python's \w, \d, \b and \s are Unicode-property based,
        # so the UCD version is part of the oracle. A Python upgrade that moves a
        # codepoint between categories changes these expectations, and the rot
        # guard in the Go test is what makes that loud instead of silent.
        "unicodedata_version": __import__("unicodedata").unidata_version,
        "re_ascii_flag_set_in_module": any(
            p.flags & re.ASCII for p in PATTERNS.values()
        ),
        "patterns": {
            name: {"pattern": p.pattern, "flags": int(p.flags)}
            for name, p in sorted(PATTERNS.items())
        },
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument(
        "--out",
        default=str(Path(__file__).with_name("text_reference_parity.json")),
    )
    args = parser.parse_args()

    corpus = build_corpus()
    text = json.dumps(corpus, indent=2, ensure_ascii=True, sort_keys=False) + "\n"

    if args.stdout:
        sys.stdout.write(text)
    else:
        Path(args.out).write_text(text, encoding="utf-8")
        counts: dict[str, int] = {}
        for case in corpus["cases"]:
            counts[case["axis"]] = counts.get(case["axis"], 0) + 1
        print(f"wrote {args.out}")
        print(f"  cases: {len(corpus['cases'])}")
        for axis, n in sorted(counts.items()):
            print(f"    {axis}: {n}")
        print(f"  UCD: {corpus['unicodedata_version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
