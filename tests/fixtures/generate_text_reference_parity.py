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

# Declared in the form the live-python corpus guard recognises, so this corpus
# is GUARDED rather than merely committed. The guard's ratchet counts
# generators whose output it cannot find; an undeclared path would have made
# this one of them, and the count may only go down.
OUTPUT_PATH = Path(__file__).with_name("text_reference_parity.json")

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

# The whitespace axis, which exists because `\s` is the easiest class to read as
# formatting and the one that changes an edge's SEMANTIC TYPE when it diverges:
# 'closes<WS>#42' is ref_type='closes' if <WS> matches \s and 'references' if it
# does not, because the closing pattern fails and the plain pattern still hits.
#
# Two groups, and the SECOND is the one that tests the rewrite rather than the
# naive port. Measured on CPython 3.14.7 / Go 1.24:
#
#     Python re \s  ==  str.isspace()            29 runes  (identical sets)
#     Go RE2 \s                                   5 runes  ([\t\n\f\r ])
#     Go unicode.IsSpace                         25 runes
#     Python \s  MINUS  Go unicode.IsSpace       {U+001C, U+001D, U+001E, U+001F}
#     Go unicode.IsSpace  MINUS  Python \s       {} (empty)
#
# So Python's \s is Go's unicode.IsSpace plus exactly the four information
# separators, and the correct substitution is
#
#     unicode.IsSpace(r) || (r >= 0x1C && r <= 0x1F)
#
# Group 1 (unicode_ws) catches a port that left RE2's \s in place. Group 2
# (isspace_gap) is the only group that can tell the CORRECT substitution from
# `unicode.IsSpace` alone -- every rune in group 1 is already handled by plain
# IsSpace, so a corpus containing only group 1 would pass a rewrite that is
# wrong on four runes. U+200B is the negative control: it looks like a space,
# reads like one in a bug report, and is \s on NEITHER plane.
WHITESPACE_VARIANTS = [
    ("ascii_space", " ", "unicode_ws"),
    ("tab", "\t", "unicode_ws"),
    ("nbsp", " ", "unicode_ws"),
    ("nel", "", "unicode_ws"),
    ("line_separator", " ", "unicode_ws"),
    ("paragraph_separator", " ", "unicode_ws"),
    ("ideographic_space", "　", "unicode_ws"),
    ("en_quad", " ", "unicode_ws"),
    ("narrow_nbsp", " ", "unicode_ws"),
    ("file_separator", "", "isspace_gap"),
    ("group_separator", "", "isspace_gap"),
    ("record_separator", "", "isspace_gap"),
    ("unit_separator", "", "isspace_gap"),
    ("zero_width_space", "​", "negative_control"),
    ("word_joiner", "⁠", "negative_control"),
]

# The shapes whose meaning depends on \s. Each has a separator position where a
# whitespace variant decides which pattern wins, and therefore the ref_type.
WHITESPACE_SHAPES = [
    ("github_closing", "closes{}#42"),
    ("github_closing_fixes", "fixes{}#42"),
    ("github_merge_pr", "merge{}pull{}request{}#42"),
    ("github_squash_trailing", "fix the thing (#42){}"),
    ("revert_body", "{}this reverts commit abc"),
]

# The `$` axis, built to ATTACK a specific equivalence rather than to sample.
#
# GITHUB_SQUASH_PR_PATTERN ends `\)\s*$`. Python's `$` (no MULTILINE) matches at
# end of string OR just before a final newline; Go RE2's `$` (no `(?m)`) matches
# only at end of text. The Go port relies on the greedy Unicode `\s*` absorbing
# the difference, since newlines are in the class -- an argument, not a proof.
#
# These rows are the proof or the refutation. If the argument is right every row
# agrees across planes; if it is wrong, a trailing-newline row diverges and says
# so. The `\r\n` and lone-`\r` rows are here because `\r` is `\s` on both planes
# but is NOT what Python's `$` special-cases -- only `\n` is -- so they separate
# "absorbed by \s*" from "special-cased by $".
DOLLAR_ANCHOR = [
    ("bare", "fix the thing (#42)"),
    ("trailing_lf", "fix the thing (#42)\n"),
    ("trailing_lf_lf", "fix the thing (#42)\n\n"),
    ("trailing_space_lf", "fix the thing (#42) \n"),
    ("trailing_lf_space", "fix the thing (#42)\n "),
    ("trailing_crlf", "fix the thing (#42)\r\n"),
    ("trailing_cr", "fix the thing (#42)\r"),
    ("trailing_tab", "fix the thing (#42)\t"),
    ("trailing_nbsp", "fix the thing (#42)\u00a0"),
    ("trailing_fs", "fix the thing (#42)\u001c"),
    ("body_after_lf", "fix the thing (#42)\nbody text here"),
    ("second_line_paren", "subject\nfix the thing (#42)"),
    ("paren_not_at_end", "fix the thing (#42) and more"),
]

# The SUBJECT-TRUNCATION axis.
#
# extract_squash_pr_refs does `text.lstrip().split("\n", 1)[0]` -- it strips
# leading whitespace from the WHOLE text FIRST, then takes the first line. The
# order matters: a message that begins with a newline has an empty first line,
# so a port that splits before stripping sees an empty subject and finds
# nothing, while Python strips the newline away and matches on what follows.
#
# `lstrip()` uses Python's str whitespace set, which is the same 29 runes as
# `\s`, so the leading-NBSP and leading-U+001C rows are here to catch a port
# that strips only ASCII space.
#
# This axis exists because the `$`-semantics axis above PASSED for the wrong
# reason: both planes truncate to the subject, so `$` never sees a trailing
# newline and the Python-vs-RE2 `$` difference is unreachable in this extractor.
# Discovering that the stated mechanism was wrong is what surfaced the real
# divergence, which is in the truncation itself rather than in the anchor.
SUBJECT_TRUNCATION = [
    ("leading_newline", chr(10) + "  fix the thing (#42)"),
    ("leading_two_newlines", chr(10) * 2 + "fix the thing (#42)"),
    ("leading_spaces", "   fix the thing (#42)"),
    ("leading_tab", chr(9) + "fix the thing (#42)"),
    ("leading_nbsp", chr(0xA0) + "fix the thing (#42)"),
    ("leading_fs", chr(0x1C) + "fix the thing (#42)"),
    ("leading_ideographic", chr(0x3000) + "fix the thing (#42)"),
    ("leading_newline_then_body", chr(10) + "fix (#42)" + chr(10) + "body"),
    ("leading_ws_then_newline", "  " + chr(10) + "fix the thing (#42)"),
    ("only_newlines", chr(10) * 3),
    ("leading_zwsp_not_ws", chr(0x200B) + "fix the thing (#42)"),
]

# The GitLab MR axis, built to attack the OTHER flagged equivalence.
#
# Python has one pattern: `(?:merge\s+request|see\s+merge\s+request)\b[^!\n]*!(\d+)`.
# The Go port splits it into a keyword match plus a separate `[^!\n]*!(\d+)` scan
# from the boundary. These shapes are the ones where a split could diverge from a
# single regex: an intervening `!` that the `[^!\n]*` must stop at, a newline
# that must also stop it, more than one MR in the text, and a keyword with no `!`
# at all after it.
GITLAB_MR = [
    ("plain", "see merge request group/proj!42"),
    ("no_see", "merge request group/proj!42"),
    ("intervening_bang", "see merge request wow! group/proj!42"),
    ("bang_immediately", "see merge request!42"),
    ("newline_before_bang", "see merge request group/proj\n!42"),
    ("two_mrs", "see merge request a!1 and merge request b!2"),
    ("keyword_no_bang", "see merge request group/proj"),
    ("keyword_twice_one_bang", "merge request merge request x!7"),
    ("uppercase", "SEE MERGE REQUEST group/proj!42"),
    ("nbsp_between", "see\u00a0merge\u00a0request group/proj!42"),
    ("fs_between", "see\u001cmerge\u001crequest group/proj!42"),
    ("bang_no_digits", "see merge request group/proj!"),
    ("digits_then_text", "see merge request group/proj!42abc"),
]

# The UCD-RESIDUE axis, added after codex round 1 found the class was
# OBSERVABLE rather than theoretical.
#
# U+11DE0 is Nd in Go's Unicode 17 tables and Cn -- unassigned -- in CPython's
# UCD 16. The package doc classified this residue as RARE-but-reachable and the
# all-runes guard asserted it was Cn-only. Both were true and neither was
# enough: no case in the corpus CONTAINED such a rune, because the neighbour and
# digit alphabets were built from scripts a human would think of.
#
# The direction inverts through the boundary logic, which is the part the
# rune-set framing hid. `python-only == 0` says Go's CLASS never misses a rune
# Python's class has. But a GO-ONLY rune widens Go's word class, so a boundary
# that holds for Python FAILS for Go -- and Go misses the whole match:
#
#     'see merge request<U+11DE0>!45'   Python [45]        Go nil
#     '#1<U+11DE0>'                     Python key '1'     Go key '1<U+11DE0>'
#
# So a residue rune can make Go emit LESS than Python (a dropped edge) or emit a
# DIFFERENT key. Neither is visible in a rune-set diff.
UCD_RESIDUE = [
    ("mr_boundary", "see merge request" + chr(0x11DE0) + "!45"),
    ("plain_ref_suffix", "#1" + chr(0x11DE0)),
    ("plain_ref_prefix", chr(0x11DE0) + "#1"),
    ("jira_suffix", "PROJ-1" + chr(0x11DE0)),
    ("jira_prefix", chr(0x11DE0) + "PROJ-1"),
    ("closing_ref", "closes #1" + chr(0x11DE0)),
    ("digit_body", "merge pull request #" + chr(0x11DE0)),
    ("digit_mixed", "merge pull request #1" + chr(0x11DE0) + "2"),
    ("gitlab_path", "grp" + chr(0x11DE0) + "/proj#7"),
    ("squash_suffix", "fix the thing (#42)" + chr(0x11DE0)),
]

# The MAGNITUDE axis. Python's int() is arbitrary precision; Go's int is 64-bit.
# The corpus's digit bodies were all two characters, so no case could reach the
# boundary at all.
#
# Three regimes, and they need different answers rather than one:
#   - fits int64 comfortably: must round-trip
#   - fits int64 but near the top: must round-trip (a too-conservative guard
#     refuses these, which is what round 1 found)
#   - exceeds int64: CANNOT round-trip, and is a declared divergence rather
#     than a bug -- Go has no value to return
MAGNITUDE = [
    ("small", "merge pull request #42"),
    ("seven_digits", "merge pull request #1234567"),
    ("eighteen_digits", "merge pull request #123456789012345678"),
    ("near_int64_max", "merge pull request #9223372036854775806"),
    ("int64_max", "merge pull request #9223372036854775807"),
    ("int64_max_plus_one", "merge pull request #9223372036854775808"),
    ("five_e18", "merge pull request #5000000000000000000"),
    ("twenty_three_digits", "merge pull request #99999999999999999999999"),
    ("leading_zeros", "merge pull request #0000000000000000000000042"),
    ("zero", "merge pull request #0"),
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

    # Axis 3b: whitespace variants at the separator position of every shape
    # whose ref_type depends on \s. The group label travels into the case id so
    # a failure says WHICH equivalence broke -- RE2-\s-left-in-place, or the
    # four-rune isspace gap, or a negative control that should never match.
    for shape_name, template in WHITESPACE_SHAPES:
        slots = template.count("{}")
        for ws_name, ws_char, group in WHITESPACE_VARIANTS:
            cases.append(
                _case(
                    f"whitespace/{shape_name}/{group}/{ws_name}",
                    "whitespace",
                    template.format(*([ws_char] * slots)),
                )
            )

    # Axis 3c: the `$` equivalence, and 3d: the GitLab MR split.
    for label, text in DOLLAR_ANCHOR:
        cases.append(_case(f"dollar/{label}", "dollar_anchor", text))
    for label, text in GITLAB_MR:
        cases.append(_case(f"gitlabmr/{label}", "gitlab_mr", text))
    for label, text in SUBJECT_TRUNCATION:
        cases.append(_case(f"subject/{label}", "subject_truncation", text))
    for label, text in UCD_RESIDUE:
        cases.append(_case(f"residue/{label}", "ucd_residue", text))
    for label, text in MAGNITUDE:
        cases.append(_case(f"magnitude/{label}", "magnitude", text))

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
        default=str(OUTPUT_PATH),
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
