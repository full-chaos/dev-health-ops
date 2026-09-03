"""Emit CPython's exact whitespace predicate for the Go port (CHAOS-4441).

WHY A FIXTURE RATHER THAN A COMMENT
-----------------------------------
Go's `unicode.IsSpace` is a strict SUBSET of CPython's `str.isspace()`. The
difference is four code points -- 0x1c FILE SEPARATOR, 0x1d GROUP SEPARATOR,
0x1e RECORD SEPARATOR, 0x1f UNIT SEPARATOR -- and it has already caused one
defect in this lane: `RequireOrganizationScope("\\x1c")` accepted a scope Python
rejects, because `strings.TrimSpace` left it non-empty (codex round 2, PR2).

That fact currently lives in a code comment asserting it was measured once.
This file replaces the assertion with the measurement itself, regenerated on
demand and diffed by a rot guard, so a Python upgrade that adds a whitespace
code point is caught by CI rather than by a divergent hash in production.

PYTHON HAS THREE OVERLAPPING RULES HERE, NOT ONE
------------------------------------------------
  1. str.isspace() / .strip() / .split()  -- adds 0x1c-0x1f to Go's set
  2. int() / float()                      -- REJECTS 0x1c-0x1f; Go's
                                             strings.TrimSpace matches this one
  3. str.splitlines()                     -- 0x1c, 0x1d, 0x1e but NOT 0x1f,
                                             plus 0x0b, 0x0c, 0x85, U+2028/9

Rules 1 and 3 are not nested: 0x1f is whitespace but not a line boundary. A
port that shares a single predicate between them is wrong in one direction or
the other, which is why all three sets are emitted here rather than derived.

WHERE IT BITES BEYOND .strip()
------------------------------
`evidence._truncate_text` collapses whitespace with `" ".join(value.split())`.
`str.split()` with no argument splits on the SAME set as `str.isspace()` --
verified here rather than assumed -- so a Go port built on `strings.Fields`
leaves 0x1c-0x1f embedded in the text. That text is part of `source_texts`,
which is hashed into `input_hash`, which is the LLM skip-existing key. A
whitespace disagreement is therefore a cost defect, not a cosmetic one.

Usage:
    uv run python tests/fixtures/generate_python_whitespace_golden.py [--stdout]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

OUTPUT_PATH = Path(__file__).parent / "python_whitespace_python_golden.json"

# Go's unicode.IsSpace set, for recording the delta explicitly rather than
# leaving a reader to work it out. Sourced from the Go documentation for
# unicode.IsSpace: '\t', '\n', '\v', '\f', '\r', ' ', U+0085 NEL, U+00A0 NBSP,
# plus the Unicode Z category.
GO_UNICODE_ISSPACE = {
    0x09,
    0x0A,
    0x0B,
    0x0C,
    0x0D,
    0x20,
    0x85,
    0xA0,
    0x1680,
    0x2000,
    0x2001,
    0x2002,
    0x2003,
    0x2004,
    0x2005,
    0x2006,
    0x2007,
    0x2008,
    0x2009,
    0x200A,
    0x2028,
    0x2029,
    0x202F,
    0x205F,
    0x3000,
}


def _float_pads_without_changing_value(char: str) -> bool:
    """True when `char` may surround a numeral without altering float()'s result.

    float() is a SECOND numeric parser with its own space rule, and
    confidenceFromString mirrors it with strings.TrimSpace on the same reasoning
    as parsePythonInt. That reasoning was asserted and not measured, exactly as
    int()'s was, so it is derived here too.

    The exclusion list is wider than int()'s because float() accepts '.', 'e'
    and 'E' inside a numeral; including them would derive them as "padding".
    """
    try:
        return float(char + "5" + char) == 5.0
    except ValueError:
        return False


def _int_pads_without_changing_value(char: str) -> bool:
    """True when `char` may surround a numeral without altering int()'s result."""
    try:
        return int(char + "5" + char) == 5
    except ValueError:
        return False


def main() -> None:
    isspace = [cp for cp in range(0x110000) if chr(cp).isspace()]

    # str.split() with no argument is documented to split on whitespace, but
    # "whitespace" is not stated to be str.isspace()'s set. Drive it rather
    # than trust the wording: if any isspace code point failed to split, or any
    # non-isspace code point did split, the two predicates would need separate
    # ports.
    split_disagrees = [
        cp for cp in isspace if ("a" + chr(cp) + "b").split() != ["a", "b"]
    ]
    # The reverse direction, sampled across the planes rather than exhaustively
    # (a full sweep costs minutes and the interesting candidates are the
    # separators, the format characters and the Mongolian vowel separator,
    # which lost its space property in Unicode 6.3).
    reverse_candidates = [0x180E, 0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF, 0x00AD]
    split_extra = [
        cp
        for cp in reverse_candidates
        if cp not in set(isspace) and ("a" + chr(cp) + "b").split() == ["a", "b"]
    ]

    # str.splitlines() is a THIRD character class, distinct from both of the
    # above. Notably it treats 0x1c-0x1e as line boundaries but NOT 0x1f, while
    # str.isspace() accepts all four -- so the two sets are not nested and a
    # port cannot share one predicate between them.
    splitlines_boundaries = [
        cp for cp in range(0x110000) if len(("a" + chr(cp) + "b").splitlines()) > 1
    ]
    # CRLF must count as ONE boundary, not two, or a Go port emits a spurious
    # empty line between them.
    crlf_is_one_boundary = "a\r\nb".splitlines() == ["a", "b"]

    # Driven cases for the Go port: every boundary in isolation, the CRLF
    # pair, boundaries at the start/end, runs of them, and the shapes
    # _commit_subject actually meets. Expectations come from calling
    # splitlines(), never from describing it.
    splitline_inputs = [
        "",
        "a",
        "a\nb",
        "a\nb\nc",
        "a\n",
        "\na",
        "\n",
        "a\n\nb",
        "a\r\nb",
        "a\r\n",
        "a\n\rb",
        "a\r\rb",
        "first\x1csecond",
        "  leading\nreal subject\nbody",
        "\x1c  \nreal subject\nbody",
        "\t\n   \nsubject",
        "修復\u2028バグ",
        "a\x85b",
        "a\x0bb\x0cc",
        "a\x1cb\x1dc\x1ed",
        "a\x1fb",
    ] + [f"a{chr(cp)}b" for cp in splitlines_boundaries]
    splitline_cases = [
        {
            "input_hex": value.encode("utf-8", "surrogatepass").hex(),
            "lines_hex": [
                line.encode("utf-8", "surrogatepass").hex()
                for line in value.splitlines()
            ],
        }
        for value in splitline_inputs
    ]

    # THE THIRD RULE, now measured rather than only asserted.
    #
    # Comments in this port have long claimed that int()/float() REJECT
    # 0x1c-0x1f while str.strip() removes them -- the reason parsePythonInt uses
    # strings.TrimSpace and not pythonparity.Strip. That claim was never pinned
    # by a corpus, only stated. Derive it.
    #
    # A character belongs to int()'s space set if it can PAD a number without
    # changing its value. Digits and signs are excluded explicitly: `int(c+'5')`
    # succeeds for any digit c, so a probe that forgets that derives a 787-entry
    # "space set" that is mostly Nd (I made exactly that error first).
    int_space = [
        cp
        for cp in range(0x110000)
        if not chr(cp).isdecimal()
        and chr(cp) not in "+-"
        and _int_pads_without_changing_value(chr(cp))
    ]
    float_space = [
        cp
        for cp in range(0x110000)
        if not chr(cp).isdecimal()
        and chr(cp) not in "+-.eE"
        and _float_pads_without_changing_value(chr(cp))
    ]

    # Bound as locals before going into the payload dict: indexing a
    # dict[str, object] hands mypy an `object`, which cannot be passed to hex().
    python_only = sorted(set(isspace) - GO_UNICODE_ISSPACE)
    go_only = sorted(GO_UNICODE_ISSPACE - set(isspace))

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_python_whitespace_golden.py. "
            "Do not hand-edit."
        ),
        "isspace_code_points": isspace,
        "go_unicode_isspace_code_points": sorted(GO_UNICODE_ISSPACE),
        "python_only_code_points": python_only,
        "go_only_code_points": go_only,
        "split_disagrees_with_isspace": split_disagrees,
        "split_splits_on_non_isspace": split_extra,
        "splitlines_boundary_code_points": splitlines_boundaries,
        "splitlines_only_code_points": sorted(
            set(splitlines_boundaries) - set(isspace)
        ),
        "isspace_only_vs_splitlines_code_points": sorted(
            set(isspace) - set(splitlines_boundaries)
        ),
        "crlf_is_one_boundary": crlf_is_one_boundary,
        "splitlines_cases": splitline_cases,
        # int()'s space set, and its deltas against the other two rules.
        "int_space_code_points": int_space,
        "isspace_only_vs_int_space": sorted(set(isspace) - set(int_space)),
        "int_space_only_vs_isspace": sorted(set(int_space) - set(isspace)),
        "float_space_code_points": float_space,
        "float_space_differs_from_int_space": sorted(set(float_space) ^ set(int_space)),
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    print(f"wrote {OUTPUT_PATH}")
    print(f"  python isspace code points: {len(isspace)}")
    print(f"  python-only vs Go:          {[hex(c) for c in python_only]}")
    print(f"  go-only vs Python:          {[hex(c) for c in go_only]}")
    print(f"  split/isspace disagreements: {split_disagrees or 'none'}")
    print(f"  splitlines boundaries:      {len(splitlines_boundaries)}")
    print(f"  int() space set:            {len(int_space)}")
    print(f"  float() space set:          {len(float_space)}")
    print(
        "  int()/float() space delta:  "
        f"{sorted(set(float_space) ^ set(int_space)) or 'none — identical'}"
    )
    print(
        "  whitespace to str.strip() but NOT to int(): "
        f"{[hex(c) for c in sorted(set(isspace) - set(int_space))]}"
    )
    print(
        "  in isspace but NOT a line boundary: "
        f"{[hex(c) for c in sorted(set(isspace) - set(splitlines_boundaries))]}"
    )


if __name__ == "__main__":
    main()
