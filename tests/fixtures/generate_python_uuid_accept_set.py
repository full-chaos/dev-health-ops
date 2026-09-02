"""Measure CPython `uuid.UUID`'s accept set.

A Go port that reaches for a general-purpose UUID parser gets a DIFFERENT set:
`github.com/google/uuid` dispatches on length and at 38 characters strips the
first and last character without checking they are braces, so arbitrary
surrounding characters parse there and raise here.

Regenerate:
    python tests/fixtures/generate_python_uuid_accept_set.py \
        --out internal/pythonparity/testdata/uuid_accept_set.json
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import uuid

B = "7b9583ee-4d24-2be7-4d09-34f815bebdd7"

CASES: list[str] = [
    B,
    B.upper(),
    B.replace("-", ""),
    B.replace("-", "").upper(),
    "00000000-0000-0000-0000-000000000000",
    "{" + B + "}",
    "{{" + B + "}}",
    "{{{" + B + "}}}",
    "{" + B,
    B + "}",
    "{" + B.replace("-", "") + "}",
    "urn:uuid:" + B,
    "URN:UUID:" + B,
    "Urn:Uuid:" + B,
    "urn:" + B,
    "uuid:" + B,
    "urn:urn:uuid:" + B,
    "urn:uuid:urn:uuid:" + B,
    "urn:uuid:{" + B + "}",
    "urn:uuid:" + B.replace("-", ""),
    " " + B + " ",
    "\t" + B,
    B + "\n",
    "X" + B + "X",
    "[" + B + "]",
    "!" + B + "?",
    B[:-1],
    B + "0",
    "not-a-uuid",
    "",
    "-" * 36,
    # HYPHEN PLACEMENT. Found by asking lane-4441's question of this corpus --
    # not "what did I vary?" but "WHAT WAS THE SAME IN EVERY CASE?" The answer
    # was hyphen position: every row until now had hyphens at the canonical
    # offsets or none at all, across only nine distinct hex payloads.
    #
    # CPython removes EVERY hyphen anywhere (`.replace('-','')`), so all of
    # these parse. A reimplementation that stripped hyphens only at the
    # canonical positions -- a plausible optimisation, since that is where they
    # are in every real UUID -- would have passed all thirty-five earlier rows.
    "7b-9583ee4d242be74d0934f815bebdd7",
    "-7b9583ee4d242be74d0934f815bebdd7",
    "7b9583ee4d242be74d0934f815bebdd7-",
    "7b9583ee-4d24-2be7-4d09-34f8-15bebdd7",
    "7-b-9-5-8-3-e-e-4-d-2-4-2-b-e-7-4-d-0-9-3-4-f-8-1-5-b-e-b-d-d-7",
    # Payload variety, the other constant: nine distinct payloads across
    # thirty-five rows, all sharing one base UUID's digits.
    "00000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
    # Contributed by lane-4441, predicted from the NORMALISATION rather than
    # from a description of the accept set. Both look like typos; both parse.
    #
    # THESE ARE COVERAGE, NOT REGRESSION ROWS -- do not prune them as
    # redundant because they have never failed. ParseUUID passes them because
    # it reproduces CPython's four steps literally, so they cost nothing today.
    # Their job is to fail a FUTURE reimplementation that optimises those steps
    # into a matcher, which is a different job from catching a present bug: the
    # optimisation is the plausible change, and these are the inputs it breaks.
    #
    #   `replace` is POSITION-INDEPENDENT, not anchored, so a trailing prefix
    #   is removed exactly like a leading one.
    B + "urn:",
    B + "uuid:",
    #   `strip('{}')` takes a CHARACTER SET, not a delimiter pair, so closing
    #   braces may LEAD and opening braces may TRAIL. Reimplementing from the
    #   accept set produces a balanced-brace check that rejects these;
    #   reimplementing the four steps gets them right for free.
    "}}" + B + "{{",
    "}" + B + "{",
]


# THE `int()` GRAMMAR. Asking "what was the same in every case?" of the rows
# above answers: every one of them was ASCII, and every one that survived the
# length gate was thirty-two HEX DIGITS. Both were assumptions, and both were
# wrong, because the last step of the normalisation is `int(hex, 16)` -- not a
# hex decode.
#
# `int()` folds Unicode decimal digits to ASCII and folds its own space set to
# U+0020, THEN applies a grammar with a sign, an `0x` prefix and underscore
# separators. Everything below fits in thirty-two characters, so everything
# below is reachable through `uuid.UUID`. A review round found the Go side
# refusing all of it.
#
# Two rows here exist only because the FOLD RUNS FIRST, which no description of
# the accept set would predict: `٠x…` parses (U+0660 becomes the `0` of an
# `0x` prefix) and `0b…` parses as plain hex (`b` is a digit, so `0b` is not a
# prefix at base 16).
_PAYLOAD = "7b9583ee4d242be74d0934f815bebdd7"
assert len(_PAYLOAD) == 32


def _int_grammar_cases() -> list[str]:
    """Thirty-two-CHARACTER values that exercise `int(s, 16)`'s own grammar."""
    cases = [
        # Unicode decimal digits fold to ASCII. Ninety-six bytes, thirty-two
        # characters -- the row that proves the gate counts characters.
        "１" * 32,
        "١" * 32,
        "１" * 16 + "1" * 16,
        "༡" * 32,
        # Fullwidth LETTERS are not Nd and do not fold.
        "ａ" * 32,
        # Surrounding whitespace, one row per shape of the space set.
        " " + _PAYLOAD[:30] + " ",
        "\t" + _PAYLOAD[:30] + "\n",
        "\v" + _PAYLOAD[:30] + "\f",
        "\r" + _PAYLOAD[:30] + "\x85",
        "\xa0" + _PAYLOAD[:30] + "　",
        " " + _PAYLOAD[:30] + " ",
        " " + _PAYLOAD[:30] + " ",
        " " + _PAYLOAD[:30] + " ",
        # NOT in int()'s space set, though `str.isspace()` says otherwise for
        # the first and a naive "it looks blank" reading says so for the second.
        "\x1c" + _PAYLOAD[:30] + "\x1c",
        "​" + _PAYLOAD[:30] + "​",
        # Interior whitespace never parses.
        _PAYLOAD[:15] + " " + _PAYLOAD[16:],
        # Sign. `+` parses; `-` parses as an int and then fails the range check.
        "+" + _PAYLOAD[:31],
        "-" + _PAYLOAD[:31],
        " +" + _PAYLOAD[:30],
        "+ " + _PAYLOAD[:30],
        # Base prefix.
        "0x" + _PAYLOAD[:30],
        "0X" + _PAYLOAD[:30],
        "0x" + "１" * 30,
        "0o" + _PAYLOAD[:30],
        "0b" + _PAYLOAD[:30],
        "٠x" + _PAYLOAD[:30],
        "0x0x" + _PAYLOAD[:28],
        # Underscore separators.
        "1_" * 15 + "11",
        "0x_" + _PAYLOAD[:29],
        "0x__" + _PAYLOAD[:28],
        "_" + _PAYLOAD[:31],
        _PAYLOAD[:31] + "_",
        "1__" + _PAYLOAD[:29],
        "+0x_" + _PAYLOAD[:28],
        # THE UNICODE-VERSION AXIS, pinned to the exact codepoints where it
        # bites. U+11DE0..U+11DE9 are Nd in Unicode 17.0.0 and do not exist in
        # 16.0.0, so Go's unicode package folds them and this interpreter does
        # not. A Go implementation that reached for `unicode.Is(unicode.Nd, r)`
        # instead of the frozen table would ACCEPT this row, which the reference
        # rejects -- the direction that writes rows for a doomed build.
        #
        # This row is the only thing standing between that mistake and a green
        # test run: the fuzz corpus samples from an alphabet that does not
        # contain these codepoints, so it cannot see this class.
        "\U00011de0" * 32,
        "\U00011de1" * 16 + "1" * 16,
    ]
    for case in cases:
        # The length gate is what makes these reachable at all. A row that is
        # not thirty-two characters tests the gate, not the grammar behind it.
        assert len(case) == 32, f"{case!r} is {len(case)} characters, want 32"
    return cases


CASES += _int_grammar_cases()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out")
    args = parser.parse_args()

    rows = []
    for case in CASES:
        try:
            rows.append(
                {"input": case, "verdict": "ACCEPT", "uuid": str(uuid.UUID(case))}
            )
        except Exception as error:  # noqa: BLE001 - the verdict IS the exception
            rows.append(
                {"input": case, "verdict": "REJECT", "error": type(error).__name__}
            )

    payload = (
        json.dumps(
            {
                "schema": "python_uuid_accept_set.v1",
                "measured_on": platform.python_version(),
                "cases": rows,
            },
            indent=1,
            sort_keys=True,
        )
        + "\n"
    )
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"written {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
