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
