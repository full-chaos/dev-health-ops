"""Generate the ClickHouse String decode golden for CHAOS-4441.

WHAT WAS MEASURED, AND WHY THE FIRST ANSWER WAS WRONG
-----------------------------------------------------
`input_hash` is a SHA over text read out of ClickHouse `String` columns, which
are arbitrary bytes. Plan section 5d originally recorded three candidate decode
policies for invalid UTF-8 -- strict, replace, surrogateescape -- implemented
`replace`, and marked it UNVERIFIED.

It is **none of the three**. Measured against a real ClickHouse container,
reading rows whose bytes were verified server-side with `hex()`:

    stored bytes   clickhouse-connect      clickhouse-go (the Go plane)
    E4BFAE         '修'                     "修"            <- agree
    FF             'ff'                    "\\xff"
    80             '80'                    "\\x80"
    E4BF           'e4bf'                  "\\xe4\\xbf"
    EDA080         'eda080'                "\\xed\\xa0\\x80"
    EDB3BF         'edb3bf'                "\\xed\\xb3\\xbf"
    61FF62         '61ff62'                "a\\xffb"

The mechanism, confirmed in the library source rather than inferred from the
observation -- `clickhouse_connect/driver/buffer.py:135-138`:

    try:
        app(x.decode(encoding))
    except UnicodeDecodeError:
        app(x.hex())

So the policy is: **on any decode failure, the WHOLE value is replaced by the
lowercase hex of its raw bytes.** Not per byte -- `61FF62` becomes `'61ff62'`,
so the valid 'a' and 'b' are hexed too. The result is always pure ASCII.

CONSEQUENCES FOR THE PORT
-------------------------
1. The divergence lives in the READER, not in MarshalPythonJSON. By the time
   Python's encoder sees the value it is ordinary ASCII. Fixing this in the
   JSON encoder -- which is where section 5d first put it -- would have been
   the wrong layer entirely.
2. The Go driver hands back the raw invalid bytes, so `chquery` must apply the
   same substitution at scan time or every hash diverges for such a row.
3. Because the substituted value is pure ASCII, the U+FFFD path in
   MarshalPythonJSON is unreachable through the ClickHouse route.

THE THING THIS FILE ACTUALLY CHECKS
-----------------------------------
Given the mechanism, the Go port reduces to "is this valid UTF-8, and if not,
hex it". That is only correct if Go's `utf8.Valid` accepts EXACTLY the byte
sequences Python's `bytes.decode('utf-8')` accepts. Both claim strict UTF-8,
but "both are strict" is the kind of generalisation that has already been wrong
twice in this lane, so the acceptance sets are compared sequence by sequence
rather than assumed equal.

Usage:
    uv run python tests/fixtures/generate_clickhouse_string_decode_golden.py [--stdout]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

OUTPUT_PATH = Path(__file__).parent / "clickhouse_string_decode_python_golden.json"


def _driver_decode(raw: bytes) -> str:
    """The EXACT expression clickhouse_connect/driver/buffer.py:135-138 uses.

    Copied as an expression rather than imported because it is an inline try/
    except inside a read loop, not a callable. The container measurement above
    is what establishes that this expression is really what runs; this function
    is the same two lines, kept adjacent to the corpus that depends on them.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.hex()


def _byte_corpus() -> list[tuple[str, bytes]]:
    cases: list[tuple[str, bytes]] = []

    # Every single byte. 0x00-0x7f decode; 0x80-0xff are all invalid alone.
    for value in range(0x100):
        cases.append((f"single_{value:02x}", bytes([value])))

    # Two-byte sequences around the valid lead-byte range.
    for lead in (0xC0, 0xC1, 0xC2, 0xDF, 0xE0, 0xF0, 0xF4, 0xF5, 0xFF):
        for trail in (0x00, 0x7F, 0x80, 0xBF, 0xC0, 0xFF):
            cases.append((f"pair_{lead:02x}{trail:02x}", bytes([lead, trail])))

    named: list[tuple[str, bytes]] = [
        # Valid, one per length.
        ("valid_ascii", b"hello"),
        ("valid_2byte", "é".encode()),
        ("valid_3byte", "修".encode()),
        ("valid_4byte", "\U0001f600".encode()),
        ("valid_max", "\U0010ffff".encode()),
        ("valid_empty", b""),
        # Overlong encodings: representable in fewer bytes, so rejected.
        ("overlong_slash_2byte", bytes([0xC0, 0xAF])),
        ("overlong_slash_3byte", bytes([0xE0, 0x80, 0xAF])),
        ("overlong_nul_2byte", bytes([0xC0, 0x80])),
        # Surrogates encoded as UTF-8 (WTF-8): rejected by strict UTF-8.
        ("surrogate_d800", bytes([0xED, 0xA0, 0x80])),
        ("surrogate_dfff", bytes([0xED, 0xBF, 0xBF])),
        ("surrogate_dcff", bytes([0xED, 0xB3, 0xBF])),
        # Surrogate PAIR encoded as two WTF-8 runs (CESU-8).
        ("cesu8_pair", bytes([0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80])),
        # Above U+10FFFF.
        ("above_max_f4", bytes([0xF4, 0x90, 0x80, 0x80])),
        ("above_max_f5", bytes([0xF5, 0x80, 0x80, 0x80])),
        ("five_byte", bytes([0xF8, 0x88, 0x80, 0x80, 0x80])),
        # Truncated multi-byte sequences.
        ("truncated_2of3", bytes([0xE4, 0xBF])),
        ("truncated_1of3", bytes([0xE4])),
        ("truncated_3of4", bytes([0xF0, 0x9F, 0x98])),
        # Valid text with one bad byte in the middle, start and end -- these
        # pin that the WHOLE value is hexed, not just the offending byte.
        ("mixed_middle", b"a\xffb"),
        ("mixed_leading", b"\xffab"),
        ("mixed_trailing", b"ab\xff"),
        ("mixed_multibyte_context", "修".encode() + b"\xff" + "修".encode()),
        # A value that is ALREADY valid lowercase hex -- indistinguishable
        # afterwards from a substituted one, which is worth knowing.
        ("already_hex_text", b"61ff62"),
        # BOM and NUL, both valid.
        ("bom", b"\xef\xbb\xbf"),
        ("embedded_nul", b"a\x00b"),
        # Long invalid run, to check the substitution is not truncated.
        ("long_invalid", bytes([0xFF]) * 200),
    ]
    cases.extend(named)
    return cases


def main() -> None:
    cases: list[dict[str, Any]] = []
    for label, raw in _byte_corpus():
        decoded = _driver_decode(raw)
        try:
            raw.decode("utf-8")
            valid = True
        except UnicodeDecodeError:
            valid = False
        cases.append(
            {
                "label": label,
                "raw_hex": raw.hex(),
                "python_valid_utf8": valid,
                # Always pure ASCII when substituted; carried as hex anyway so
                # the fixture never has to represent an undecodable value.
                "decoded_hex": decoded.encode("utf-8", "surrogatepass").hex(),
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_clickhouse_string_decode_golden.py. "
            "Do not hand-edit."
        ),
        "_policy": (
            "clickhouse-connect (driver/buffer.py:135-138) decodes String columns as "
            "UTF-8 and, on UnicodeDecodeError, substitutes the LOWERCASE HEX of the "
            "whole raw value. Verified end to end against a real ClickHouse container: "
            "stored FF reads back as 'ff', stored 61FF62 as '61ff62'. The Go driver "
            "returns the raw bytes instead, so chquery must apply the same "
            "substitution at scan time."
        ),
        "clickhouse_connect_version_measured": "0.15.1",
        "cases": cases,
    }

    # allow_nan=False so a non-finite value fails loudly naming itself rather
    # than emitting the bare token `Infinity`, which is not valid JSON and which
    # Go's decoder rejects with "invalid character 'I'" -- silently making the
    # corpus unreadable. There are no floats here today; the guard is for the
    # day someone adds one.
    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    invalid = sum(1 for c in cases if not c["python_valid_utf8"])
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}  ({invalid} invalid, {len(cases) - invalid} valid)")


if __name__ == "__main__":
    main()
