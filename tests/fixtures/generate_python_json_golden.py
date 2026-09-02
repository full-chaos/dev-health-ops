"""Generate the Python-JSON serialization golden for CHAOS-4441.

WHAT THIS PINS, AND WHY IT IS NOT A HAND-WRITTEN MATRIX
------------------------------------------------------
`evidence.build_text_bundle` derives `input_hash` as a SHA-256 over CPython's

    json.dumps(input_payload, sort_keys=True, default=str)

and that hash is the LLM skip-existing key (`materialize.py`:
`WHERE categorization_input_hash IN %(input_hashes)s`). Go's `encoding/json`
does not produce Python's bytes -- they diverge on separators, on every code
point from U+007F upward, on astral code points, and on invalid UTF-8. A
divergent hash matches no stored row, so every work unit re-categorizes on
every run: a real, repeating LLM bill with no error and no telemetry.

Per CHAOS-4803, this generator IMPORTS the reference rather than imitating it:
the `bundle_cases` drive `build_text_bundle` itself, and the `serializer_cases`
call the exact `json.dumps(..., sort_keys=True, default=str)` expression that
`evidence.py` uses. Nothing here re-implements the behaviour it is checking.

THE THREE AXES
--------------
A corpus is only as good as the axes it varies, and a missing axis is invisible
from inside the corpus. This one crosses:

  1. FIELD      -- which position the value sits in: the work_unit_id, an outer
                   source-type key, an inner source-id key, or a text value.
                   Escaping and sorting are applied at different layers, so a
                   value that is safe in one position is not proof for another.
  2. DOCUMENT   -- the shape around it: empty payload, empty inner maps, single
                   key, many keys, and keys whose sort order is contested.
  3. VALUE      -- the byte content itself: ASCII, the short escapes, every C0
                   control, DEL, non-ASCII BMP, astral, U+2028/9, and lone
                   surrogates.

Usage:
    uv run python tests/fixtures/generate_python_json_golden.py
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.evidence import build_text_bundle

OUTPUT_PATH = Path(__file__).parent / "python_json_python_golden.json"


def _hex(value: str) -> str:
    """Encode a str as hex of its UTF-8 bytes.

    `surrogatepass` is required because the corpus deliberately contains lone
    surrogates, which a plain `.encode("utf-8")` refuses. Go reads the hex back
    as raw bytes, so a lone surrogate arrives there as invalid UTF-8 -- which is
    exactly the situation being pinned, not an artefact of the transport.
    """
    return value.encode("utf-8", "surrogatepass").hex()


# --- Axis 3: value shapes -------------------------------------------------
# Each entry is (label, value, reachable_in_go). `reachable_in_go` is False for
# values a Go string cannot hold, where the two planes CANNOT agree by
# construction; those are recorded so the divergence is documented and pinned
# rather than discovered later.
VALUE_CORPUS: list[tuple[str, str, bool]] = [
    ("empty", "", True),
    ("plain_ascii", "fix the thing", True),
    ("slash", "a/b", True),  # NOT escaped by either plane
    ("quote", 'say "hi"', True),
    ("backslash", "back\\slash", True),
    ("html_chars", "Title & <b>body</b>", True),  # Go escapes these; Python does not
    ("newline", "line1\nline2", True),
    ("tab", "a\tb", True),
    ("carriage_return", "a\rb", True),
    ("backspace", "a\bb", True),
    ("formfeed", "a\fb", True),
    ("vertical_tab", "a\vb", True),  # no short form:
    ("nul", "a\x00b", True),
    ("bell", "a\x07b", True),
    ("escape_char", "a\x1bb", True),
    ("unit_separator", "a\x1fb", True),
    ("del", "a\x7fb", True),  # Python escapes; Go emits literally
    ("c1_control", "a\x80b", True),
    ("latin1", "café fix", True),
    ("cjk", "修復バグ", True),
    ("rtl", "مرحبا", True),
    ("line_separator", "a b", True),  # agrees, but coincidentally
    ("paragraph_separator", "a b", True),
    ("bmp_max", "a￿b", True),
    ("replacement_char", "a�b", True),
    ("emoji_astral", "ship it \U0001f600", True),
    ("astral_max", "a\U0010ffffb", True),
    ("combining", "é", True),  # differs from precomposed é
    ("zwj_sequence", "\U0001f469‍\U0001f4bb", True),
    ("long_text", "x" * 400, True),
    ("whitespace_only", "   \t\n  ", True),
    ("lone_high_surrogate", "a\ud800b", False),
    ("lone_low_surrogate", "a\udfffb", False),
    ("surrogateescape_byte", "a\udcffb", False),  # what errors="surrogateescape" yields
]

# --- Axis 2: document shapes ----------------------------------------------
# The `{}` placeholder marks where the axis-3 value is injected; each shape is
# rendered once per field position (axis 1).
SOURCE_TYPES = ("issue", "pr", "commit")


def _document_shapes(value: str, field: str) -> list[tuple[str, str, dict[str, Any]]]:
    """Return (label, work_unit_id, sources) for each document shape."""
    unit = "0" * 64

    if field == "work_unit_id":
        return [("value_in_work_unit_id", value, {t: {} for t in SOURCE_TYPES})]

    if field == "source_type_key":
        return [
            ("value_as_only_source_type", unit, {value: {"id-1": "text"}}),
            (
                "value_as_source_type_among_others",
                unit,
                {value: {"id-1": "text"}, "issue": {"id-2": "other"}, "pr": {}},
            ),
        ]

    if field == "source_id_key":
        return [
            ("value_as_only_source_id", unit, {"issue": {value: "text"}}),
            (
                "value_as_source_id_among_others",
                unit,
                {
                    "issue": {value: "text", "linear:X-1": "other", "a": "third"},
                    "pr": {},
                },
            ),
        ]

    # field == "text_value"
    return [
        ("value_as_only_text", unit, {"issue": {"linear:X-1": value}}),
        (
            "value_as_text_among_others",
            unit,
            {
                "issue": {"linear:X-1": value, "linear:X-2": "plain"},
                "pr": {"r#1": "pr text"},
                "commit": {},
            },
        ),
    ]


# --- Axis 1: field positions ----------------------------------------------
FIELDS = ("work_unit_id", "source_type_key", "source_id_key", "text_value")


def _serializer_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []

    # Structural shapes that carry no axis-3 value, pinned on their own: these
    # are where separator and empty-container handling show up.
    structural: list[tuple[str, str, dict[str, Any]]] = [
        ("empty_sources", "0" * 64, {}),
        ("all_source_types_empty", "0" * 64, {t: {} for t in SOURCE_TYPES}),
        ("single_source_single_text", "0" * 64, {"issue": {"linear:X-1": "hello"}}),
        (
            "key_sort_is_contested",
            "0" * 64,
            {
                # Deliberately inserted in NON-sorted order, mixing case,
                # underscore, colon and non-ASCII: pins that sorting is by code
                # point and not by anything locale- or case-aware.
                "pr": {"b": "1", "A": "2", "a": "3", "_": "4", ":": "5", "É": "6"},
            },
        ),
    ]
    for label, unit, sources in structural:
        cases.append(_build_serializer_case(label, "structural", unit, sources, True))

    for value_label, value, reachable in VALUE_CORPUS:
        for field in FIELDS:
            for shape_label, unit, sources in _document_shapes(value, field):
                cases.append(
                    _build_serializer_case(
                        f"{value_label}__{shape_label}",
                        field,
                        unit,
                        sources,
                        reachable,
                    )
                )
    return cases


def _build_serializer_case(
    label: str,
    field: str,
    work_unit_id: str,
    sources: dict[str, Any],
    reachable_in_go: bool,
) -> dict[str, Any]:
    """Serialize one payload through the EXACT expression evidence.py uses."""
    payload = {"work_unit_id": work_unit_id, "sources": sources}
    serialized = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    return {
        "label": label,
        "field_axis": field,
        "reachable_in_go": reachable_in_go,
        "work_unit_id_hex": _hex(work_unit_id),
        "sources_hex": {
            _hex(source_type): {_hex(k): _hex(v) for k, v in texts.items()}
            for source_type, texts in sources.items()
        },
        # ensure_ascii guarantees this is pure ASCII, so it survives the
        # fixture round trip verbatim and can be compared byte for byte.
        "serialized": serialized,
        "sha256": digest,
    }


def _bundle_cases() -> list[dict[str, Any]]:
    """Drive build_text_bundle itself, end to end.

    These pin the payload CONSTRUCTION as well as the serialization -- the
    truncation limits, the empty-text filtering, the handle numbering and the
    source ordering all feed the same hash. They are the layer that catches a
    correct serializer fed a wrongly-built payload.
    """
    cases: list[dict[str, Any]] = []

    scenarios: list[tuple[str, dict[str, Any]]] = [
        (
            "empty_everything",
            {
                "issue_ids": [],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {},
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "0" * 64,
            },
        ),
        (
            "one_of_each_plain",
            {
                "issue_ids": ["linear:X-1"],
                "pr_ids": ["repo#1"],
                "commit_ids": ["repo@abc123"],
                "work_item_map": {
                    "linear:X-1": {"title": "Fix login", "description": "It broke"}
                },
                "pr_map": {"repo#1": {"title": "fix: login", "body": "closes X-1"}},
                "commit_map": {"repo@abc123": {"message": "fix: login"}},
                "parent_titles": {"linear:X-1": "Auth epic"},
                "epic_titles": {"linear:X-1": "Q3 auth"},
                "work_unit_id": "1" * 64,
            },
        ),
        (
            "non_ascii_and_astral_titles",
            {
                "issue_ids": ["linear:X-2"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:X-2": {
                        "title": "café \U0001f600 fix",
                        "description": "Title & <b>body</b>   done",
                    }
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "2" * 64,
            },
        ),
        (
            "empty_text_is_filtered",
            {
                "issue_ids": ["linear:X-3", "linear:X-4"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:X-3": {"title": "", "description": ""},
                    "linear:X-4": {"title": "real", "description": ""},
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "3" * 64,
            },
        ),
        (
            "over_the_truncation_limits",
            {
                # MAX_ISSUES=6, MAX_PRS=6, MAX_COMMITS=12, MAX_FIELD_CHARS=280,
                # MAX_SOURCE_CHARS=900 -- exceed all of them at once so the
                # caps are pinned jointly rather than one at a time.
                "issue_ids": [f"linear:X-{n}" for n in range(10)],
                "pr_ids": [f"repo#{n}" for n in range(10)],
                "commit_ids": [f"repo@c{n}" for n in range(20)],
                "work_item_map": {
                    f"linear:X-{n}": {
                        "title": f"issue {n} " + "t" * 400,
                        "description": "d" * 1200,
                    }
                    for n in range(10)
                },
                "pr_map": {
                    f"repo#{n}": {"title": f"pr {n} " + "p" * 400, "body": "b" * 1200}
                    for n in range(10)
                },
                "commit_map": {
                    f"repo@c{n}": {"message": f"commit {n} " + "m" * 400}
                    for n in range(20)
                },
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "4" * 64,
            },
        ),
        (
            "whitespace_only_optional_fields",
            {
                # Python strips `type`, `parent_id` and `epic_id` BEFORE the
                # truthiness test, but NOT `title`/`description`. So a
                # whitespace-only type contributes nothing, while a
                # whitespace-only title is kept and then collapsed to "" by
                # truncation -- an asymmetry that is easy to normalise away.
                # Without the pre-strip, an orphan "Type: " line appears.
                # 0x1f is used deliberately: TrimSpace does not remove it.
                "issue_ids": ["linear:WS1", "linear:WS2", "linear:WS3"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:WS1": {
                        "title": "real title",
                        "description": "",
                        "type": "  \x1f \t ",
                        "parent_id": "   ",
                        "epic_id": "\x1f",
                    },
                    # Whitespace-only title, which IS kept before truncation.
                    "linear:WS2": {"title": "   \x1f  ", "description": ""},
                    # Parent/epic ids that strip to something real and ARE found.
                    "linear:WS3": {
                        "title": "third",
                        "description": "",
                        "parent_id": " \x1fP1\x1f ",
                        "epic_id": " E1 ",
                        "labels": ["", "  ", "kept"],
                    },
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {"P1": "Parent title"},
                "epic_titles": {"E1": "Epic title"},
                "work_unit_id": "d" * 64,
            },
        ),
        # --- commit MESSAGE line boundaries: the field axis, not just the value ---
        # Found by mutation testing: replacing SplitLines with a newline-only
        # split left every other case passing, because the separator bytes in
        # the corpus all sat in an issue TITLE. _commit_subject is the only
        # caller of splitlines, so only a commit message exercises it.
        (
            "commit_message_non_newline_boundaries",
            {
                "issue_ids": [],
                "pr_ids": [],
                "commit_ids": [
                    "repo@fs",
                    "repo@gs",
                    "repo@rs",
                    "repo@vt",
                    "repo@ff",
                    "repo@nel",
                    "repo@ls",
                    "repo@ps",
                    "repo@crlf",
                    "repo@lead",
                    "repo@usonly",
                    "repo@allws",
                ],
                "work_item_map": {},
                "pr_map": {},
                "commit_map": {
                    # Each boundary in the subject position: CPython stops at it,
                    # a newline-only split swallows the rest of the message.
                    "repo@fs": {"message": "subject\x1cbody line"},
                    "repo@gs": {"message": "subject\x1dbody line"},
                    "repo@rs": {"message": "subject\x1ebody line"},
                    "repo@vt": {"message": "subject\x0bbody line"},
                    "repo@ff": {"message": "subject\x0cbody line"},
                    "repo@nel": {"message": "subject\x85body line"},
                    "repo@ls": {"message": "subject\u2028body line"},
                    "repo@ps": {"message": "subject\u2029body line"},
                    # CRLF must count as ONE boundary, not two.
                    "repo@crlf": {"message": "subject\r\n\r\nbody"},
                    # Blank and whitespace-only leading lines are skipped, and
                    # 0x1f is whitespace but NOT a boundary -- so it is stripped
                    # from the line rather than ending it.
                    "repo@lead": {"message": "\n  \t \n\x1fsubject\x1frest\nbody"},
                    # 0x1f is whitespace but NOT a line boundary, so this first
                    # line survives splitlines and .strip() reduces it to "" --
                    # the subject is the SECOND line. A port using TrimSpace
                    # (which does not strip 0x1f) would take "\x1f" as a truthy
                    # subject, truncate it to "", and SET the commit key to an
                    # empty string instead of omitting it. Key presence changes
                    # input_hash, so this is a hash defect, not a cosmetic one.
                    "repo@usonly": {"message": "\x1f\nreal subject"},
                    # Whitespace-only throughout: no subject at all, so the key
                    # must be absent rather than present-and-empty.
                    "repo@allws": {"message": "\x1f\n \t \n\x1c\x1d"},
                },
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "b" * 64,
            },
        ),
        (
            "duplicate_ids_consume_the_cap",
            {
                # sorted(ids)[:MAX] is applied to the raw list INCLUDING
                # duplicates, so repeats push distinct ids out of the window.
                # Deduplicating first is the obvious tidy-up and changes which
                # sources are included -- also found by mutation testing.
                "issue_ids": ["linear:A"] * 5 + ["linear:B", "linear:C"],
                "pr_ids": ["repo#1"] * 6 + ["repo#2"],
                "commit_ids": ["repo@a"] * 12 + ["repo@b"],
                "work_item_map": {
                    "linear:A": {"title": "issue A", "description": ""},
                    "linear:B": {"title": "issue B", "description": ""},
                    "linear:C": {
                        "title": "issue C -- dropped by the cap",
                        "description": "",
                    },
                },
                "pr_map": {
                    "repo#1": {"title": "pr one", "body": ""},
                    "repo#2": {"title": "pr two -- dropped by the cap", "body": ""},
                },
                "commit_map": {
                    "repo@a": {"message": "commit a"},
                    "repo@b": {"message": "commit b -- dropped by the cap"},
                },
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "c" * 64,
            },
        ),
        # --- truncation boundary, the axis the ASCII case above cannot reach ---
        # _truncate_text slices with `compact[:limit]`, which counts CODE
        # POINTS. A Go port using byte slicing keeps 280 BYTES: 93 CJK chars
        # instead of 280, or 70 emoji instead of 280 -- and can cut a rune in
        # half, producing invalid UTF-8 that then encodes as U+FFFD. Every one
        # of those changes input_hash. Pure-ASCII fixtures cannot see any of
        # it, because there one char is one byte.
        (
            "truncation_boundary_cjk",
            {
                "issue_ids": ["linear:CJK"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:CJK": {
                        "title": "修" * 400,
                        "description": "復" * 400,
                    }
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "6" * 64,
            },
        ),
        (
            "truncation_boundary_astral",
            {
                # 4 bytes per code point: the worst char-to-byte ratio, and the
                # case most likely to be cut mid-rune by a byte-slicing port.
                "issue_ids": ["linear:EMOJI"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:EMOJI": {"title": "\U0001f600" * 400, "description": ""}
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "7" * 64,
            },
        ),
        (
            "truncation_exactly_on_the_limit",
            {
                # 279 / 280 / 281 characters: off-by-one in either direction
                # changes whether the ellipsis is appended at all.
                "issue_ids": ["linear:E279", "linear:E280", "linear:E281"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:E279": {"title": "a" * 279, "description": ""},
                    "linear:E280": {"title": "a" * 280, "description": ""},
                    "linear:E281": {"title": "a" * 281, "description": ""},
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "8" * 64,
            },
        ),
        (
            "python_only_whitespace_in_split",
            {
                # `" ".join(value.split())` splits on PYTHON whitespace, which
                # includes 0x1c-0x1f. Go's strings.Fields uses unicode.IsSpace,
                # a strict subset that does NOT, so a Go port built on Fields
                # leaves these bytes embedded and diverges. Same delta already
                # documented for str.strip() in investment/scope.go.
                "issue_ids": ["linear:WS"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:WS": {
                        "title": "alpha\x1cbeta\x1fgamma\x1ddelta\x1eepsilon",
                        "description": "  collapse\t\tthese   spaces\n\nplease  ",
                    }
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "9" * 64,
            },
        ),
        (
            "nested_truncation_non_ascii",
            {
                # MAX_FIELD_CHARS applies per field, then MAX_SOURCE_CHARS
                # applies to the joined result -- two truncations in sequence,
                # both code-point counted, with a "\n" join between them.
                "issue_ids": ["linear:NEST"],
                "pr_ids": [],
                "commit_ids": [],
                "work_item_map": {
                    "linear:NEST": {
                        "title": "修" * 300,
                        "description": "復" * 300,
                        "type": "バグ" * 50,
                        "labels": ["ラベル" * 20, "another" * 30],
                        "parent_id": "P1",
                        "epic_id": "E1",
                    }
                },
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {"P1": "親" * 200},
                "epic_titles": {"E1": "叙事詩" * 100},
                "work_unit_id": "a" * 64,
            },
        ),
        (
            "ids_present_but_absent_from_maps",
            {
                # A dangling id: present in the id list, missing from the map.
                "issue_ids": ["linear:MISSING"],
                "pr_ids": ["repo#404"],
                "commit_ids": ["repo@deadbeef"],
                "work_item_map": {},
                "pr_map": {},
                "commit_map": {},
                "parent_titles": {},
                "epic_titles": {},
                "work_unit_id": "5" * 64,
            },
        ),
    ]

    for label, kwargs in scenarios:
        bundle = build_text_bundle(**kwargs)
        cases.append(
            {
                "label": label,
                "inputs": kwargs,
                "input_hash": bundle.input_hash,
                "source_block_hex": _hex(bundle.source_block),
                "text_source_count": bundle.text_source_count,
                "text_char_count": bundle.text_char_count,
                "handle_map": {
                    handle: list(pair) for handle, pair in bundle.handle_map.items()
                },
                "source_texts_hex": {
                    _hex(source_type): {_hex(k): _hex(v) for k, v in texts.items()}
                    for source_type, texts in bundle.source_texts.items()
                },
            }
        )
    return cases


def main() -> None:
    serializer_cases = _serializer_cases()
    bundle_cases = _bundle_cases()

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_python_json_golden.py. Do not "
            "hand-edit. Regenerate with: uv run python "
            "tests/fixtures/generate_python_json_golden.py"
        ),
        "_what_this_pins": (
            "CPython json.dumps(payload, sort_keys=True, default=str) byte output "
            "and its sha256, which is evidence.build_text_bundle's input_hash and "
            "therefore the LLM skip-existing key."
        ),
        "serializer_cases": serializer_cases,
        "bundle_cases": bundle_cases,
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"

    # --stdout is how the Go rot guard re-runs this against live Python and
    # byte-diffs the result, without writing over the frozen fixture it is
    # checking. Nothing else about the output differs between the two modes.
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)

    reachable = sum(1 for case in serializer_cases if case["reachable_in_go"])
    print(f"wrote {OUTPUT_PATH}")
    print(
        f"  serializer_cases: {len(serializer_cases)} "
        f"({reachable} reachable in Go, {len(serializer_cases) - reachable} python-only)"
    )
    print(f"  bundle_cases:     {len(bundle_cases)}")


if __name__ == "__main__":
    main()
