"""Measure the BRIDGE'S OWN ADMISSION for every build-scope shape.

The Go pre-step in `cmd/dev-health-worker/workgraph_issue_pr_links.go` runs
before the Python bridge and writes to `work_graph_issue_pr`. So the property
that matters is not "does Go parse this string the same way" but:

    THE REFERENCE RAISES  =>  Go must REFUSE before writing anything.
    THE REFERENCE RUNS    =>  Go's window must equal the reference's.

Getting that backwards persists mapping rows for a build the bridge is about to
reject. Three separate defects in three review rounds were that exact shape.

So this table records the reference's admission, not a parser's opinion:

1. `worker_workgraph._scope_arguments` — the REAL function, imported and
   called. It raises on a non-object scope and on any unsupported field.
2. `run_work_graph_build`'s window derivation
   (`workers/work_graph_tasks.py:121-135`), reproduced verbatim below against a
   FROZEN `now` because the surrounding function opens a database. The six
   lines it reproduces are quoted in `_derive_window` so a reader can diff them.

The corpus is two-dimensional — the DOCUMENT's shape crossed with its VALUES —
because the value axis alone missed two real divergences (a top-level `null`
scope and the numeric-falsy family), and the shape axis is where the dangerous
direction lives.

Regenerate (prefer the container; the local interpreter is a faithful stand-in
only while it matches the shipped 3.14 line, and the header records which ran):

    docker cp tests/fixtures/generate_build_scope_parity_table.py dev-health-api-1:/tmp/
    docker exec -i dev-health-api-1 python /tmp/generate_build_scope_parity_table.py --stdout
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import json
import pathlib
import platform
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from typing import Any

# APPEND, never insert(0): inserting put the container path AHEAD of an explicit
# PYTHONPATH, so a stale /app/src could shadow the checkout the rot guard just
# verified -- a green run against the wrong bridge (CHAOS-4837 round 1, P2).
sys.path.append("/app/src")

# A fixed instant so "now" defaults are reproducible. The Go differential uses
# the same value for its own clock.
FROZEN_NOW = datetime(2026, 9, 1, 12, 30, 45, 500000, tzinfo=timezone.utc)

# Document shapes. The scope reaches the bridge as decoded JSON, so these are
# the shapes `_scope_arguments` can actually receive.
DOCUMENTS: list[tuple[str, Any]] = [
    ("object_empty", {}),
    ("null", None),
    ("array", []),
    ("array_nonempty", ["2026-08-15"]),
    ("string", "2026-08-15"),
    ("number", 123),
    ("bool", True),
    ("object_unsupported_field", {"not_a_field": 1}),
    ("object_unsupported_plus_valid", {"to_date": "2026-08-15", "nope": 1}),
]

# Value shapes, each placed under `to_date` in an otherwise valid object.
VALUES: list[Any] = [
    None,
    "",
    " ",
    "\t",
    "\n",
    "  \t ",
    False,
    True,
    0,
    0.0,
    -0.0,
    0e100,
    1e-400,
    1,
    123,
    [],
    {},
    ["2026-08-15"],
    "2026-08-15",
    "20260815",
    "2026-W33-6",
    "2026-243",
    "2026-08-15T06:07",
    "20260815T060708",
    "20260815T0607",
    "2026-08-15T06:07:08",
    "2026-08-15 06:07:08",
    "2026-08-15T06:07:08.123",
    "2026-08-15T06:07:08Z",
    "2026-08-15T06:07:08+00:00",
    "2026-08-15T06:07:08+0000",
    "2026-08-15T06:07:08+00",
    "2026-08-15T06:07:08+00:00:00",
    "2026-08-15T06:07:08+05:00",
    "2026-08-15T06:07:08-08:00",
    # BASIC date crossed with an OFFSET. The value axis had basic dates and it
    # had offsets, but never the two together -- and the Go layout table pairs
    # offsets only with EXTENDED dates, so the combination is exactly where a
    # divergence could hide. Asking "what was the same in every case?" of the
    # offset rows answers: every one of them was an extended date.
    "20260815T060708Z",
    "20260815T060708+00:00",
    "20260815T060708+0000",
    "20260815T060708+00",
    "20260815T0607+00:00",
    "20260815T060708+05:00",
    "20260815T060708-08:00",
    # A basic DATE with an offset and no time at all. Recorded because the
    # reference's handling of it is not what a reader would guess.
    "20260815+00:00",
    # EXTENDED date at MINUTE precision crossed with an offset -- the other half
    # of the same cross-product. The Go offset layouts carry minute precision
    # only in the colon-less and hour-only offset spellings, so the colon form
    # is missing. Named by the same review that named the basic-date gap; both
    # halves are recorded here so neither can be closed while the other stands.
    "2026-08-15T06:07+00:00",
    "2026-08-15T06:07Z",
    "2026-08-15T06:07+0000",
    "2026-08-15T06:07+05:00",
    # Space separator crossed with an offset, for the same reason.
    "2026-08-15 06:07:08+00:00",
    "2026-08-15 06:07:08Z",
]

# THE FULL DATE-TIME GRID.
#
# The hand-written values above grew one cell at a time, each added because a
# review round found the cell. That is how a layout gap survived to a seventh
# round: the corpus had basic dates and it had offsets, and nobody had crossed
# them; then it had the basic-date crossing and still not the minute-precision
# one. Closing cells does not close the class.
#
# So the datetime family is now a CROSS-PRODUCT rather than a list. Every
# combination is measured against the reference, and whichever ones Go refuses
# are enumerated as fail-closed divergences. A future layout change cannot open
# a hole here without the differential reporting a STALE entry.
_DATE_FORMS = [("2026-08-15", "extended"), ("20260815", "basic")]
_SEPARATORS = ["T", " ", "t", "_"]
_TIME_FORMS = {
    "extended": ["", "06:07", "06:07:08", "06:07:08.123"],
    "basic": ["", "0607", "060708", "060708.123"],
}
_OFFSETS = ["", "Z", "+00:00", "+0000", "+00", "+00:00:00", "+05:00", "-08:00"]

# OFFSET WELL-FORMEDNESS, the axis the grid did not have.
#
# `_OFFSETS` varies which VALID offset is used. It never varies whether the
# offset is valid at all, so every cell of the cross-product had a well-formed
# suffix -- and a review round walked straight into the gap by constructing
# malformed ones. A Go implementation that stripped colons before counting
# digits read "+00:", "+0:0", "+:00", "+::00", "+00::" and "+00:00:" as a zero
# offset and ACCEPTED all six, which the reference rejects: the dangerous
# direction, in a step that writes before the bridge validates.
#
# The lesson is the same one that produced the grid, one level up. Crossing the
# valid spellings closed the layout family and left "is it a spelling at all?"
# untested, because a cross-product only varies the axes it is given.
#
# Crossed with two bodies rather than the whole grid: well-formedness is a
# property of the suffix, and the body's form was already shown independent.
_MALFORMED_OFFSETS = [
    "+00:",
    "+0:0",
    "+:00",
    "+::00",
    "+00::",
    "+00:00:",
    "+0",
    "+000",
    "+00000",
    "+0:00",
    "+00:0",
    "++00:00",
    "+00:00:00:00",
    # Out of range rather than misspelled: the reference requires |offset| < 24h.
    "+24:00",
    "-24:00",
    "+99:00",
    # In range but non-zero via carry, which the reference ACCEPTS: "+00:60" is
    # one hour. Included so the corpus records that too.
    "+00:60",
    "+00:59:60",
]
_MALFORMED_BODIES = ["2026-08-15T06:07:08", "20260815T060708"]

# FIELD VALUE RANGES, the axis the grid did not have EITHER.
#
# The third time this exact lesson has been paid for on this branch. The grid
# varies the SHAPE of a date-time exhaustively and holds its VALUES constant:
# every cell is 2026-08-15, so nothing tests what happens at the edges of what
# a year, month or day may be.
#
# A review round constructed "0000-01-01T00:00:00+00:00". CPython raises
# "year must be in 1..9999, not 0"; Go's "2006" layout element reads "0000"
# happily, so Go ACCEPTED it. That is the dangerous direction, and worse than a
# normal one: a year-zero lower bound is not just wrong, it is meaningless to
# ClickHouse, which reads the Go driver's "0000-01-01 00:00:00" as
# "2026-01-01 00:00:00" and returns thousands of rows for a window nobody asked
# for.
#
# Shape and value are independent axes. Crossing one exhaustively says nothing
# about the other -- a cross-product only varies the axes it is given, which is
# now written here for the third time because it keeps being the answer.
_RANGE_VALUES = [
    # Year bounds. CPython's range is 1..9999 inclusive.
    "0000-01-01",
    "0000-01-01T00:00:00",
    "0000-01-01T00:00:00+00:00",
    "00000101",
    "0001-01-01",
    "0001-01-01T00:00:00Z",
    "9999-12-31",
    "9999-12-31T23:59:59",
    "9999-12-31T23:59:59.999999",
    # Month and day bounds, where Go's time.Parse validates and CPython does
    # too -- recorded so that agreement is asserted rather than assumed.
    "2026-00-01",
    "2026-13-01",
    "2026-01-00",
    "2026-01-32",
    "2026-02-30",
    "2026-02-29",  # 2026 is not a leap year
    "2024-02-29",  # 2024 is
    "2026-04-31",
    # Time-of-day bounds. "24:00" is the one the reference rolls to the next
    # day rather than rejecting, measured earlier on this lane.
    "2026-08-15T24:00",
    "2026-08-15T24:00:00",
    "2026-08-15T23:59:60",
    "2026-08-15T25:00:00",
    "2026-08-15T00:60:00",
    "2026-08-15T00:00:60",
]


def _datetime_grid() -> list[str]:
    """Every date x separator x time x offset combination, deduplicated."""
    grid: list[str] = []
    for date_text, kind in _DATE_FORMS:
        for time_text in _TIME_FORMS[kind]:
            for separator in _SEPARATORS:
                for offset in _OFFSETS:
                    if not time_text:
                        # No time means no separator; a bare date with an offset
                        # is still worth measuring (the reference reads the sign
                        # as a separator), so emit it once rather than per
                        # separator.
                        if separator != "T":
                            continue
                        grid.append(date_text + offset)
                        continue
                    grid.append(date_text + separator + time_text + offset)
    return list(dict.fromkeys(grid))


# The tail of the hand-written list: separators and malformed values that are
# not part of the datetime grid's axes.
VALUES += [
    "2026-08-15t06:07:08",
    "2026-08-15_06:07:08",
    "15/08/2026",
    "not-a-date",
    "2026-13-45",
]

# Appended LAST so its de-duplication sees every hand-written value: the grid
# re-derives several of them (the "t" and "_" separators among them), and a
# duplicate row would make the corpus look broader than it is.
VALUES += [value for value in _datetime_grid() if value not in VALUES]
VALUES += [
    body + offset
    for body in _MALFORMED_BODIES
    for offset in _MALFORMED_OFFSETS
    if body + offset not in VALUES
]
VALUES += [value for value in _RANGE_VALUES if value not in VALUES]


# The production source this file COPIES, anchored by text rather than by line
# number so the check survives unrelated edits above it.
_PRODUCTION_SOURCE = "src/dev_health_ops/workers/work_graph_tasks.py"
_ENCLOSING = "def run_work_graph_build("
_REGION_START = "now = datetime.now(timezone.utc)"
_REGION_END = "parsed_repo_id = uuid.UUID(repo_id) if repo_id else None"


def _production_window_digest() -> str:
    """Digest the production lines `_derive_window` reproduces.

    # Why this exists

    `_derive_window` COPIES six lines out of `run_work_graph_build` rather than
    calling it, because the surrounding function opens a database. That copy is
    invisible to every guard built on this file: change production's
    `timedelta(days=30)` to 31 and this generator emits byte-identical output,
    so the fixture stays "fresh" while the reference has moved (CHAOS-4837
    round 1, P1).

    Embedding this digest in the payload closes it WITHOUT a second mechanism:
    production changes -> digest changes -> regenerated fixture differs -> the
    existing rot guard goes red saying REFERENCE drift.

    # WHAT IS DIGESTED: the UNPARSED AST, not the source text

    The region is parsed and re-emitted with `ast.unparse` before hashing, so
    the digest is over the code's STRUCTURE. Formatting-only edits stay GREEN --
    reflowing, quote style, comment text, `1e2` vs `100.0`, hex vs decimal --
    while any behavioural change moves it. Verified by probe on review
    (lane-4441): every semantics-preserving edit tried produced the same digest,
    every behavioural one a different digest, and no semantic change was found
    that it hides.

    This matters because the alternative was a text digest, which turned the
    guard RED for a comma changed to a semicolon in a docstring -- a false
    positive that trains people to regenerate without reading.

    # Anchoring, and why it asserts so much

    The first version anchored on `now = datetime.now(timezone.utc)` and used a
    bare `find()`. That string occurs TWICE in the module, and `find()` took the
    earlier one -- so the digest silently covered lines 63-136: a DIFFERENT
    window derivation, a Celery decorator, and a full docstring, of which the
    six intended lines were a small part. It still contained the target, so the
    round-1 falsification passed and the bug was invisible from the outside.

    Two consequences, both measured by lane-4441 on review:
      * a comma changed to a semicolon IN A DOCSTRING flipped the digest and
        turned the guard red claiming REFERENCE drift, with behaviour unchanged
      * the docstring's own claim was false -- it digested ~60 lines it does not
        reproduce, so anyone checking the region boundaries would read the wrong
        span

    So the search now starts inside the function, and EVERY assumption raises
    rather than degrading: an ambiguous anchor is exactly the condition that
    made the first version silently wrong, so it is now the loudest failure.
    """
    # Importing this module initialises Celery, which configures logging to
    # STDOUT -- and stdout is the payload. Left unredirected it interleaved log
    # lines into the JSON and produced a fixture that would not parse. Contain
    # it rather than avoiding the import: resolving the module through the
    # import system is what makes this work both in the checkout and in the
    # container, where the generator runs from /tmp.
    #
    # This is durable BEYOND the import window, and only by luck of how logging
    # works -- worth stating so nobody "simplifies" it. `logging` binds its
    # handler to the stream OBJECT `sys.stderr` pointed at during the redirect,
    # so later logging in measure() still goes to stderr after the block exits.
    # Had it stored the NAME `sys.stdout` and looked it up lazily, this would
    # have covered only the import and the payload would be corrupted later.
    # Verified on review (lane-4441): root handler is ['stderr'] after the
    # block, post-import logging writes 0 lines to stdout, and an exception
    # raised inside the redirect PROPAGATES with sys.stdout restored.
    with contextlib.redirect_stdout(sys.stderr):
        import dev_health_ops.workers.work_graph_tasks as production

    path = pathlib.Path(production.__file__).resolve()
    source = path.read_text(encoding="utf-8")

    if source.count(_ENCLOSING) != 1:
        raise SystemExit(
            f"{_ENCLOSING!r} occurs {source.count(_ENCLOSING)} times in {path}; "
            "the digest cannot locate the enclosing function unambiguously."
        )
    within = source.index(_ENCLOSING)

    start = source.find(_REGION_START, within)
    end = source.find(_REGION_END, within)
    if start < 0 or end < 0:
        raise SystemExit(
            f"cannot locate the window-derivation region in {path} "
            f"(start={start} end={end}). The anchors moved -- re-anchor this "
            "digest and RE-VERIFY that _derive_window still reproduces production."
        )
    # ORDERING. `start < 0 or end < 0` does not catch anchors that are present
    # but REVERSED: the slice would then be empty, `ast.parse("")` succeeds, and
    # the digest becomes e3b0c442... -- the SHA of nothing. Stable, plausible,
    # covering NOTHING, and green forever including after a regeneration.
    #
    # That is precisely what this function's own docstring warns about: a digest
    # that silently degrades is worse than none, because it still reads as a
    # check. The `< 0` guard does not watch this door. Ordering holds today only
    # by accident of how the production function is written, and an accident of
    # production layout must not be load-bearing for a guard whose whole purpose
    # is noticing that layout change. Found by lane-4441 on re-review.
    if start >= end:
        raise SystemExit(
            f"the end anchor precedes the start anchor in {path} "
            f"(start={start} end={end}); the region would be EMPTY and its digest "
            "would be the SHA of nothing. Re-anchor before trusting this digest."
        )

    # Ambiguity INSIDE the function is what the first version got wrong at module
    # scope. Assert it here too rather than trusting that one search fixed it.
    span = source[within:end]
    if span.count(_REGION_START) != 1:
        raise SystemExit(
            f"start anchor {_REGION_START!r} is ambiguous within {_ENCLOSING!r}: "
            f"{span.count(_REGION_START)} matches. Re-anchor before trusting this digest."
        )

    # Take whole LINES so the block dedents cleanly.
    start = source.rfind("\n", 0, start) + 1
    region = source[start : end + len(_REGION_END)]

    # Normalise via the AST, not a regex.
    #
    # The regex this replaces was `re.sub(r"#.*", "")`, which strips from any `#`
    # to end of line INCLUDING one inside a string literal -- so
    # `url = "http://x/#frag"; window = 30` and the same line with 31 both
    # normalise to `url = "http://x/` and digest EQUAL, swallowing the change.
    # Latent today (no such line in the region) but the exact "safe because
    # today's inputs are tame" shape. ast.unparse drops comments and formatting
    # without touching string contents, and RAISES on malformed input rather
    # than silently truncating it.
    try:
        normalised = ast.unparse(ast.parse(textwrap.dedent(region)))
    except SyntaxError as error:
        raise SystemExit(
            f"the window-derivation region in {path} does not parse: {error}. "
            "The anchors are probably spanning the wrong block."
        ) from error
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def _derive_window(arguments: dict[str, Any]) -> dict[str, Any]:
    """Reproduce `run_work_graph_build`'s window derivation, verbatim.

    From `src/dev_health_ops/workers/work_graph_tasks.py:121-135`:

        now = datetime.now(timezone.utc)
        if to_date:   parsed_to = datetime.fromisoformat(to_date)
        else:         parsed_to = now
        if from_date: parsed_from = datetime.fromisoformat(from_date)
        else:         parsed_from = parsed_to - timedelta(days=30)
        parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    Reproduced rather than called because the surrounding task opens a
    database. `now` is frozen so the defaults are reproducible.
    """
    import uuid

    to_date = arguments.get("to_date")
    from_date = arguments.get("from_date")
    repo_id = arguments.get("repo_id")

    parsed_to = datetime.fromisoformat(to_date) if to_date else FROZEN_NOW
    parsed_from = (
        datetime.fromisoformat(from_date)
        if from_date
        else parsed_to - timedelta(days=30)
    )
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None
    return {
        "from": parsed_from.isoformat(),
        "to": parsed_to.isoformat(),
        "repo_id": str(parsed_repo_id) if parsed_repo_id else None,
    }


def _admit(scope: Any) -> dict[str, Any]:
    """Run the reference's admission for one scope, recording RAISES or RUNS."""
    from dev_health_ops.api.internal import worker_workgraph

    row = {
        "org_id": "70d529e0-3c06-4597-8480-794fd02328b6",
        "model_ref": None,
        "llm_concurrency": 1,
    }
    try:
        arguments = worker_workgraph._scope_arguments("workgraph.build", scope, row)
    except Exception as error:  # noqa: BLE001 - the verdict IS the exception
        return {
            "verdict": "RAISES",
            "stage": "scope_arguments",
            "error": type(error).__name__,
        }
    try:
        return {"verdict": "RUNS", "window": _derive_window(arguments)}
    except Exception as error:  # noqa: BLE001
        return {"verdict": "RAISES", "stage": "window", "error": type(error).__name__}


# UUID spellings. Python's uuid.UUID strips a LOWERCASE `urn:`/`uuid:` prefix
# and surrounding braces before checking 32 hex digits, so its accepted set is
# not the one a general-purpose UUID parser implements -- google/uuid.Parse is
# case-insensitive about the URN prefix and therefore accepts input the
# reference rejects. That divergence was invisible until this corpus gained a
# FIELD axis, because every value used to be placed under `to_date` only.
_UUID = "11111111-1111-4111-8111-111111111111"
UUID_VALUES: list[Any] = [
    _UUID,
    _UUID.upper(),
    _UUID.replace("-", ""),
    _UUID.replace("-", "").upper(),
    "{" + _UUID + "}",
    "{" + _UUID.replace("-", "") + "}",
    "urn:uuid:" + _UUID,
    "URN:UUID:" + _UUID,
    "Urn:Uuid:" + _UUID,
    "urn:uuid:" + _UUID.replace("-", ""),
    "urn:uuid:{" + _UUID + "}",
    "  " + _UUID + "  ",
    _UUID + "x",
    _UUID[:-1],
    "not-a-uuid",
    # Shapes CPython accepts that a "prefix + literal braces" description of the
    # accept set gets WRONG: `replace` removes every occurrence anywhere, and
    # `strip('{}')` removes any number of leading/trailing braces, balanced or
    # not. Measured, not described.
    "{{" + _UUID + "}}",
    "{{{" + _UUID + "}}}",
    "{" + _UUID,
    _UUID + "}",
    "urn:" + _UUID,
    "uuid:" + _UUID,
    "urn:urn:uuid:" + _UUID,
    # Arbitrary surrounding characters. google/uuid.Parse strips these at length
    # 38 WITHOUT checking they are braces; CPython raises. The dangerous
    # direction, and the reason this corpus exists.
    "X" + _UUID + "X",
    "[" + _UUID + "]",
    "!" + _UUID + "?",
    # NON-ASCII, which is where the two fields of one scope document part
    # company. `repo_id` reaches `uuid.UUID`, whose last step is `int(hex, 16)`
    # and which FOLDS Unicode decimal digits to ASCII -- so "１"*32 is a valid
    # UUID there. `to_date` reaches `datetime.fromisoformat`, which is
    # ASCII-ONLY and rejects the same characters outright. Both measured.
    #
    # Two CPython parsers with opposite Unicode policies, in the same document,
    # and until now the corpus put non-ASCII values only under the date fields.
    # A Go port that reached for one policy and applied it to both would be
    # wrong on one of them, in a direction no other row can see.
    "１" * 32,
    "１" * 16 + "1" * 16,
    "٠" * 32,
    "0x" + "1" * 30,
    " " + "1" * 30 + " ",
    "1_1_1_1_1_1_1_1_1_1_1_1_1_1_1_11",
    "+" + "1" * 31,
    # Nd in Unicode 17.0.0 (Go) and absent from 16.0.0 (this interpreter): the
    # ten codepoints where Go's unicode package is WIDER than CPython's table.
    "\U00011de0" * 32,
]

# The FIELD axis. Every scope field the bridge admits, crossed with every value
# shape -- the axis whose absence hid a repo_id-specific divergence through four
# review rounds. `heuristic_*` are admissible to the bridge and unused by this
# step; they are included to prove their presence never breaks it.
FIELDS = ["to_date", "from_date", "repo_id", "heuristic_window", "heuristic_confidence"]


def measure() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, document in DOCUMENTS:
        rows.append({"case": f"document:{name}", "scope": document, **_admit(document)})
    for field in FIELDS:
        values = VALUES + (UUID_VALUES if field == "repo_id" else [])
        for value in values:
            scope = {field: value}
            rows.append({"case": f"field:{field}", "scope": scope, **_admit(scope)})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default="/tmp/build_scope_parity_table.json")
    args = parser.parse_args()

    payload = (
        json.dumps(
            {
                "schema": "build_scope_parity_table.v2",
                "production_window_digest": _production_window_digest(),
                "frozen_now": FROZEN_NOW.isoformat(),
                "measured_on": platform.python_version(),
                "cases": measure(),
            },
            indent=1,
            sort_keys=True,
        )
        + "\n"
    )
    if args.stdout:
        sys.stdout.write(payload)
    else:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"written {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
