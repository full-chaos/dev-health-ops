"""CHAOS-4815 — generate build-scope documents from a GRAMMAR and measure both planes.

# Why this exists

The `workgraph.build` scope gate was wrong in five consecutive review rounds.
Every fix was correct; every one was followed by a gap on an axis nobody had
enumerated:

    round 1  empty string                     value semantics (truthiness)
    round 2  whitespace                       value semantics, the neighbour
    round 3  null / numeric-falsy / +00       document shape
    round 4  repo_id, basic datetimes         WHICH FIELD carried the value
    round 5  20260815T060708+0000             date grammar x offset grammar

The corpus was hand-authored throughout, so it could only contain shapes its
author thought of. That converts "the author's model of parsing" into "the
author's model of which inputs matter" — real progress, and the same failure one
level up. The round-4 write-up said "the missing axis is invisible from inside
it"; round 5 then found an axis missing from inside that very statement.

Round 5's gap is the clearest case for this file: `20260815T060708+0000` is the
CROSS PRODUCT of two grammars already enumerated separately — basic dates, and
offsets. No one wrote the pair down. A generator over both produces it without
anyone having the idea.

# What it does

Composes scope documents from grammars rather than listing them, records the
REFERENCE's verdict for each (the bridge's own `_scope_arguments`, imported and
called, then `run_work_graph_build`'s window derivation against a frozen clock),
and emits them for a Go differential to replay. Generation is seeded, so a
failure is reproducible from its seed alone.

    python tests/fixtures/generate_scope_grammar_corpus.py --seed 1 --count 400
    python tests/fixtures/generate_scope_grammar_corpus.py --shrink failures.json

The `--shrink` mode takes documents a differential disagreed on and emits
structurally simpler candidates, so a failure is reported as the smallest
document that still diverges rather than as whatever random shape found it.
"""

from __future__ import annotations

import argparse
import itertools
import json
import platform
import random
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, "/app/src")

FROZEN_NOW = datetime(2026, 9, 1, 12, 30, 45, 500000, tzinfo=timezone.utc)

# Fields the bridge admits for this kind (worker_workgraph.py:74-80). An
# unsupported field is generated too, because the bridge rejects the whole
# request for one and a port that ignores unknown keys diverges silently.
ADMITTED_FIELDS = [
    "from_date",
    "to_date",
    "repo_id",
    "heuristic_window",
    "heuristic_confidence",
]
UNSUPPORTED_FIELDS = ["not_a_field", "org_id", "run_id"]

# --- value grammars -------------------------------------------------------
#
# Each is a small alphabet. The generator takes their CROSS PRODUCT, which is
# the part a hand-authored list cannot do: round 5's defect was exactly a pair
# of these that nobody thought to combine.

DATE_BODIES = ["2026-08-15", "20260815", "2026-W33-6", "2026-243"]
TIME_BODIES = ["", "06", "06:07", "06:07:08", "06:07:08.123", "0607", "060708"]
SEPARATORS = ["T", " ", "t", "_"]
OFFSETS = [
    "",
    "Z",
    "+00:00",
    "+0000",
    "+00",
    "+00:00:00",
    "+05:00",
    "-08:00",
    "+05",
    "-0800",
]

# Annotated explicitly: these are heterogeneous by design -- the falsy family
# spans null, string, bool, int, float, list and dict because the reference
# gates on TRUTHINESS, not on type, and a port that unmarshals into a typed
# field diverges on every non-string member.
FALSY: list[Any] = [None, "", False, 0, 0.0, -0.0, [], {}]
WHITESPACE: list[Any] = [" ", "\t", "\n", "  \t "]
NON_STRINGS: list[Any] = [True, 1, 123, 1.5, ["2026-08-15"], {"a": 1}]

# The MAGNITUDE axis (contributed by lane-4752-go: its corpus had no positive id
# above 42, and a port bounding values at 2^31 mislabelled large rows silently).
# Boundaries matter here in BOTH representations -- as JSON numbers, where a
# port's decoder may overflow, and as STRINGS, where a port's integer parser
# may bound at a width the reference does not have. Python's int() is unbounded;
# every Go integer type is not, so every boundary is a candidate divergence.
MAGNITUDES: list[Any] = [
    0,
    -0,
    1,
    -1,
    2147483647,
    2147483648,
    -2147483648,
    -2147483649,  # int32
    4294967295,
    4294967296,  # uint32
    9223372036854775807,
    9223372036854775808,  # int64
    18446744073709551615,
    18446744073709551616,  # uint64
    int("9" * 40),
    int("1" + "0" * 40),
    0.0,
    -0.0,
    1e308,
    # NOT 1e309. `json.loads("1e309")` yields `inf`, so the value IS reachable
    # over the wire -- but `json.dumps(inf)` emits the bare token `Infinity`,
    # which is not valid JSON and which Go's decoder refuses. Including it would
    # make this corpus file unreadable by the differential it feeds.
    #
    # That is a real cross-plane divergence in the TOOLING rather than in the
    # adapter, and it is the same class lane-4441 flagged for U+2028: two
    # encoders that mostly agree, differing on a carve-out. Both planes DO
    # agree on the input itself -- Python finds `inf` truthy and raises in
    # fromisoformat, and the Go adapter's float parse returns out-of-range
    # and then fails its not-a-string check -- so the case is covered by a
    # direct test rather than a corpus row it cannot survive.
]
MAGNITUDE_STRINGS: list[Any] = [str(value) for value in MAGNITUDES] + [
    "0" * 40,  # collapses to zero in both planes
    "9" * 40,
    "00000000000000000000000000000001",
]

_UUID = "7b9583ee-4d24-2be7-4d09-34f815bebdd7"
UUID_PREFIXES = ["", "urn:uuid:", "URN:UUID:", "urn:", "uuid:", "urn:urn:uuid:"]
UUID_BODIES = [_UUID, _UUID.upper(), _UUID.replace("-", ""), _UUID[:-1]]
UUID_WRAPPERS = [
    ("", ""),
    ("{", "}"),
    ("{{", "}}"),
    ("{", ""),
    ("", "}"),
    ("}}", "{{"),
    ("X", "X"),
    (" ", " "),
]

# --- document grammar -----------------------------------------------------

NON_OBJECT_DOCUMENTS = [None, [], ["2026-08-15"], "2026-08-15", 123, True, 1.5]


def datetime_values() -> Iterator[str]:
    """The cross product that round 5 proved a list cannot cover."""
    for date, sep, time, offset in itertools.product(
        DATE_BODIES, SEPARATORS, TIME_BODIES, OFFSETS
    ):
        if not time:
            # A bare date takes no separator and no offset in this grammar.
            if sep == "T" and not offset:
                yield date
            continue
        yield f"{date}{sep}{time}{offset}"


def uuid_values() -> Iterator[str]:
    for prefix, body, (open_wrap, close_wrap) in itertools.product(
        UUID_PREFIXES, UUID_BODIES, UUID_WRAPPERS
    ):
        yield f"{prefix}{open_wrap}{body}{close_wrap}"


def value_alphabet() -> list[Any]:
    values: list[Any] = []
    values.extend(FALSY)
    values.extend(WHITESPACE)
    values.extend(NON_STRINGS)
    values.extend(datetime_values())
    values.extend(uuid_values())
    values.extend(MAGNITUDES)
    values.extend(MAGNITUDE_STRINGS)
    values.extend(["not-a-date", "15/08/2026", "2026-13-45"])
    return values


def documents(rng: random.Random, count: int) -> Iterator[Any]:
    """Yield scope documents: non-objects, then random field/value combinations."""
    yield from NON_OBJECT_DOCUMENTS

    alphabet = value_alphabet()
    for _ in range(count):
        field_count = rng.choice([1, 1, 1, 2, 3])
        fields = rng.sample(ADMITTED_FIELDS, min(field_count, len(ADMITTED_FIELDS)))
        # Named distinctly from the non-object documents above: reusing one name
        # for both a bare value and a mapping made the whole generator's element
        # type ambiguous, which the type checker caught before this ever ran.
        scope: dict[str, Any] = {field: rng.choice(alphabet) for field in fields}
        # One in twelve carries an unsupported field, which the bridge rejects
        # outright -- the dangerous direction if a port ignores unknown keys.
        if rng.randrange(12) == 0:
            scope[rng.choice(UNSUPPORTED_FIELDS)] = rng.choice(alphabet)
        yield scope


# --- the reference --------------------------------------------------------


def _derive_window(arguments: dict[str, Any]) -> dict[str, Any]:
    """`run_work_graph_build`'s derivation (work_graph_tasks.py:121-135), verbatim."""
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


def admit(scope: Any) -> dict[str, Any]:
    """The bridge's own admission, then the reference's window derivation."""
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


# --- shrinking ------------------------------------------------------------


def shrink_candidates(document: Any) -> Iterator[Any]:
    """Structurally simpler documents, smallest first.

    A property-based failure is only useful if it is reported as the minimal
    document that still diverges. A three-field scope with a random UUID is a
    fact about the generator; `{"to_date": "20260815T060708+0000"}` is a fact
    about the code.
    """
    if not isinstance(document, dict):
        return
    fields = sorted(document)
    # Drop one field at a time, fewest fields first.
    for keep in range(1, len(fields)):
        for subset in itertools.combinations(fields, keep):
            yield {field: document[field] for field in subset}
    # Then simplify values within the single-field forms.
    for field in fields:
        value = document[field]
        if isinstance(value, str):
            for simpler in (value.strip(), value.lstrip("{").rstrip("}")):
                if simpler and simpler != value:
                    yield {field: simpler}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--count", type=int, default=400)
    parser.add_argument(
        "--shrink", help="JSON file of documents a differential disagreed on"
    )
    parser.add_argument("--out")
    args = parser.parse_args()

    if args.shrink:
        with open(args.shrink, encoding="utf-8") as handle:
            failures = json.load(handle)
        cases = []
        for failure in failures:
            document = (
                failure["scope"]
                if isinstance(failure, dict) and "scope" in failure
                else failure
            )
            for candidate in shrink_candidates(document):
                cases.append(
                    {"scope": candidate, "shrunk_from": document, **admit(candidate)}
                )
        payload: dict[str, Any] = {"schema": "scope_grammar_shrink.v1", "cases": cases}
    else:
        rng = random.Random(args.seed)
        cases = [
            {"scope": document, **admit(document)}
            for document in documents(rng, args.count)
        ]
        payload = {
            "schema": "scope_grammar_corpus.v1",
            "seed": args.seed,
            "count": args.count,
            "measured_on": platform.python_version(),
            "frozen_now": FROZEN_NOW.isoformat(),
            "cases": cases,
        }

    # allow_nan=False: json.dumps otherwise emits the bare tokens `Infinity`,
    # `-Infinity` and `NaN`, which are NOT valid JSON and which Go's decoder
    # rejects with a bare "invalid character 'I'". Failing here names the
    # offending value instead of writing a corpus that silently cannot be read.
    rendered = json.dumps(payload, indent=1, sort_keys=True, allow_nan=False) + "\n"
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        print(f"written {args.out}: {len(cases)} cases", file=sys.stderr)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
