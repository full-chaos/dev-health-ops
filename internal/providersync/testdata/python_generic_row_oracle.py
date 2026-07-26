#!/usr/bin/env python3
"""Generic CLI runner for the declarative oracle registry (CHAOS-3162).

Usage:
    python3 python_generic_row_oracle.py <pair_id> <cases.json>

<pair_id> is e.g. "github/prs/row". Importing its registration module is a
side effect of the naming convention below -- this file itself never
mentions github, launchdarkly, or any other provider by name, which is the
whole point: adding a pair means adding one file under oracle_pairs/, never
editing this one.

<cases.json> is a JSON array of case objects; each MUST have a unique "id"
key (a stable, human-readable name for the case) plus whatever keys the
pair's build_row expects. The array must be non-empty: a vacuous comparison
(nothing to compare) is a caller error, not a silent pass -- codex findings
#6/#7, CHAOS-3162's second adversarial review.

Prints one JSON object to stdout:

    {
      "cases": [{"id": ..., "row": {...typed-encoded...}}, ...],
      "excluded_fields": {"field": "reason", ...}
    }

Every LEAF value in "row" is TYPE-TAGGED: {"t": "<pytype>", "v": "<string>"}
(bare JSON `null` for None, which needs no type tag since it carries no
ambiguity). This is codex finding #2, CHAOS-3162's second adversarial
review: bare JSON numbers decode as Go float64, which collapses an int and
an integral float to the same value AND loses precision above 2**53; a bare
ISO string is indistinguishable from a genuinely-string field that happens
to look like a timestamp. Tagging every leaf with its Python type and
carrying the value as an untouched STRING (never a JSON number, which is
the one JSON representation with a lossy default Go decode) closes both
holes: the comparator on the Go side compares the (tag, string) pairs
structurally, so "5" tagged int and "5" tagged float compare UNEQUAL, and
"9007199254740992" compares by exact string equality, never by decoding
through float64 at all.
"""

from __future__ import annotations

import importlib
import json
import pathlib
import sys
from datetime import date, datetime, timezone
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from internal.providersync.testdata import oracle_registry  # noqa: E402


def _module_name_for_pair(pair_id: str) -> str:
    parts = pair_id.split("/")
    if len(parts) != 3:
        raise ValueError(
            f"pair id must be '<provider>/<dataset>/<boundary>', got {pair_id!r}"
        )
    return "internal.providersync.testdata.oracle_pairs." + "_".join(parts)


def _encode(value: Any) -> Any:
    """Type-tag every leaf value (codex finding #2). See module docstring
    for the wire format and why. bool is checked before int: in Python,
    bool is an int subclass, so `isinstance(True, int)` is True and an
    int-first check would silently mis-tag every boolean as "int"."""
    if value is None:
        return None
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, float):
        return {"t": "float", "v": repr(value)}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    if isinstance(value, datetime):
        # Always UTC, always explicit -- never the ambient system timezone
        # (an earlier version of this function used value.astimezone() with
        # no argument, which silently converted to local time and would have
        # made every timestamp comparison fail depending on where the
        # process happened to run).
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        encoded = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return {"t": "datetime", "v": encoded}
    if isinstance(value, date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, dict):
        return {key: _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    raise TypeError(
        f"_encode: unsupported type {type(value)!r} for value {value!r} -- "
        "every leaf type this oracle can emit must have an explicit, "
        "type-tagged encoding; falling through to an untagged pass-through "
        "would silently reopen codex finding #2"
    )


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    pair_id, cases_path = sys.argv[1], sys.argv[2]

    importlib.import_module(_module_name_for_pair(pair_id))
    spec = oracle_registry.get(pair_id)

    cases = json.loads(pathlib.Path(cases_path).read_text())
    if not isinstance(cases, list):
        raise ValueError("cases.json must be a JSON array")
    if not cases:
        raise ValueError(
            "cases.json is empty -- a comparison run with zero cases proves "
            "nothing and must not be allowed to look like a pass (codex "
            "findings #6/#7, CHAOS-3162 second review)"
        )

    seen_ids: set[str] = set()
    results = []
    for case in cases:
        case_id = case.get("id")
        if not case_id:
            raise ValueError(f"case missing required 'id' key: {case}")
        if case_id in seen_ids:
            raise ValueError(
                f"duplicate case id {case_id!r} -- python_generic_row_oracle.py "
                "keys its output by case id, so a duplicate would let one "
                "case's Python result be compared against a DIFFERENT case's "
                "Go result on the caller side without either side detecting "
                "it (codex finding #6, CHAOS-3162 second review)"
            )
        seen_ids.add(case_id)
        row = spec.build_row(case)
        if not isinstance(row, dict):
            raise TypeError(
                f"case {case_id!r}: build_row returned {type(row)!r}, not a dict"
            )
        oracle_registry.check_completeness(pair_id, case_id, row)
        results.append({"id": case_id, "row": _encode(row)})

    print(json.dumps({"cases": results, "excluded_fields": spec.excluded_fields}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
