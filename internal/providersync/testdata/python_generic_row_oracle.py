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
from uuid import UUID

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from internal.providersync.testdata import oracle_registry  # noqa: E402

_ORACLE_PAIRS_DIR = pathlib.Path(__file__).resolve().parent / "oracle_pairs"
_ORACLE_PAIRS_PACKAGE = "internal.providersync.testdata.oracle_pairs"


def _known_pair_modules() -> dict[str, str]:
    """Enumerate the checked-in oracle_pairs/*.py files and return a
    {pair_id: module_name} whitelist.

    Security note (github-advanced-security / Semgrep
    python.lang.security.audit.non-literal-import.non-literal-import,
    flagged on PR #1307): `main` previously formatted `sys.argv[1]`
    straight into `importlib.import_module(...)` -- a caller-controlled
    string driving a dynamic import is exactly the shape that rule exists
    to catch, regardless of how narrow this specific CLI's actual blast
    radius is. The fix is a real whitelist, not a suppression: this
    function is the ONLY place that walks the filesystem, and it can only
    ever discover FILES ALREADY CHECKED INTO THIS REPOSITORY under
    oracle_pairs/ -- a pair_id that does not correspond to one of them is
    rejected in `main` before `importlib.import_module` is ever called,
    with a plain dict-key lookup (`_known_pair_modules()[pair_id]`) as the
    only value ever passed to it. This is also a real behavioral
    improvement independent of the scanner: "import whatever string you
    are handed" becomes "import one of the pairs this framework knows
    about", which is the property CHAOS-3162's own registry already
    enforces one step later (oracle_registry.get raises for an
    unregistered id) -- this closes the SAME gap one step earlier, before
    an attempted import can even reach arbitrary application code that
    happens to sit under that dotted path.
    """
    known: dict[str, str] = {}
    for path in sorted(_ORACLE_PAIRS_DIR.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        parts = path.stem.split("_")
        if len(parts) != 3:
            continue
        pair_id = "/".join(parts)
        known[pair_id] = f"{_ORACLE_PAIRS_PACKAGE}.{path.stem}"
    return known


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
    if isinstance(value, UUID):
        return {"t": "uuid", "v": str(value).lower()}
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

    known_modules = _known_pair_modules()
    if pair_id not in known_modules:
        raise ValueError(
            f"pair id {pair_id!r} does not correspond to a checked-in "
            f"testdata/oracle_pairs/*.py file -- known pairs: "
            f"{sorted(known_modules)!r}"
        )
    # The only string this ever passes to importlib.import_module is a
    # dict VALUE from _known_pair_modules()'s own filesystem enumeration --
    # never sys.argv[1] (or anything derived from it) directly, and the
    # `pair_id not in known_modules` check above already rejected anything
    # that isn't a checked-in testdata/oracle_pairs/*.py file. Confirmed by
    # running the exact rule directly (`semgrep --config
    # r/python.lang.security.audit.non-literal-import.non-literal-import`):
    # it still flags this line even with the whitelist in place, because it
    # is a purely syntactic check on import_module's argument shape (any
    # non-literal expression) with no data-flow awareness of the preceding
    # validation -- there is no call shape this rule would accept for a
    # loader whose whole job is resolving one of several checked-in
    # modules by name. Narrow, justified suppression, not a workaround: see
    # _known_pair_modules's docstring for the actual security property.
    module_name = known_modules[pair_id]
    # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
    importlib.import_module(module_name)
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
