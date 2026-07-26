#!/usr/bin/env python3
"""Generic CLI runner for the declarative oracle registry (CHAOS-3162).

Usage:
    python3 python_generic_row_oracle.py <pair_id> <cases.json>

<pair_id> is e.g. "github/prs/row". Importing its registration module is a
side effect of the naming convention below -- this file itself never
mentions github, launchdarkly, or any other provider by name, which is the
whole point: adding a pair means adding one file under oracle_pairs/, never
editing this one.

<cases.json> is a JSON array of case objects; each MUST have an "id" key
(a stable, human-readable name for the case) plus whatever keys the pair's
build_row expects. Prints one JSON object to stdout:

    {
      "cases": [{"id": ..., "row": {...fully JSON-encoded...}}, ...],
      "excluded_fields": {"field": "reason", ...}
    }
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
    if isinstance(value, datetime):
        # Always UTC, always explicit -- never the ambient system timezone
        # (an earlier version of this function used value.astimezone() with
        # no argument, which silently converted to local time and would have
        # made every timestamp comparison fail depending on where the
        # process happened to run).
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    return value


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

    results = []
    for case in cases:
        case_id = case.get("id")
        if not case_id:
            raise ValueError(f"case missing required 'id' key: {case}")
        row = spec.build_row(case)
        results.append({"id": case_id, "row": _encode(row)})

    print(json.dumps({"cases": results, "excluded_fields": spec.excluded_fields}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
