"""Regenerates failure_modes_golden.json from the REAL resolver.

    PYTHONPATH=src .venv/bin/python internal/queryapi/recommendations/testdata/gen_failure_modes_golden.py \
        > internal/queryapi/recommendations/testdata/failure_modes_golden.json

Each case runs resolvers.recommendations.resolve_recommendations with the
ClickHouse read (queries.client.query_dicts) replaced by a scripted one, and
records what the resolver answers: the rows it returns, or the exception that
escapes it. So which failures become an empty list and which fail the field
are Python's own answers, never hand authored.
"""

import asyncio
import json
import types
from datetime import date, datetime, timezone

from dev_health_ops.api.graphql.models.recommendations import WindowInput, WindowUnit
from dev_health_ops.api.graphql.resolvers import recommendations as module
from dev_health_ops.api.queries import client as client_module

module.utc_today = lambda: date(2026, 9, 29)

ROW = {
    "rule_id": "r",
    "team_id": "t",
    "org_id": "o",
    "latest_severity": "warning",
    "latest_window_start": date(2026, 4, 1),
    "latest_window_end": date(2026, 4, 7),
    "latest_computed_at": datetime(2026, 4, 8, tzinfo=timezone.utc),
    "latest_title": "T",
    "latest_rationale": "R",
    "latest_success_criterion": "S",
    "latest_evidence_json": "[]",
}


def scripted(mode):
    async def query_dicts(client, sql, params):
        if mode == "rows":
            return [ROW, ROW]
        if mode == "empty":
            return []
        if mode == "raises_at_once":
            raise RuntimeError("clickhouse unavailable")
        if mode == "raises_after_rows":
            # A stream that yields rows and then fails: query_dicts is all or
            # nothing, so the failure surfaces as one raise.
            raise RuntimeError("stream interrupted")
        raise AssertionError(mode)

    return query_dicts


async def run(window, mode):
    client_module.query_dicts = scripted(mode)
    context = types.SimpleNamespace(org_id="test-org", client=object())
    try:
        rows = await module.resolve_recommendations(context, "team-a", window)
    except BaseException as exc:
        return {"exception": type(exc).__name__}
    return {"rows": len(rows)}


CASES = [
    ("ok window", WindowInput(value=4, unit=WindowUnit.WEEK), "rows"),
    ("ok window", WindowInput(value=4, unit=WindowUnit.WEEK), "empty"),
    ("ok window", WindowInput(value=4, unit=WindowUnit.WEEK), "raises_at_once"),
    ("ok window", WindowInput(value=4, unit=WindowUnit.WEEK), "raises_after_rows"),
    ("oversized window", WindowInput(value=1000000, unit=WindowUnit.DAY), "rows"),
    (
        "oversized window",
        WindowInput(value=1000000, unit=WindowUnit.DAY),
        "raises_at_once",
    ),
    ("oversized window", WindowInput(value=142857143, unit=WindowUnit.WEEK), "empty"),
]

out = []
for label, window, mode in CASES:
    result = asyncio.run(run(window, mode))
    out.append({"window": label, "mode": mode, **result})
print(json.dumps(out, indent=1))
