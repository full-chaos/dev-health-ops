"""Regenerates window_golden.json from the REAL production window function.

    PYTHONPATH=src .venv/bin/python internal/queryapi/recommendations/testdata/gen_window_golden.py \
        > internal/queryapi/recommendations/testdata/window_golden.json

Every (unit, value) goes through resolvers.recommendations._window_to_dates
with today pinned, so the dates and the exceptions that escape are Python's
own answers, never hand authored.
"""

import json
from datetime import date

from dev_health_ops.api.graphql.models.recommendations import WindowInput, WindowUnit
from dev_health_ops.api.graphql.resolvers import recommendations as module

TODAY = date(2026, 9, 29)
module.utc_today = lambda: TODAY

VALUES = [
    0,
    1,
    2,
    4,
    52,
    1000,
    99999,
    142857142,
    142857143,
    71428571,
    71428572,
    999999999,
    1000000000,
    1000000,
    738000,
    738522,
    738523,
    2147483647,
    -1,
    -52,
    -1000000,
    -999999999,
    -1000000000,
    -2147483648,
]


def run(unit, value):
    try:
        start, end = module._window_to_dates(WindowInput(value=value, unit=unit))
    except BaseException as exc:
        return {"unit": unit.name, "value": value, "exception": type(exc).__name__}
    return {
        "unit": unit.name,
        "value": value,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


print(
    json.dumps([run(unit, value) for unit in WindowUnit for value in VALUES], indent=1)
)
