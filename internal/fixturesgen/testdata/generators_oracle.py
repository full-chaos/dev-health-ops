"""Live-Python oracle of the fixture generators ported to Go (CHAOS-6884).

A line protocol on stdin/stdout, one JSON object per line:

  {"kind": "random", "seed": "<int as text>", "ops": [["getrandbits", 64], ...]}
      -> {"values": [{"t": ..., "v": ...}, ...]}
     drives a REAL random.Random(seed) with each op: getrandbits(k), randrange(n),
     randint(a, b), choice (the index random.choice takes over range(n)) and random().

  {"kind": "product_telemetry", "org_id": ..., "days": ..., "sessions_per_day": ...,
   "seed": <int|null>, "end_time": "<iso8601 with offset>"}
      -> {"columns": [...], "rows": [[{"t","v"}, ...], ...], "ceiling": <int|null>}
     runs the REAL ProductTelemetryGenerator and the REAL
     persist_product_telemetry_events, with only the ClickHouse client replaced by a
     capture, so the rows are exactly what the production path would insert. The columns
     are PRODUCT_TELEMETRY_COLUMNS from the production module, not a list kept here.

Every leaf is tagged {"t": type, "v": text} so no bare JSON number reaches a comparison.
"""

import asyncio
import json
import random
import struct
import sys
from datetime import datetime

from dev_health_ops.api.product_telemetry import persist
from dev_health_ops.fixtures.generators.product_telemetry import (
    SOURCE,
    ProductTelemetryGenerator,
    ProductTelemetrySeedSpec,
)
from dev_health_ops.fixtures.ttl_horizon import max_generated_age_days_for_table


def leaf(value):
    if value is None:
        return {"t": "null", "v": ""}
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, float):
        return {"t": "float", "v": struct.pack(">d", value).hex()}
    if isinstance(value, datetime):
        assert value.tzinfo is None, "persist hands ClickHouse naive UTC datetimes"
        return {"t": "datetime", "v": value.isoformat()}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    raise TypeError(f"unhandled leaf type {type(value)}")


def run_random(request):
    rng = random.Random(int(request["seed"]))
    values = []
    for op in request["ops"]:
        name, args = op[0], op[1:]
        if name == "getrandbits":
            values.append(leaf(rng.getrandbits(args[0])))
        elif name == "randrange":
            values.append(leaf(rng.randrange(args[0])))
        elif name == "randint":
            values.append(leaf(rng.randint(args[0], args[1])))
        elif name == "choice":
            values.append(leaf(rng.choice(range(args[0]))))
        elif name == "random":
            values.append(leaf(rng.random()))
        else:
            raise ValueError(name)
    return {"values": values}


class CaptureClient:
    def __init__(self):
        self.rows = []
        self.columns = None

    def insert(self, table, rows, column_names):
        assert table == "product_telemetry_events"
        self.rows.extend(rows)
        self.columns = list(column_names)


class CaptureSink:
    def __init__(self, client):
        self.client = client

    def close(self):
        pass


def run_product_telemetry(request):
    end_time = datetime.fromisoformat(request["end_time"])
    spec = ProductTelemetrySeedSpec(
        org_id=request["org_id"],
        days=request["days"],
        sessions_per_day=request["sessions_per_day"],
        seed=request["seed"],
        end_time=end_time,
    )
    events = ProductTelemetryGenerator(spec).generate_events()
    client = CaptureClient()
    persist.create_sink = lambda: CaptureSink(client)
    asyncio.run(persist.persist_product_telemetry_events(events, source=SOURCE))
    return {
        "columns": client.columns or list(persist.PRODUCT_TELEMETRY_COLUMNS),
        "rows": [[leaf(v) for v in row] for row in client.rows],
        "ceiling": max_generated_age_days_for_table("product_telemetry_events"),
    }


def run_synthetic_orgs(request):
    # The fallback ids of `fixtures product-telemetry`, computed as its source does
    # (runner.py run_product_telemetry_fixtures).
    import uuid

    return {
        "ids": [
            str(uuid.uuid5(uuid.NAMESPACE_URL, f"seed-org-{idx}"))
            for idx in range(int(request["count"]))
        ]
    }


def main():
    import logging

    logging.disable(logging.CRITICAL)
    for line in sys.stdin:
        request = json.loads(line)
        try:
            if request["kind"] == "random":
                answer = run_random(request)
            elif request["kind"] == "synthetic_orgs":
                answer = run_synthetic_orgs(request)
            elif request["kind"] == "product_telemetry":
                answer = run_product_telemetry(request)
            else:
                raise ValueError(request["kind"])
        except Exception as exc:  # reported, never swallowed: the Go side fails on it
            answer = {"error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(answer), flush=True)


main()
