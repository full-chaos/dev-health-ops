"""Live-Python oracle of the service-credentials command lines (CHAOS-6892).

A line protocol on stdin/stdout, one JSON object per line:

  {"verb": "rotate", "args": ["--scope", "x", "--", "ID"]}  ->  {"exit": 0, "ns": {...}} or {"exit": 2}

Each request runs the REAL argparse: dev_health_ops.cli.build_parser().parse_args(
["service-credentials", verb, *args]), with its stderr and stdout swallowed. "exit" is 0 for a parse
(or --help) and the SystemExit code otherwise; "ns" carries every value the verb's handler reads,
as text ({"t": "null"} when absent), so no bare number reaches a comparison.
"""

import contextlib
import io
import json
import sys

from dev_health_ops import cli as devhops_cli


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    if isinstance(value, list):
        return {"t": "list", "v": json.dumps(value)}
    return {"t": "str", "v": str(value)}


FIELDS = [
    "service",
    "scope",
    "expires_at",
    "created_by_user_id",
    "overlap_seconds",
    "credential_id",
    "db",
    "log_level",
    "analytics_db",
    "llm_provider",
    "model",
]


def run(parser, request):
    argv = ["service-credentials", request["verb"], *request["args"]]
    sink = io.StringIO()
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            ns = parser.parse_args(argv)
    except SystemExit as exc:
        return {"exit": exc.code if isinstance(exc.code, int) else 1}
    return {"exit": 0, "ns": {name: tag(getattr(ns, name, None)) for name in FIELDS}}


def main():
    import logging

    logging.disable(logging.CRITICAL)
    parser = devhops_cli.build_parser()
    for line in sys.stdin:
        print(json.dumps(run(parser, json.loads(line))), flush=True)


main()
