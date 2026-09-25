"""Live-Python oracle for `dho sync git|prs --provider local` (CHAOS-6775).

A line protocol on stdin/stdout, one JSON object per line:

  {"args": ["git", "--provider", "local", ...], "env": {...}}  -> {"stage": ...}

Each request runs the REAL verb end to end, with nothing replaced:
build_parser().parse_args(["sync", *args]), main()'s _resolve_org, the
preflight, then processors.sync.run_sync_target(ns), which opens the real
ClickHouse store (--analytics-db, an HTTP DSN) and runs process_local_repo. The
Go test compares the ClickHouse tables afterwards, so this program only reports
how the run ended: "ok", "exit" (SystemExit with its text) or "error" (an
uncaught exception, with its type).
"""

import contextlib
import io
import json
import os
import sys

from dev_health_ops import cli as devhops_cli
from dev_health_ops.processors import sync as sync_mod

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
    "REPO_UUID",
]


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    return {"t": "str", "v": str(value)}


def run(request):
    for name in MANAGED_ENV:
        os.environ.pop(name, None)
    for name, value in (request.get("env") or {}).items():
        os.environ[name] = value
    sink = io.StringIO()
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            ns = devhops_cli.build_parser().parse_args(["sync", *request["args"]])
    except SystemExit as exc:
        return {"stage": tag("argparse"), "code": tag(exc.code)}
    devhops_cli._resolve_org(ns)
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            devhops_cli.run_preflight_checks(devhops_cli.build_parser(), ns)
            code = sync_mod.run_sync_target(ns)
    except SystemExit as exc:
        return {"stage": tag("exit"), "message": tag(exc.code)}
    except Exception as exc:  # an uncaught traceback: exit 1
        return {"stage": tag("error"), "type": tag(type(exc).__name__)}
    return {"stage": tag("ok"), "code": tag(code)}


def main():
    for line in sys.stdin:
        print(json.dumps(run(json.loads(line))), flush=True)


main()
