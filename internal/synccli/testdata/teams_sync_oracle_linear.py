"""Live-Python oracle for `dho sync teams --provider linear` (CHAOS-6908).

A line protocol on stdin/stdout, one JSON object per line:

  {"argv": ["--org", "...", "sync", "teams", "--provider", "linear", ...],
   "env": {...}, "linear_base": "http://127.0.0.1:port"}  -> {"stage": ...}

Each request runs the REAL verb end to end: dev_health_ops.cli.main(argv)
(argument parsing, the preflight, providers.teams.sync_teams, the real
ClickHouseStore over an HTTP DSN). Python's LinearClient hardcodes its GraphQL
endpoint as the module-level constant LINEAR_API_URL (no env override exists
at all -- a named difference from GitHub/GitLab, whose Go verb needed a new
env spelling for the same reason); this program repoints that constant at the
fake server before every request instead. The Go test compares the ClickHouse
tables afterwards, so this program only reports how the run ended: "ok" with
the exit code, "exit" (SystemExit) or "error" (an uncaught exception).
"""

import contextlib
import io
import json
import os
import sys

import dev_health_ops.providers.linear.client as linear_client_module
from dev_health_ops import cli as devhops_cli

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "ORG_ID",
    "LINEAR_API_KEY",
]
_SET_BY_REQUEST: set[str] = set()
_ORIGINAL_LINEAR_API_URL = linear_client_module.LINEAR_API_URL


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    return {"t": "str", "v": str(value)}


def run(request):
    for name in [*MANAGED_ENV, *_SET_BY_REQUEST]:
        os.environ.pop(name, None)
    _SET_BY_REQUEST.clear()
    os.environ["DISABLE_DOTENV"] = "1"
    for name, value in (request.get("env") or {}).items():
        os.environ[name] = value
        _SET_BY_REQUEST.add(name)
    base = request.get("linear_base")
    linear_client_module.LINEAR_API_URL = (
        base.rstrip("/") + "/graphql" if base else _ORIGINAL_LINEAR_API_URL
    )
    sink = io.StringIO()
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            code = devhops_cli.main(request["argv"])
    except SystemExit as exc:
        return {"stage": tag("exit"), "code": tag(exc.code)}
    except Exception as exc:  # an uncaught traceback: exit 1
        return {"stage": tag("error"), "type": tag(type(exc).__name__)}
    return {"stage": tag("ok"), "code": tag(code)}


def main():
    import logging

    for line in sys.stdin:
        logging.disable(logging.NOTSET)
        print(json.dumps(run(json.loads(line))), flush=True)


main()
