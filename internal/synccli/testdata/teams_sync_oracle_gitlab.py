"""Live-Python oracle for `dho sync teams --provider gitlab` (CHAOS-6907).

A line protocol on stdin/stdout, one JSON object per line:

  {"argv": ["--org", "...", "sync", "teams", "--provider", "gitlab", ...],
   "env": {...}}  -> {"stage": ...}

Each request runs the REAL verb end to end: dev_health_ops.cli.main(argv)
(argument parsing, the preflight, providers.teams.sync_teams, the real
ClickHouseStore over an HTTP DSN). Unlike the GitHub oracle, no client
monkeypatch is needed: the gitlab branch of providers/teams.py already reads
its base URL from the GITLAB_URL environment variable
(`os.getenv("GITLAB_URL", "https://gitlab.com")`), so the request's own env
dict points python-gitlab at the fake server directly. The Go test compares
the ClickHouse tables afterwards, so this program only reports how the run
ended: "ok" with the exit code, "exit" (SystemExit) or "error" (an uncaught
exception).
"""

import contextlib
import io
import json
import os
import sys

from dev_health_ops import cli as devhops_cli

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "ORG_ID",
    "GITLAB_TOKEN",
    "GITLAB_URL",
]
_SET_BY_REQUEST: set[str] = set()


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
