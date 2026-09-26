"""Live-Python oracle for `dho sync teams --provider github` (CHAOS-6895).

A line protocol on stdin/stdout, one JSON object per line:

  {"argv": ["--org", "...", "sync", "teams", "--provider", "github", ...],
   "env": {...}, "github_base": "http://127.0.0.1:port"}  -> {"stage": ...}

Each request runs the REAL verb end to end, with nothing replaced but the address
PyGithub talks to: dev_health_ops.cli.main(argv) (argument parsing, the preflight,
providers.teams.sync_teams, the real ClickHouseStore over an HTTP DSN). The Go test
compares the ClickHouse tables afterwards, so this program only reports how the run
ended: "ok" with the exit code, "exit" (SystemExit) or "error" (an uncaught exception).
"""

import contextlib
import io
import json
import os
import sys

import github

from dev_health_ops import cli as devhops_cli

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "ORG_ID",
    "GITHUB_TOKEN",
]
_SET_BY_REQUEST: set[str] = set()
_BASE = {"url": None}
_ORIGINAL_INIT = github.Github.__init__


def _patched_init(self, *args, **kwargs):
    if _BASE["url"]:
        kwargs["base_url"] = _BASE["url"]
    _ORIGINAL_INIT(self, *args, **kwargs)


setattr(github.Github, "__init__", _patched_init)


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
    _BASE["url"] = request.get("github_base")
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
