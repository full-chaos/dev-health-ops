"""Live-Python oracle for `dho`'s root flags.

Reads a JSON list of argv lists from stdin. For each it runs the REAL Python
producer, build_parser().parse_args(argv) followed by main()'s _resolve_org,
with the environment the root defaults read cleared, and prints one JSON line:
for each argv, {"stage": "ok", "ns": {...}} with every root dest tagged
{"t": type, "v": string}, or {"stage": "exit", "code": N}.
"""

import argparse
import contextlib
import io
import json
import os
import sys

os.environ["DISABLE_DOTENV"] = "1"

from dev_health_ops import cli as devhops_cli  # noqa: E402

MANAGED_ENV = [
    "LOG_LEVEL",
    "POSTGRES_URI",
    "DATABASE_URI",
    "CLICKHOUSE_URI",
    "ORG_ID",
    "LLM_PROVIDER",
    "LLM_MODEL",
    "LLM_API_KEY",
    "LLM_BASE_URL",
    "INVESTMENT_LLM_CONCURRENCY",
]
DESTS = [
    "log_level",
    "db",
    "analytics_db",
    "org",
    "llm_provider",
    "model",
    "llm_api_key",
    "llm_base_url",
    "llm_concurrency",
]


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    return {"t": "str", "v": str(value)}


def run_case(argv):
    for name in MANAGED_ENV:
        os.environ.pop(name, None)
    parser = devhops_cli.build_parser()
    sink = io.StringIO()
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            ns = parser.parse_args(argv)
    except SystemExit as exit_:
        return {"stage": "exit", "code": tag(exit_.code if exit_.code is not None else 0)}
    devhops_cli._resolve_org(ns)
    return {
        "stage": "ok",
        "ns": {dest: tag(getattr(ns, dest, "<absent>")) for dest in DESTS},
    }


cases = json.load(sys.stdin)
print(json.dumps([run_case(argv) for argv in cases]))
