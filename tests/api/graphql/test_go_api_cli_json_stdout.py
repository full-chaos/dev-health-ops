"""`--json` must emit ONLY JSON, proven through the REAL entrypoint.

This file exists because of a defect that every in-process test was
structurally incapable of catching. `dev-hops go-api routing status --json`
produced correct JSON preceded on stdout by the process's startup banner
("Sentry initialised", "OpenTelemetry tracing disabled", "Rate limiter using
Redis storage", …), so `json.load()` raised and a caller had to slice from
the first `{`.

Every test for that command captured stdout with `capsys`, INSIDE the
process, where the banner does not appear. The bug was only reachable via
`python -m dev_health_ops.cli`, so that is what these tests run --
subprocess, real argv, real startup path.

No database is required: `status` is contractually resilient to an
unreachable registry (it reports `registry_db_error` and exits 0), which
makes it the ideal subject -- the JSON contract is exercised on a machine
with nothing running.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
UNREACHABLE_PG = "postgresql+asyncpg://nobody:nobody@127.0.0.1:1/none"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "PYTHONPATH": "src",
        "DISABLE_DOTENV": "1",
        "OTEL_ENABLED": "false",
        "POSTGRES_URI": UNREACHABLE_PG,
    }
    env.pop("GO_API_QUERY_API_URL", None)
    return subprocess.run(
        [sys.executable, "-m", "dev_health_ops.cli", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


def test_status_json_stdout_is_parseable_with_no_slicing() -> None:
    """The whole contract, in one assertion: `json.loads(stdout)` works."""
    result = _run("go-api", "routing", "status", "--json")

    assert result.returncode == 0, result.stderr[-2000:]
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:  # pragma: no cover - the failure we guard
        pytest.fail(
            "stdout is not parseable JSON -- something is writing to stdout "
            f"ahead of the payload: {exc}\nfirst 300 chars: {result.stdout[:300]!r}"
        )
    assert "python_plane_schema_digest" in payload
    # Proves the command really ran its full path rather than exiting early.
    assert payload["registry_db_error"] is not None


def test_status_json_stdout_carries_no_log_banner() -> None:
    """Named guard for the exact strings that broke it.

    Asserted individually, no `or`: each is a distinct emitter, and a
    single one leaking is a regression even if the others are fixed.
    """
    result = _run("go-api", "routing", "status", "--json")
    for banner in (
        "Sentry initialised",
        "OpenTelemetry tracing",
        "Rate limiter using",
        "ClickHouse Connect",
        "go_api_operation_catalog.loaded",
    ):
        assert banner not in result.stdout, (
            f"{banner!r} reached stdout; --json must be machine-readable"
        )


def test_status_without_json_may_log_freely() -> None:
    """The converse: the human-readable mode is not silenced.

    A fix that muted logging everywhere would trade one defect for a worse
    one -- an operator debugging a broken stack needs that output.
    """
    result = _run("go-api", "routing", "status")
    assert result.returncode == 0
    assert "python plane schema_digest" in result.stdout
