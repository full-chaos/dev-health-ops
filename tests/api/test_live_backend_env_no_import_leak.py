"""Guard: importing the auth test modules must never leak live-backend env.

CHAOS-5409: three modules under ``tests/api/`` used to write
``CLICKHOUSE_URI``, ``JWT_SECRET_KEY`` and ``SETTINGS_ENCRYPTION_KEY`` into
``os.environ`` at IMPORT time via a module-level ``os.environ.setdefault(...)``.
Because pytest imports every module at collection, this activated four
``tests/**/*_live.py`` suites (and any other ``CLICKHOUSE_URI``-gated test)
against a REAL ClickHouse on any host with one listening -- not an isolated
test-fixture artefact, an ambient process-global write with no owner.

This module is deliberately UNGATED (no ``skipif`` on any live-backend
variable, trap #45) so it always runs, including in CI where no ClickHouse is
reachable -- a test that only runs when the bug's precondition is absent would
prove nothing.

The check runs in a FRESH subprocess rather than re-importing the modules in
this process: pytest has already imported every collected test file by the
time any test body executes, so a same-process ``import tests.api.foo`` here
would be a sys.modules cache hit that could never observe a fresh-import
side effect, passing vacuously regardless of collection order (trap #47/#48
class -- a check that cannot fail is the defect it hunts).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_WATCHED_VARS = ("CLICKHOUSE_URI", "JWT_SECRET_KEY", "SETTINGS_ENCRYPTION_KEY")

_MODULES = (
    "tests.api.test_analytics_auth",
    "tests.api.test_work_unit_explain_hardening",
    "tests.api.test_telemetry_auth",
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

_PROBE = f"""
import importlib
import os
import sys

watched = {_WATCHED_VARS!r}
for name in watched:
    assert name not in os.environ, f"pre-set unexpectedly: {{name}}"

for mod in {_MODULES!r}:
    importlib.import_module(mod)

leaked = [name for name in watched if name in os.environ]
if leaked:
    print("LEAKED:" + ",".join(leaked))
    sys.exit(1)
print("CLEAN")
"""


def test_importing_auth_test_modules_does_not_leak_live_backend_env() -> None:
    env = {k: v for k, v in os.environ.items() if k not in _WATCHED_VARS}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=_REPO_ROOT,
        env={**env, "PYTHONPATH": f"src:{_REPO_ROOT}"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "importing the auth test modules leaked live-backend env "
        f"(CHAOS-5409 regression):\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "CLEAN" in result.stdout, result.stdout
