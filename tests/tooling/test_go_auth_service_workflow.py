"""Pins for the Auth Control Plane gate workflow (CHAOS-4887).

The gate logic is ``ci/check_go.sh auth``; the workflow only schedules it. What
could rot silently, and is pinned here: a job reusing a required context name (a
second producer of a required check, the CHAOS-4834 defect class), the fuzz
matrix drifting from the registry in ``check_go.sh`` (a parser whose fuzz job no
longer exists), and a fan-in that stops failing on a skipped relevant job.
"""

# CHAOS-10177: the YAML-shape tests of this file moved to internal/cigates/workflows_auth_service_test.go; the tests left here execute
# scripts and move in CHAOS-10178 (PR C), which then deletes this file.
from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = ROOT / "ci" / "check_go.sh"


def _run_closure(allow: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "AUTH_CLOSURE_ALLOW": str(allow)}
    return subprocess.run(
        ["bash", str(CHECK_GO), "auth", "closure"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_closure_check_executes_and_fails_on_a_forbidden_or_stale_module(
    tmp_path: Path,
) -> None:
    real = ROOT / "ci" / "auth_closure_allow.tsv"
    lines = real.read_text(encoding="utf-8").splitlines()

    ok = _run_closure(real)
    assert ok.returncode == 0, ok.stderr

    missing = tmp_path / "missing.tsv"
    missing.write_text(
        "\n".join(x for x in lines if not x.startswith("github.com/jackc/pgx/v5\t"))
        + "\n"
    )
    res = _run_closure(missing)
    assert res.returncode != 0 and "github.com/jackc/pgx/v5" in res.stderr

    stale = tmp_path / "stale.tsv"
    stale.write_text(
        "\n".join(lines) + "\ngithub.com/ClickHouse/clickhouse-go/v2\tplanted\n"
    )
    res = _run_closure(stale)
    assert res.returncode != 0 and "no longer link" in res.stderr
