from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_go_external_ingest_sink_golden_is_current() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/worker/generate_external_ingest_sink_golden.py"),
            "--check",
        ],
        cwd=root,
        env={**os.environ, "OTEL_ENABLED": "false"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
