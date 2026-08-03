"""Regression coverage for the Go integration-package inventory gate."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_integration_coverage_inventory_completes_and_stays_nonempty() -> None:
    """A large inventory must not deadlock before the first package is printed.

    Bash implements a here-string as a producer that fills a pipe before the
    consumer starts. In environments whose pipe capacity is smaller than this
    repository's inventory, that producer blocks forever. Running the public
    discovery verb exercises the real inventory size and the same shell seam
    used by ``local_validate.sh``.
    """

    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-coverage"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "  RUN  internal/providersync" in result.stdout
    assert "package(s) discovered, 0 denylisted" in result.stdout
