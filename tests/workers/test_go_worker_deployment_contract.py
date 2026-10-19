"""CHAOS-3052 and later: the Python image's job-contract packaging.

Every other guard of the Go worker deployment contract moved to Go
(internal/cigates/chartgates/contract_test.go, dep*). This one test reads pyproject.toml and the
Python Dockerfile, so it stays until the Python image goes (CHAOS-3165 line).
"""

from __future__ import annotations

import re
from pathlib import Path

import tomllib

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _REPO_ROOT / "pyproject.toml"
_APP_DOCKERFILE = _REPO_ROOT / "docker" / "Dockerfile"


def test_python_image_packages_and_validates_job_contracts() -> None:
    project = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    data_files = project["tool"]["setuptools"]["data-files"]
    patterns = {
        pattern
        for destination, destination_patterns in data_files.items()
        if destination == "contracts/jobs/v1"
        or destination.startswith("contracts/jobs/v1/")
        for pattern in destination_patterns
    }
    packaged = {
        path.relative_to(_REPO_ROOT)
        for pattern in patterns
        for path in _REPO_ROOT.glob(pattern)
    }
    expected = {
        path.relative_to(_REPO_ROOT)
        for path in (_REPO_ROOT / "contracts/jobs/v1").rglob("*")
        if path.is_file() and path.suffix in {".json", ".md"}
    }
    assert packaged == expected

    dockerfile = _APP_DOCKERFILE.read_text(encoding="utf-8")
    # The stage header is matched by its STAGE NAME, not by the base image.
    # CHAOS-4922 moved the base to an ARG (`FROM ${PYTHON_BASE_IMAGE} AS
    # runtime`) and this split silently produced an IndexError -- the test was
    # coupled to a detail it does not assert anything about. Anchoring on
    # `AS runtime` keeps it working across base-image changes, which is the only
    # thing that ever moves here.
    # Case-insensitive with optional leading whitespace: Dockerfile
    # instructions are case-insensitive and may be indented, so `from ... as
    # runtime` is a valid stage header this would otherwise miss (codex round 1).
    runtime = re.split(
        r"^\s*FROM\s+.*\s+AS\s+runtime\s*$", dockerfile, maxsplit=1, flags=re.M | re.I
    )[1]
    runtime = runtime.split("FROM runtime AS api", maxsplit=1)[0]
    assert "load_registry(); load_migration_jobs()" in runtime
