from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import tomllib
import yaml

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
REQUIREMENTS = ROOT / "ci" / "requirements-live-python-oracles.txt"
LOCKFILE = ROOT / "uv.lock"


def _canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _locked_packages() -> dict[str, dict[str, Any]]:
    lock = tomllib.loads(LOCKFILE.read_text(encoding="utf-8"))
    return {
        _canonical_name(str(package["name"])): package for package in lock["package"]
    }


def _locked_closure(root: str) -> dict[str, str]:
    packages = _locked_packages()
    pending = [_canonical_name(root)]
    closure: dict[str, str] = {}
    while pending:
        name = pending.pop()
        if name in closure:
            continue
        package = packages[name]
        closure[name] = str(package["version"])
        pending.extend(
            _canonical_name(str(dependency["name"]))
            for dependency in package.get("dependencies", [])
        )
    return closure


def _pinned_requirements() -> dict[str, str]:
    pins: dict[str, str] = {}
    for raw_line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, version = line.partition("==")
        assert separator, f"live-oracle requirement must be exactly pinned: {line}"
        canonical_name = _canonical_name(name)
        assert canonical_name not in pins, (
            f"duplicate live-oracle requirement: {canonical_name}"
        )
        pins[canonical_name] = version
    return pins


def test_go_quality_bootstraps_locked_live_oracle_dependencies() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["go-quality"]["steps"]
    commands = [str(step.get("run", "")).strip() for step in steps]

    install = (
        "python -m pip install --no-deps -r ci/requirements-live-python-oracles.txt"
    )
    quality_gate = "bash ci/check_go.sh all"
    assert install in commands
    assert commands.index(install) < commands.index(quality_gate)

    pins = _pinned_requirements()
    locked = _locked_packages()
    for package, version in pins.items():
        assert str(locked[package]["version"]) == version, (
            f"live-oracle pin for {package} must match uv.lock"
        )
    for root in ("httpx", "pygithub", "croniter"):
        for package, version in _locked_closure(root).items():
            assert pins.get(package) == version, (
                f"live Python oracles require locked {package}=={version}"
            )


def test_sync_coverage_oracle_import_does_not_bootstrap_worker_runtime() -> None:
    script = textwrap.dedent(
        """
        import builtins

        real_import = builtins.__import__

        def reject_worker_runtime(name, *args, **kwargs):
            if name == "dev_health_ops.sync.planner" or name.startswith(
                "dev_health_ops.workers"
            ):
                raise AssertionError(f"coverage import initialized worker runtime: {name}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = reject_worker_runtime
        import dev_health_ops.api.services.sync_coverage  # noqa: F401
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
