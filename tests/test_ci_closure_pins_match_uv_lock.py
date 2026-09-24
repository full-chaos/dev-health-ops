"""Every exact pin in a `ci/requirements-*.txt` closure equals uv.lock's version.

The CI closures are installed with `pip install --no-deps`, so each file is a
hand-kept copy of a slice of `uv.lock`. Nothing ties the copy to the lock, so
the two drifted (the ClickHouse-migrations closure kept starlette 1.3.1 after the
lock moved to the deployed 1.7.0, and eighteen more pins fell behind), and a job
that installs a closure then tests against dependencies the project no longer
ships. This reads every closure file in `ci/` and compares each pin with the
lock, so a new closure file is covered without editing this test.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib

_ROOT = Path(__file__).resolve().parents[1]
_CLOSURES = sorted((_ROOT / "ci").glob("requirements-*.txt"))
_LOCK = _ROOT / "uv.lock"


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _locked_versions() -> dict[str, str]:
    lock = tomllib.loads(_LOCK.read_text(encoding="utf-8"))
    return {
        _canonical(str(package["name"])): str(package["version"])
        for package in lock["package"]
        if "version" in package
    }


def _pins(path: Path) -> list[tuple[str, str]]:
    pins: list[tuple[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, version = line.partition("==")
        assert separator, f"{path.name}: requirement must be exactly pinned: {line}"
        pins.append((_canonical(name), version))
    return pins


def test_ci_has_closure_files_to_check() -> None:
    assert _CLOSURES, "no ci/requirements-*.txt found; the guard would pass vacuously"


@pytest.mark.parametrize("closure", _CLOSURES, ids=lambda path: path.name)
def test_closure_pins_equal_uv_lock(closure: Path) -> None:
    locked = _locked_versions()
    pins = _pins(closure)
    assert pins, f"{closure.name} pins nothing"

    names = [name for name, _ in pins]
    assert len(names) == len(set(names)), f"{closure.name} pins a package twice"

    unlocked = sorted(name for name, _ in pins if name not in locked)
    assert not unlocked, (
        f"{closure.name} pins packages uv.lock does not have: {unlocked}"
    )

    drifted = sorted(
        f"{name}=={version} (uv.lock has {locked[name]})"
        for name, version in pins
        if locked[name] != version
    )
    assert not drifted, f"{closure.name} drifts from uv.lock: {drifted}"
