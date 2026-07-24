"""Load a named live Python module for a checked-in Go parity oracle.

The Go tests pass source paths so a failure identifies the production contract
being compared.  Those paths must not become an arbitrary-code input: each
oracle admits only its fixed source file, then imports that known module from
this checkout through Python's normal import machinery.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _module_origins(module: ModuleType) -> set[Path]:
    origins: set[Path] = set()
    module_file = getattr(module, "__file__", None)
    if module_file:
        origins.add(Path(module_file).resolve())
    module_path = getattr(module, "__path__", ())
    origins.update(Path(path).resolve() for path in module_path)
    return origins


def _reject_conflicting_preloads(source_root: Path) -> None:
    for name, module in tuple(sys.modules.items()):
        if name != "dev_health_ops" and not name.startswith("dev_health_ops."):
            continue
        if module is None:
            raise RuntimeError(f"preloaded module {name} has no module object")
        origins = _module_origins(module)
        if not origins or any(
            not _is_within(origin, source_root) for origin in origins
        ):
            raise RuntimeError(
                f"preloaded module {name} is outside checked-out source {source_root}"
            )


def _prioritize_source_root(source_root: Path) -> None:
    canonical = str(source_root)
    sys.path[:] = [
        entry for entry in sys.path if Path(entry or ".").resolve() != source_root
    ]
    sys.path.insert(0, canonical)


def _verified_module_path(module: ModuleType, expected: Path) -> None:
    module_path = Path(getattr(module, "__file__", "")).resolve()
    if module_path != expected:
        raise RuntimeError(
            f"oracle imported {module_path}, expected checked-out source {expected}"
        )


def load_live_module(source: Path, *, relative_path: str, module_name: str) -> Any:
    """Import the allowlisted production module named by a parity oracle."""
    source_root = (ROOT / "src").resolve()
    expected = (ROOT / relative_path).resolve(strict=True)
    if not _is_within(expected, source_root):
        raise ValueError(f"oracle source is outside checked-out source: {expected}")
    if source.resolve(strict=True) != expected:
        raise ValueError(f"unexpected oracle source: {source}")

    _prioritize_source_root(source_root)
    _reject_conflicting_preloads(source_root)

    existing = sys.modules.get(module_name)
    if existing is not None:
        _verified_module_path(existing, expected)
        return existing

    spec = importlib.util.spec_from_file_location(module_name, expected)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load oracle module from {expected}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    # Production imports initialize observability in this test process.  Keep
    # that bootstrap chatter out of the JSON-only Go oracle protocol.
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    _verified_module_path(module, expected)
    return module
