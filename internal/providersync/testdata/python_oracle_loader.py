"""Load live Python modules for checked-in Go parity oracles.

This loader is for dedicated, short-lived oracle subprocesses only.  It clears
all loaded ``dev_health_ops`` modules before each import so a caller cannot
substitute a forged cached package or module.  The source argument must resolve
to one of the five fixed production files below.
"""

from __future__ import annotations

import importlib
import io
import sys
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = (ROOT / "src").resolve(strict=True)


def _import_launchdarkly_processor() -> Any:
    return importlib.import_module("dev_health_ops.processors.launchdarkly")


def _import_linear_budget() -> Any:
    return importlib.import_module("dev_health_ops.providers.linear.budget")


def _import_jira_budget() -> Any:
    return importlib.import_module("dev_health_ops.providers.jira.budget")


def _import_launchdarkly_budget() -> Any:
    return importlib.import_module("dev_health_ops.providers.launchdarkly.budget")


def _import_dataset_adapters() -> Any:
    return importlib.import_module("dev_health_ops.processors.dataset_adapters")


ALLOWED_MODULES: dict[Path, tuple[str, Callable[[], Any]]] = {
    (SOURCE_ROOT / "dev_health_ops/processors/launchdarkly.py").resolve(strict=True): (
        "dev_health_ops.processors.launchdarkly",
        _import_launchdarkly_processor,
    ),
    (SOURCE_ROOT / "dev_health_ops/providers/linear/budget.py").resolve(strict=True): (
        "dev_health_ops.providers.linear.budget",
        _import_linear_budget,
    ),
    (SOURCE_ROOT / "dev_health_ops/providers/jira/budget.py").resolve(strict=True): (
        "dev_health_ops.providers.jira.budget",
        _import_jira_budget,
    ),
    (SOURCE_ROOT / "dev_health_ops/providers/launchdarkly/budget.py").resolve(
        strict=True
    ): (
        "dev_health_ops.providers.launchdarkly.budget",
        _import_launchdarkly_budget,
    ),
    (SOURCE_ROOT / "dev_health_ops/processors/dataset_adapters.py").resolve(
        strict=True
    ): ("dev_health_ops.processors.dataset_adapters", _import_dataset_adapters),
}


def _purge_dev_health_modules() -> None:
    """Remove cached project modules by key without inspecting hostile values."""
    for name in tuple(sys.modules):
        if name != "dev_health_ops" and not name.startswith("dev_health_ops."):
            continue
        sys.modules.pop(name, None)


def _prioritize_source_root() -> None:
    sys.path[:] = [
        entry for entry in sys.path if Path(entry or ".").resolve() != SOURCE_ROOT
    ]
    sys.path.insert(0, str(SOURCE_ROOT))


def load_live_module(source: Path) -> Any:
    """Import the canonical module allowlisted for ``source``."""
    expected = source.resolve(strict=True)
    allowed = ALLOWED_MODULES.get(expected)
    if allowed is None:
        raise ValueError(f"unexpected oracle source: {source}")
    module_name, module_loader = allowed

    _prioritize_source_root()
    _purge_dev_health_modules()
    importlib.invalidate_caches()
    # Production imports initialize observability in this test process.  Keep
    # that bootstrap chatter out of the JSON-only Go oracle protocol.
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        module = module_loader()
    spec = module.__spec__
    if spec is None or spec.origin is None:
        raise RuntimeError(f"oracle module {module_name} has no import origin")
    origin = Path(spec.origin).resolve(strict=True)
    if origin != expected:
        raise RuntimeError(f"oracle imported {origin}, expected {expected}")
    return module
