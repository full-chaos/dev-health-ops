"""Load fixed live Python sources for checked-in Go parity oracles.

The Go quality job intentionally has only a stock Python interpreter.  These
oracles must execute the production functions they compare, but importing a
``dev_health_ops`` package normally also runs unrelated application initializers
that require the full service dependency set (including SQLAlchemy).

This loader therefore creates an isolated project namespace and executes only
the five fixed oracle sources, plus three dependency-free support modules.  No
caller-controlled module name or source path is ever imported.  The target
source itself remains live: editing a production function changes oracle output.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = (ROOT / "src").resolve(strict=True)


def _source(relative_path: str) -> Path:
    """Resolve one checked-in source path once, while building the allowlist."""
    return (SOURCE_ROOT / relative_path).resolve(strict=True)


_BUDGET_TYPES_SOURCE = _source("dev_health_ops/sync/budget_types.py")
_DATASETS_SOURCE = _source("dev_health_ops/sync/datasets.py")
_USAGE_SOURCE = _source("dev_health_ops/providers/usage.py")

_SAFE_SOURCE_MODULES: dict[str, Path] = {
    "dev_health_ops.sync.budget_types": _BUDGET_TYPES_SOURCE,
    "dev_health_ops.sync.datasets": _DATASETS_SOURCE,
    "dev_health_ops.providers.usage": _USAGE_SOURCE,
}


def _target_launchdarkly_processor() -> None:
    _install_module(
        "dev_health_ops.metrics.schemas",
        {"FeatureFlagEventRecord": object, "FeatureFlagRecord": object},
    )


def _target_budget() -> None:
    _load_safe_source("dev_health_ops.sync.budget_types")
    _load_safe_source("dev_health_ops.sync.datasets")
    _load_safe_source("dev_health_ops.providers.usage")
    _install_module(
        "dev_health_ops.workers.sync_bootstrap", {"SyncTaskContext": object}
    )


def _unsupported_dependency(*_args: Any, **_kwargs: Any) -> Any:
    """Fail loudly if an oracle starts exercising an intentionally absent path."""
    raise RuntimeError("parity oracle attempted an unsupported application dependency")


def _target_dataset_adapters() -> None:
    _load_safe_source("dev_health_ops.sync.datasets")
    _install_module(
        "dev_health_ops.credentials.resolver",
        {
            "github_credentials_from_mapping": _unsupported_dependency,
            "gitlab_credentials_from_mapping": _unsupported_dependency,
            "jira_credentials_from_mapping": _unsupported_dependency,
            "pagerduty_credentials_from_mapping": _unsupported_dependency,
            "resolve_gitlab_url": _unsupported_dependency,
        },
    )
    _install_module("dev_health_ops.metrics.sinks.ingestion", {"IngestionSink": object})
    _install_module(
        "dev_health_ops.providers.usage",
        {
            "PROVIDER_USAGE_OBSERVATION_KEY": "provider_usage_observations",
            "attach_partial_observations": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.storage",
        {
            "resolve_db_type": _unsupported_dependency,
            "run_with_store": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.workers.async_runner", {"run_async": _unsupported_dependency}
    )
    _install_module(
        "dev_health_ops.workers.sync_bootstrap",
        {"ProviderRuntime": object, "SyncTaskContext": object},
    )


ALLOWED_MODULES: dict[Path, tuple[str, Callable[[], None]]] = {
    _source("dev_health_ops/processors/launchdarkly.py"): (
        "dev_health_ops.processors.launchdarkly",
        _target_launchdarkly_processor,
    ),
    _source("dev_health_ops/providers/linear/budget.py"): (
        "dev_health_ops.providers.linear.budget",
        _target_budget,
    ),
    _source("dev_health_ops/providers/jira/budget.py"): (
        "dev_health_ops.providers.jira.budget",
        _target_budget,
    ),
    _source("dev_health_ops/providers/launchdarkly/budget.py"): (
        "dev_health_ops.providers.launchdarkly.budget",
        _target_budget,
    ),
    _source("dev_health_ops/processors/dataset_adapters.py"): (
        "dev_health_ops.processors.dataset_adapters",
        _target_dataset_adapters,
    ),
}


def _purge_dev_health_modules() -> None:
    """Remove cached project modules by key without inspecting hostile values."""
    for name in tuple(sys.modules):
        if name == "dev_health_ops" or name.startswith("dev_health_ops."):
            sys.modules.pop(name, None)


def _install_package(name: str) -> ModuleType:
    module = ModuleType(name)
    module.__package__ = name
    package_path = SOURCE_ROOT.joinpath(*name.split(".")).resolve(strict=True)
    module.__path__ = [str(package_path)]
    _register_module(name, module)
    return module


def _register_module(name: str, module: ModuleType) -> None:
    sys.modules[name] = module
    parent_name, _, child_name = name.rpartition(".")
    if parent_name:
        parent = sys.modules[parent_name]
        setattr(parent, child_name, module)


def _install_namespace() -> None:
    for name in (
        "dev_health_ops",
        "dev_health_ops.credentials",
        "dev_health_ops.metrics",
        "dev_health_ops.metrics.sinks",
        "dev_health_ops.processors",
        "dev_health_ops.providers",
        "dev_health_ops.providers.jira",
        "dev_health_ops.providers.launchdarkly",
        "dev_health_ops.providers.linear",
        "dev_health_ops.storage",
        "dev_health_ops.sync",
        "dev_health_ops.workers",
    ):
        _install_package(name)


def _install_module(name: str, values: dict[str, Any]) -> ModuleType:
    module = ModuleType(name)
    module.__package__ = name.rpartition(".")[0]
    module.__dict__.update(values)
    _register_module(name, module)
    return module


def _load_source_module(name: str, source: Path) -> ModuleType:
    """Execute one fixed, resolved source file under its fixed module name."""
    spec = importlib.util.spec_from_file_location(name, source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load fixed oracle module {name}")
    module = importlib.util.module_from_spec(spec)
    _register_module(name, module)
    spec.loader.exec_module(module)
    return module


def _load_safe_source(name: str) -> ModuleType:
    source = _SAFE_SOURCE_MODULES[name]
    return _load_source_module(name, source)


def load_live_module(source: Path) -> Any:
    """Execute the canonical production module allowlisted for ``source``."""
    expected = source.resolve(strict=True)
    allowed = ALLOWED_MODULES.get(expected)
    if allowed is None:
        raise ValueError(f"unexpected oracle source: {source}")
    module_name, configure = allowed

    _purge_dev_health_modules()
    _install_namespace()
    importlib.invalidate_caches()
    configure()
    module = _load_source_module(module_name, expected)
    spec = module.__spec__
    if spec is None or spec.origin is None:
        raise RuntimeError(f"oracle module {module_name} has no import origin")
    origin = Path(spec.origin).resolve(strict=True)
    if origin != expected:
        raise RuntimeError(f"oracle imported {origin}, expected {expected}")
    return module
