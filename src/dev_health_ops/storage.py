from __future__ import annotations

import importlib.util
import sys
import warnings
from pathlib import Path


warnings.warn(
    "dev_health_ops.storage is now a package; import from dev_health_ops.storage instead.",
    DeprecationWarning,
    stacklevel=2,
)

_package_init = Path(__file__).resolve().parent / "storage" / "__init__.py"
spec = importlib.util.spec_from_file_location(
    "dev_health_ops._storage_pkg", _package_init
)
if spec is None or spec.loader is None:
    raise ImportError("Could not load dev_health_ops.storage package")

module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

__all__ = getattr(module, "__all__", [])
globals().update({name: getattr(module, name) for name in __all__})
