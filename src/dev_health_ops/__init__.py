from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dev-health-ops")
except PackageNotFoundError:
    __version__ = "dev"

__all__ = ["__version__"]
