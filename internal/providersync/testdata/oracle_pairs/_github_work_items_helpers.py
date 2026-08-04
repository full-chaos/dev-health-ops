"""Shared case decoding for live GitHub work-item row oracle pairs."""

from __future__ import annotations

import pathlib
import sys
import types
from datetime import datetime
from types import SimpleNamespace
from typing import Any


def install_minimal_oracle_imports() -> None:
    """Load the normalizer without unrelated API/client runtime dependencies.

    The Go quality image intentionally contains only the Python standard
    library plus the few dependencies used directly by provider code.  The
    aggregate ``dev_health_ops.models`` initializer imports API licensing,
    which in turn imports FastAPI even though these row oracles only need the
    standalone work-item and AI-attribution dataclasses.  The normalizer also
    imports five client Protocols for annotations; importing their concrete
    module would pull transport/auth dependencies into a row-only oracle.

    Installing a normal namespace package for the model directory and a typed
    annotation-only client module preserves the real production model and
    normalizer modules/functions.  Nothing involved in producing row values is
    replaced.
    """
    if "dev_health_ops.models" in sys.modules:
        return
    models_dir = (
        pathlib.Path(__file__).resolve().parents[4] / "src/dev_health_ops/models"
    )
    package = types.ModuleType("dev_health_ops.models")
    package.__path__ = [str(models_dir)]
    package.__package__ = "dev_health_ops.models"
    sys.modules[package.__name__] = package

    client_name = "dev_health_ops.providers.github.client"
    if client_name not in sys.modules:
        client = types.ModuleType(client_name)
        for name in (
            "_GitHubCommentLike",
            "_GitHubEventLike",
            "_GitHubIssueLike",
            "_GitHubMilestoneLike",
            "_GitHubPullRequestLike",
        ):
            setattr(client, name, type(name, (), {}))
        sys.modules[client_name] = client


def object_from_case(value: Any) -> Any:
    """Build the PyGithub-shaped object consumed by production normalizers."""
    if isinstance(value, dict):
        return SimpleNamespace(
            **{
                key: (
                    datetime.fromisoformat(item.replace("Z", "+00:00"))
                    if key.endswith("_at") and isinstance(item, str)
                    else object_from_case(item)
                )
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return [object_from_case(item) for item in value]
    return value
