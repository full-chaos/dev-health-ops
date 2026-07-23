#!/usr/bin/env python3
"""Adversarial subprocess probe for the parity-oracle module cache boundary."""

from __future__ import annotations

import json
import pathlib
import sys
from types import ModuleType
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from internal.providersync.testdata.python_oracle_loader import (  # noqa: E402
    load_live_module,
)

HOSTILE_PRELOAD_NAMES = (
    # Every parent package installed by _install_namespace.
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
    # Every fixed dependency-free source module.
    "dev_health_ops.providers.usage",
    "dev_health_ops.sync.budget_types",
    "dev_health_ops.sync.datasets",
    # Every remaining stub module installed by an oracle target.
    "dev_health_ops.credentials.resolver",
    "dev_health_ops.metrics.schemas",
    "dev_health_ops.metrics.sinks.ingestion",
    "dev_health_ops.workers.async_runner",
    "dev_health_ops.workers.sync_bootstrap",
    # The target selected by this probe.
    "dev_health_ops.processors.dataset_adapters",
)


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    source = pathlib.Path(sys.argv[1]).resolve(strict=True)
    touches: list[str] = []

    class HostileModule(ModuleType):
        def __getattr__(self, name: str) -> Any:
            touches.append(f"{self.__name__}.{name}")
            return object()

    preloads = {name: HostileModule(name) for name in HOSTILE_PRELOAD_NAMES}
    forged_target = preloads["dev_health_ops.processors.dataset_adapters"]
    forged_target.__file__ = str(source)
    sys.modules.update(preloads)

    module = load_live_module(source)
    spec = module.__spec__
    if spec is None or spec.origin is None:
        raise AssertionError("canonical module has no import origin")
    origin = pathlib.Path(spec.origin).resolve(strict=True)
    reused = sorted(
        name for name, preload in preloads.items() if sys.modules.get(name) is preload
    )
    if module is forged_target or origin != source or touches or reused:
        raise AssertionError(
            f"unsafe module resolution: forged={module is forged_target}, "
            f"origin={origin}, touches={touches}, reused={reused}"
        )
    print(
        json.dumps(
            {
                "canonical": True,
                "hostile_touches": touches,
                "origin": str(origin),
                "preloaded_modules": sorted(preloads),
                "reused_modules": reused,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
