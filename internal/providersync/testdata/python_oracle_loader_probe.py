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


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    source = pathlib.Path(sys.argv[1]).resolve(strict=True)
    touches: list[str] = []

    class HostileModule(ModuleType):
        def __getattr__(self, name: str) -> Any:
            touches.append(name)
            return object()

    forged_target = ModuleType("dev_health_ops.processors.dataset_adapters")
    forged_target.__file__ = str(source)
    sys.modules["dev_health_ops"] = HostileModule("dev_health_ops")
    sys.modules["dev_health_ops.processors"] = HostileModule(
        "dev_health_ops.processors"
    )
    sys.modules["dev_health_ops.processors.dataset_adapters"] = forged_target

    module = load_live_module(source)
    spec = module.__spec__
    if spec is None or spec.origin is None:
        raise AssertionError("canonical module has no import origin")
    origin = pathlib.Path(spec.origin).resolve(strict=True)
    if module is forged_target or origin != source or touches:
        raise AssertionError(
            f"unsafe module resolution: forged={module is forged_target}, "
            f"origin={origin}, touches={touches}"
        )
    print(
        json.dumps(
            {"canonical": True, "hostile_touches": touches, "origin": str(origin)},
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
