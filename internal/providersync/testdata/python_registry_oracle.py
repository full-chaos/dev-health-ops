#!/usr/bin/env python3
"""Emit the live Python dataset registry as stable JSON for Go parity tests."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    source = pathlib.Path(sys.argv[1]).resolve()
    spec = importlib.util.spec_from_file_location("dev_health_dataset_registry", source)
    if spec is None or spec.loader is None:
        return 2
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    registry: dict[str, list[dict[str, object]]] = {}
    for provider in ("github", "gitlab", "jira", "linear", "launchdarkly"):
        registry[provider] = [
            {
                "provider": item.provider,
                "dataset": item.dataset_key,
                "cost_class": item.default_cost_class.value,
                "watermark": item.watermark_behavior.value,
                "legacy_targets": sorted(item.legacy_targets),
                "processor_flags": dict(sorted(item.processor_flags.items())),
            }
            for item in module.supported_datasets(provider)
        ]
    json.dump(registry, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
