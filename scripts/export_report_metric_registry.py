"""Export the Python report metric registry for language-neutral consumers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dev_health_ops.reports.metric_registry import METRIC_REGISTRY

DEFAULT_OUTPUT = Path("internal/jobs/report/metric_registry.json")


def build_payload() -> dict[str, Any]:
    """Return the complete, deterministically ordered report metric registry."""
    return {
        "schema_version": 1,
        "metrics": [
            {
                "canonical_name": definition.canonical_name,
                "display_name": definition.display_name,
                "unit": definition.unit,
                "dimensions": list(definition.dimensions),
                "source_table": definition.source_table,
            }
            for _, definition in sorted(METRIC_REGISTRY.items())
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    args.output.write_text(
        json.dumps(build_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
