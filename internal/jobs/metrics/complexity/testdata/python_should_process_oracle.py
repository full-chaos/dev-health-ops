"""Oracle for ScanConfig.ShouldProcess: what Python's ComplexityScanner selects.

`should_process` (analytics/complexity.py) decides which files the complexity
family scans at all, using the include/exclude globs in
src/dev_health_ops/config/complexity.yaml. Those same globs gate provider
file-content ingestion, so a divergence changes which files EXIST to be
scanned, not merely which get scanned.

Reads a JSON array of paths on stdin; writes each path's verdict from the real
ComplexityScanner, loaded against the real config file. This is the ORACLE: it
instantiates the Python class rather than reimplementing its rules.
"""

from __future__ import annotations

import json
import sys

from dev_health_ops.analytics.complexity import (
    DEFAULT_COMPLEXITY_CONFIG_PATH,
    ComplexityScanner,
)


def main() -> int:
    paths = json.load(sys.stdin)
    scanner = ComplexityScanner(config_path=DEFAULT_COMPLEXITY_CONFIG_PATH)
    json.dump(
        {
            "config_path": str(DEFAULT_COMPLEXITY_CONFIG_PATH),
            "include_globs": list(scanner.include_globs),
            "exclude_globs": list(scanner.exclude_globs),
            "high_threshold": scanner.high_threshold,
            "very_high_threshold": scanner.very_high_threshold,
            "verdicts": [
                {"path": p, "should_process": scanner.should_process(p)} for p in paths
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
