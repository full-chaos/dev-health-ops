"""Cross-language contract: the Go PagerDuty bridge path must exist in FastAPI.

The Go stream consumer forwards every valid delivery to a fixed internal bridge
path. A path that FastAPI does not serve answers 404, which Go classifies as a
permanent rejection and quarantines+ACKs — total event loss. Changing either
side alone must therefore fail here.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PAGERDUTY_BRIDGE_PATH = "/api/internal/worker-operational/pagerduty"

# Every non-test Go source that names an operational bridge path. The PagerDuty
# endpoint is assembled from a base URL plus this literal suffix, so the literal
# is what both runtimes have to agree on.
_GO_SOURCES = (
    _REPO_ROOT / "internal" / "jobs" / "pagerduty" / "compatibility.go",
    _REPO_ROOT / "cmd" / "dev-health-stream-runner" / "dependencies.go",
    _REPO_ROOT / "cmd" / "dev-health-worker" / "operational.go",
)
_BRIDGE_PATH_PATTERN = re.compile(r'"(/api/internal/worker-operational/[a-z0-9-]+)"')


def _go_bridge_paths() -> set[str]:
    paths: set[str] = set()
    for source in _GO_SOURCES:
        assert source.exists(), f"{source} is a contract source and must exist"
        paths.update(_BRIDGE_PATH_PATTERN.findall(source.read_text(encoding="utf-8")))
    return paths


def _fastapi_paths() -> set[str]:
    from dev_health_ops.api.main import app

    return {getattr(route, "path", "") for route in app.routes}


def test_go_pagerduty_bridge_path_is_served_by_fastapi() -> None:
    go_paths = _go_bridge_paths()

    assert _PAGERDUTY_BRIDGE_PATH in go_paths, (
        "the Go PagerDuty stream consumer must post to " + _PAGERDUTY_BRIDGE_PATH
    )
    assert _PAGERDUTY_BRIDGE_PATH in _fastapi_paths(), (
        "FastAPI must serve " + _PAGERDUTY_BRIDGE_PATH
    )


def test_every_go_operational_bridge_path_is_served_by_fastapi() -> None:
    unserved = _go_bridge_paths() - _fastapi_paths()

    assert unserved == set(), f"Go posts to unserved bridge paths: {sorted(unserved)}"
