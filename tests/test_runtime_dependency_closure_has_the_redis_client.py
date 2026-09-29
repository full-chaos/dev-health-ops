"""The locked RUNTIME closure of dev-health-ops includes the `redis` client.

api/middleware/rate_limit.py builds `limits` with a `redis://` storage URI
whenever REDIS_URL is set, and `limits` imports the `redis` package for that
scheme (limits/storage/redis.py raises ConfigurationError "'redis' prerequisite
not available" at Limiter construction otherwise -- i.e. at API boot).

`redis` used to reach the runtime closure only through `celery[redis]`. When the
Celery app was removed (CHAOS-7059) it stayed in uv.lock, pulled by `fakeredis`
alone -- a DEV extra -- so every environment that installs runtime dependencies
without dev extras (the api image) lost it while every developer venv kept it:
the tests all passed and the container crashed at boot.

So this test resolves the closure from uv.lock the way an install without dev
extras does (base `dependencies` of the project, following each dependency's
requested extras) and asserts `redis` is in it. It cannot be satisfied by a dev
extra.
"""

from __future__ import annotations

from pathlib import Path

import tomllib

_ROOT = Path(__file__).resolve().parents[1]


def _runtime_closure(lock: dict, project: str) -> set[str]:
    packages = {package["name"]: package for package in lock["package"]}
    seen: set[str] = set()
    stack: list[tuple[str, tuple[str, ...]]] = [
        (dep["name"], tuple(dep.get("extra", ())))
        for dep in packages[project]["dependencies"]
    ]
    while stack:
        name, extras = stack.pop()
        key = f"{name}[{','.join(sorted(extras))}]"
        if key in seen:
            continue
        seen.add(key)
        package = packages[name]
        edges = list(package.get("dependencies", ()))
        for extra in extras:
            edges += package.get("optional-dependencies", {}).get(extra, ())
        for dep in edges:
            stack.append((dep["name"], tuple(dep.get("extra", ()))))
    return {entry.split("[", 1)[0] for entry in seen}


def test_locked_runtime_closure_includes_redis() -> None:
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    closure = _runtime_closure(lock, "dev-health-ops")
    assert "redis" in closure, (
        "redis is not in the locked runtime closure of dev-health-ops: "
        "api/middleware/rate_limit.py uses a redis:// limits storage, and "
        "limits imports `redis` for it, so an install without dev extras "
        "crashes the API at boot. Declare `limits[redis]` (or `redis`) in "
        "[project].dependencies and re-lock."
    )
    # The lock walk must be able to see a dependency at all: a dev-only
    # package is NOT in the closure (guards the test against vacuity).
    assert "fakeredis" not in closure


def test_the_walk_does_not_count_dev_extras() -> None:
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    project = next(p for p in lock["package"] if p["name"] == "dev-health-ops")
    dev_only = {
        dep["name"] for dep in project.get("optional-dependencies", {}).get("dev", ())
    } - {dep["name"] for dep in project["dependencies"]}
    assert dev_only, "expected dev-only dependencies in uv.lock"
    closure = _runtime_closure(lock, "dev-health-ops")
    assert "fakeredis" in dev_only and "fakeredis" not in closure
