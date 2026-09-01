#!/usr/bin/env python3
"""CHAOS-4761 discovery: every ops REST + GraphQL surface, taken from the
SERVED application and schema objects rather than from source text.

Why this file no longer parses source
-------------------------------------
The previous implementation found surfaces by matching decorators and
``include_router(...)`` calls with regexes and then re-deriving mount paths
by walking the include graph. That set is only ever as wide as the list of
patterns someone remembered to write down, and the inventory it was
cross-checked against was built from the same patterns -- so the two shared
a blind spot and agreeing with each other proved nothing about what neither
looked at. Three measured consequences on the tree this replaces:

  * three ``@strawberry.subscription`` resolvers were mounted on the served
    schema and matched no pattern, so they were neither discovered nor
    profiled (CHAOS-4761);
  * a router mounted at more than one prefix collapsed to the first
    resolvable parent chain, so the second mount was never checked
    (CHAOS-4760);
  * two ``@strawberry.field`` examples inside ``require_permission``'s
    DOCSTRING (``api/graphql/authz.py:25``, ``:30``) were discovered as
    real GraphQL surfaces and profiled as if they existed.

``app.routes`` and ``strawberry.Schema`` are what the frameworks will
actually serve. A surface cannot be added in a way that escapes them --
including via a dynamic or computed ``include_router``, which the old
static walk had to fail closed on (the retired ``UNVERIFIED ROUTE``
allowlist). The set comes from the object; the ``file:line`` anchor comes
from ``inspect`` on the object's own endpoint/resolver function, so it is
still a real source location and not a guess.

The trade
---------
Introspection has to IMPORT the application, which is heavier than parsing
(~17s for ``api.main`` on a warm checkout) and can fail for import-time
reasons. That failure is loud: ``discover()`` raises, and
``ci/check_endpoint_profiles.py`` turns the raise into a non-zero exit --
never a skipped check that reports success. Import-time telemetry side
effects are suppressed via ``OTEL_ENABLED=false`` before the import.

Known limitations (documented, never silently swallowed)
--------------------------------------------------------
  * The set is the set THIS PROCESS'S environment produces. A router mounted
    only under some runtime configuration is discovered only when that
    configuration is present. On this tree every ``include_router`` reachable
    from either app root is unconditional, and the optional
    ``prometheus-fastapi-instrumentator`` ``/metrics`` mount is backed by a
    hard dependency in ``pyproject.toml`` -- but that is a property of the
    tree, not a guarantee of the mechanism, so any route whose presence is
    configuration-dependent must say so in its inventory row's ``gaps``.
  * A router that is never mounted on either app root is NOT a surface and is
    deliberately not reported. The old walk reported it as
    ``UNMOUNTED_ROUTER``; an unmounted route serves no request.
  * ``strawberry.Schema._schema`` (the underlying ``GraphQLSchema``) is a
    private attribute; it is the only way to enumerate EVERY type's fields
    rather than just the three root types. Its absence raises rather than
    degrading to a narrower set.

Usage:
    python3 ci/discover_ops_routes.py [--root PATH] [--out PATH]
Prints a JSON report to stdout (or --out):
    {"apps": [...], "routes": [...], "graphql": [...],
     "counts": {"routes": N, "graphql": N}}
"""

from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# Every deployed FastAPI application whose surfaces this inventory covers, as
# (module, attribute) of the module-level app object. ``app_root`` in the
# output is ``"<module>::<attribute>"`` -- the same key shape
# ci/check_endpoint_profiles.py maps to a `service` enum value via
# _APP_ROOT_SERVICE, so adding a newly deployed app means adding it in BOTH
# places and a mapping miss is a hard failure there, never a silent pass.
DEPLOYED_APPS: tuple[tuple[str, str], ...] = (
    ("dev_health_ops.api.main", "app"),
    ("dev_health_ops.api.billing_edge", "app"),
)

# The served GraphQL schema object, as (module, attribute).
GRAPHQL_SCHEMA = ("dev_health_ops.api.graphql.schema", "schema")

# A websocket route has no HTTP method list. It is still a distinct served
# surface with its own authentication path, so it gets an explicit pseudo-verb
# rather than being dropped or silently folded into the GET row for the same
# path (``/graphql`` is served by all three).
WEBSOCKET_METHOD = "WEBSOCKET"


class DiscoveryImportError(RuntimeError):
    """Raised when a deployed app or the GraphQL schema cannot be imported.

    Never caught inside this module: a discovery run that cannot see the
    application must fail loudly, because the alternative -- an empty or
    partial surface set -- makes every downstream cross-check pass while
    checking nothing.
    """


PACKAGE = "dev_health_ops"


def _loaded_from(module: Any, src: Path) -> bool:
    file = getattr(module, "__file__", None)
    if not file:
        return False
    try:
        return Path(file).resolve().is_relative_to(src)
    except (OSError, ValueError):
        return False


@contextmanager
def _import_context(root: Path):
    """Import ``<root>/src``'s package, and be sure it is THAT root's.

    Two things this has to get right:

    * ``dev_health_ops.api.main`` calls ``init_tracing()`` at module scope,
      which starts OTLP exporters and retries against ``localhost:4317``.
      ``OTEL_ENABLED`` is read inside ``init_tracing``, so it has to be set
      BEFORE the import. Set, not overridden: an environment that has
      deliberately chosen a value keeps it.
    * ``import`` returns whatever is already in ``sys.modules``. Without the
      purge below, a discovery run against one root would silently enumerate
      an ALREADY-IMPORTED app from a different root and report that set as
      this root's -- a measurement quietly taken on the wrong input, which is
      worse than no measurement. Anything cached from elsewhere is dropped on
      the way in and put back on the way out, so a caller that discovers
      against several roots in one process (the gate's own contract tests do)
      gets each root's real set and leaves no cross-contamination behind.
    """
    os.environ.setdefault("OTEL_ENABLED", "false")
    src = (root / "src").resolve()
    saved_path = list(sys.path)
    cached = {
        name: mod
        for name, mod in sys.modules.items()
        if name == PACKAGE or name.startswith(PACKAGE + ".")
    }
    foreign = {n for n, m in cached.items() if not _loaded_from(m, src)}
    for name in foreign:
        del sys.modules[name]
    sys.path.insert(0, str(src))
    try:
        yield
    finally:
        sys.path[:] = saved_path
        if foreign:
            for name in [
                n for n in sys.modules if n == PACKAGE or n.startswith(PACKAGE + ".")
            ]:
                del sys.modules[name]
            sys.modules.update(cached)


def _import_attr(module: str, attr: str, src: Path) -> Any:
    try:
        mod = __import__(module, fromlist=[attr])
    except Exception as exc:  # noqa: BLE001 -- re-raised as a typed failure
        raise DiscoveryImportError(
            f"could not import {module!r} to enumerate its served surfaces: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    # Purging sys.modules is not enough on its own: `<root>/src` is only
    # PREPENDED to sys.path, so if the requested root does not contain the
    # package, the import falls through to whatever other checkout is on the
    # path and returns ITS app. That is a measurement silently taken on the
    # wrong input, which is worse than no measurement -- a set enumerated from
    # a different tree would be cross-checked against this tree's inventory
    # and the mismatch read as inventory drift. Verified, not assumed.
    if not _loaded_from(mod, src):
        raise DiscoveryImportError(
            f"{module!r} resolved to {getattr(mod, '__file__', None)!r}, which "
            f"is not under {src} -- discovery would be enumerating a different "
            "checkout's application than the root it was asked about"
        )
    try:
        return getattr(mod, attr)
    except AttributeError as exc:
        raise DiscoveryImportError(
            f"{module!r} has no attribute {attr!r} -- the deployed app/schema "
            "object this discovery is anchored to has moved or been renamed"
        ) from exc


def _anchor(func: Any, root: Path) -> tuple[str | None, int | None, bool]:
    """Repo-relative ``(file, line, is_ops_source)`` for a callable.

    ``inspect.getsourcelines`` returns the block STARTING AT THE FIRST
    DECORATOR for a decorated function, which is exactly the line the
    checked-in inventory anchors REST routes and GraphQL resolvers to --
    verified against the whole intersection of the live set and the frozen
    inventory (294 REST rows, 56 GraphQL rows, zero disagreements) when this
    replaced the source walk.
    """
    if func is None:
        return None, None, False
    try:
        target = inspect.unwrap(func)
        source_file = inspect.getsourcefile(target)
        _, line = inspect.getsourcelines(target)
    except (OSError, TypeError):
        return None, None, False
    if not source_file:
        return None, None, False
    resolved = Path(source_file).resolve()
    # "ops source" means the package tree, NOT merely "somewhere under the
    # repository root": a virtualenv lives under the root too, so a
    # root-relative test would classify fastapi's own /docs endpoint as ops
    # source and anchor a row at `.venv/lib/.../fastapi/applications.py` --
    # a path that is not in git and that no reviewer can check.
    package_root = (root / "src" / "dev_health_ops").resolve()
    if resolved.is_relative_to(package_root):
        return str(resolved.relative_to(root.resolve())), line, True
    # A framework-provided endpoint (fastapi's own /docs and /openapi.json,
    # strawberry's GraphQL router, the prometheus instrumentator's /metrics).
    # Reported as external so the checker can demand explicit provenance for
    # the row instead of anchoring it somewhere unreviewable.
    return str(resolved), line, False


def _walk_routes(routes: Any, prefix: str = "") -> list[tuple[str, Any]]:
    """Every leaf route under ``routes``, with its FULLY RESOLVED path.

    Recurses through ``Mount`` (a sub-application or a static-files mount),
    which carries its own child routes whose ``path`` is relative to the mount
    point. A router mounted twice appears as two distinct leaf entries with two
    distinct paths, which is what closes CHAOS-4760: the mount multiplicity is
    a property of the object graph, never of prefix arithmetic done here.
    """
    from starlette.routing import Mount  # imported late: needs sys.path set up

    out: list[tuple[str, Any]] = []
    for route in routes:
        path = getattr(route, "path", None)
        if path is None:
            # fastapi >= 0.137 leaves `_IncludedRouter` marker entries with no
            # `.path` in app.routes (see the fastapi pin's comment in
            # pyproject.toml). Reported, never silently skipped.
            out.append((f"<NO PATH: {type(route).__name__}>", route))
            continue
        full = prefix + path
        if isinstance(route, Mount):
            out.extend(_walk_routes(getattr(route, "routes", []) or [], full))
            continue
        out.append((full, route))
    return out


def discover_routes(root: Path, apps=DEPLOYED_APPS) -> list[dict]:
    """Every REST/websocket surface both deployed apps actually serve."""
    records: list[dict] = []
    src = (root / "src").resolve()
    for module, attr in apps:
        app = _import_attr(module, attr, src)
        app_root = f"{module}::{attr}"
        for path, route in _walk_routes(getattr(app, "routes", []) or []):
            endpoint = getattr(route, "endpoint", None)
            file, line, in_ops = _anchor(endpoint, root)
            methods = sorted(getattr(route, "methods", None) or [])
            if not methods:
                methods = [WEBSOCKET_METHOD]
            records.append(
                {
                    "app_root": app_root,
                    # A single string so a row's `method` field can name the
                    # route object's whole served verb set exactly. The old
                    # discovery reported the literal "API_ROUTE" for any
                    # @app.api_route(...) registration, which named a runtime
                    # method list without saying what was in it.
                    "method": ",".join(methods),
                    "methods": methods,
                    "path": path,
                    "file": file,
                    "line": line,
                    "endpoint_in_ops_source": in_ops,
                    "endpoint_module": getattr(endpoint, "__module__", None),
                    "endpoint_name": getattr(endpoint, "__name__", None),
                    "route_class": type(route).__name__,
                }
            )
    return records


def _graphql_root_kinds(gql_schema: Any) -> dict[str, str]:
    kinds = {}
    for attr, kind in (
        ("query_type", "graphql_field"),
        ("mutation_type", "graphql_mutation"),
        ("subscription_type", "graphql_subscription"),
    ):
        root = getattr(gql_schema, attr, None)
        if root is not None:
            kinds[root.name] = kind
    return kinds


def discover_graphql(root: Path, graphql_schema=GRAPHQL_SCHEMA) -> list[dict]:
    """Every field on the served schema that has a resolver.

    Root Query/Mutation/Subscription fields AND field resolvers on nested
    object types (``DataHealth.metricLineage`` is the one on this tree): a
    nested resolver executes on a real request and is a real surface, it is
    simply only reachable through its parent. ``root`` says which is which so
    a row can be read correctly rather than the distinction being lost.

    Fields with no resolver are plain data attributes read off an already
    resolved parent object -- no code of ours runs for them, so they are not
    surfaces.
    """
    from graphql import GraphQLObjectType  # late import: needs sys.path

    module, attr = graphql_schema
    schema = _import_attr(module, attr, (root / "src").resolve())
    gql_schema = getattr(schema, "_schema", None)
    if gql_schema is None:
        raise DiscoveryImportError(
            f"{module}.{attr} has no `_schema` attribute -- strawberry's "
            "underlying GraphQLSchema is how every type's fields are "
            "enumerated; refusing to fall back to a narrower set"
        )

    root_kinds = _graphql_root_kinds(gql_schema)
    records: list[dict] = []
    for type_name, gql_type in gql_schema.type_map.items():
        if type_name.startswith("__") or not isinstance(gql_type, GraphQLObjectType):
            continue
        for field_name, field in gql_type.fields.items():
            definition = (field.extensions or {}).get("strawberry-definition")
            resolver = getattr(definition, "base_resolver", None)
            if resolver is None:
                continue
            file, line, in_ops = _anchor(getattr(resolver, "wrapped_func", None), root)
            records.append(
                {
                    "kind": root_kinds.get(type_name, "graphql_field"),
                    "root": type_name in root_kinds,
                    "parent_type": type_name,
                    "name": field_name,
                    # The inventory keys GraphQL rows on the PYTHON name
                    # (`graphql_field_name`), not the camelCase wire name.
                    "python_name": getattr(definition, "python_name", None),
                    "file": file,
                    "line": line,
                    "resolver_in_ops_source": in_ops,
                }
            )
    return records


def discover(root: Path, apps=DEPLOYED_APPS, graphql_schema=GRAPHQL_SCHEMA) -> dict:
    """Enumerate every served surface under ``root``.

    ``apps``/``graphql_schema`` default to the real deployed objects and are
    parameters only so this gate's own contract tests can point discovery at a
    purpose-built fixture package and exercise the REAL walk rather than a
    stand-in for it. Nothing in the production path passes them, and
    ``test_discovery_defaults_are_the_real_deployed_objects`` pins the
    defaults so an override can never be mistaken for the shipped config.
    """
    with _import_context(root):
        routes = discover_routes(root, apps)
        graphql = discover_graphql(root, graphql_schema)
    return {
        "apps": [f"{m}::{a}" for m, a in apps],
        "routes": routes,
        "graphql": graphql,
        "counts": {
            "routes": len(routes),
            "graphql": len(graphql),
            "graphql_root": sum(1 for r in graphql if r["root"]),
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Enumerate served ops surfaces")
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument("--out", default=None, help="write JSON here instead of stdout")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    result = discover(root)
    text = json.dumps(result, indent=2, sort_keys=False)
    if args.out:
        Path(args.out).write_text(text + "\n")
    else:
        print(text)
    missing = [r for r in result["routes"] if r["path"].startswith("<NO PATH")]
    if missing:
        print(
            f"NOTE: {len(missing)} entry(ies) in app.routes carry no `.path` "
            "and could not be resolved to a served surface (see the fastapi "
            "pin comment in pyproject.toml). This is a report, not a failure "
            "-- ci/check_endpoint_profiles.py is the gate.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
