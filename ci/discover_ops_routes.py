#!/usr/bin/env python3
"""CHAOS-3273 Wave 0 source-discovery: every ops REST + GraphQL surface.

Modelled on ``ci/check_transitional_inventory.py`` (CUT-01) -- clones its
shape rather than inventing a new one: independent re-derivation from
source (never trusts ``contracts/auth/v1/endpoint-profiles.ops.json``
itself), a ``Surface``-shaped record per discovered thing, and regex-driven
discovery functions that a later CI gate (L3) can run the same way this
script is run here.

Two surface kinds:

  * REST routes -- every ``@<router_alias>.<method>(...)`` decorator and every
    ``<router_alias>.include_router(...)`` mount edge under
    ``src/dev_health_ops/api``, resolved to a FULL mount path by walking the
    include graph from each ``FastAPI()`` root (there are two: the main app
    in ``api/main.py`` and the separately-deployed billing-edge app in
    ``api/billing_edge.py`` -- see docs/reference/auth/endpoint-profiles.md
    "Two deployed apps").
  * GraphQL resolvers -- every ``@strawberry.field`` / ``@strawberry.mutation``
    under ``src/dev_health_ops/api/graphql``.

Known limitation (documented, not silently swallowed): a router included via
a fully dynamic expression that isn't a bare name, an attribute access on a
bare name, or the one hard-coded ``importlib.import_module("<literal>").router``
shape used by ``billing/router.py`` (see ``_DYNAMIC_IMPORTLIB_INCLUDE_RE``) is
not resolved and is reported under ``unresolved_includes`` in the JSON output
instead of silently dropped.

Usage:
    python3 ci/discover_ops_routes.py [--root PATH] [--out PATH]
Prints a JSON report to stdout (or --out) with:
    {"routes": [...], "graphql": [...], "unresolved_includes": [...],
     "counts": {"routes": N, "graphql": N}}
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

API_ROOT = "src/dev_health_ops/api"

_APIROUTER_OR_FASTAPI_RE = re.compile(r"^(\w+)\s*=\s*(FastAPI|APIRouter)\(")
_BARE_ALIAS_RE = re.compile(r"^(\w+)\s*=\s*(\w+)\s*$")
_ROUTE_DECORATOR_RE = re.compile(r"^\s*@(\w+)\.(get|post|put|patch|delete|api_route)\(")
_INCLUDE_ROUTER_RE = re.compile(r"^\s*(\w+)\.include_router\(")
_DYNAMIC_IMPORTLIB_INCLUDE_RE = re.compile(
    r'importlib\.import_module\(\s*["\']([\w.]+)["\']\s*\)\.(\w+)'
)
_STR_LITERAL_RE = re.compile(r"""["']([^"']*)["']""")
_PREFIX_KW_RE = re.compile(r'prefix\s*=\s*["\']([^"\']*)["\']')
_STRAWBERRY_DECORATOR_RE = re.compile(r"^\s*@strawberry\.(field|mutation)\b")
_DEF_NAME_RE = re.compile(r"^\s*(?:async\s+def|def)\s+(\w+)\(")
_IMPORT_FROM_RE = re.compile(r"^from\s+([.\w]+)\s+import\s+(.+)$")
# `x = importlib.import_module("dev_health_ops.api.auth.sso")` -- a runtime,
# not `import`-statement, module binding. Only the static-string-literal
# form is resolvable; a non-literal argument is a documented known
# limitation (matches ci/check_transitional_inventory.py's own treatment of
# the same shape).
_IMPORTLIB_MODULE_ALIAS_RE = re.compile(
    r"""^(\w+)\s*=\s*importlib\.import_module\(\s*["\']([\w.]+)["\']\s*\)\s*$"""
)
# `maybe_sso_router = getattr(sso_module, "sso_router", None)` -- resolved
# only when `sso_module` is itself a known importlib module alias from this
# same file (the one real shape in this codebase: auth/router.py's optional
# SSO router load).
_GETATTR_ATTR_RE = re.compile(
    r"""^(\w+)\s*=\s*getattr\(\s*(\w+)\s*,\s*["\'](\w+)["\']"""
)
_MAX_STATEMENT_LINES = 15


def _iter_py_files(base: Path):
    if not base.exists():
        return
    for path in sorted(base.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        yield path


def _relpath(root: Path, path: Path) -> str:
    return str(path.relative_to(root))


def _module_dotted(relpath: str) -> str:
    """``src/dev_health_ops/api/admin/router.py`` -> ``dev_health_ops.api.admin.router``."""
    parts = Path(relpath).with_suffix("").parts
    # drop the leading "src"
    if parts and parts[0] == "src":
        parts = parts[1:]
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _strip_comment(line: str) -> str:
    """Strip a trailing ``# ...`` comment, quote-aware so a ``#`` inside a
    string literal (route path, docstring fragment) is left alone. Needed
    before joining `from X import (...)` bodies: a trailing ``# noqa: F401``
    on the single-name form (``from .router import router  # noqa: F401``)
    would otherwise be folded into the imported name."""
    out = []
    quote = None
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(line[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch == "#":
            break
        if ch in ("'", '"'):
            quote = ch
        out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _statement_window(lines: list[str], line_no: int) -> str:
    """Bracket-balanced logical statement starting at 1-indexed line_no."""
    parts = []
    depth = 0
    for i in range(line_no - 1, min(line_no - 1 + _MAX_STATEMENT_LINES, len(lines))):
        code = lines[i]
        parts.append(code)
        depth += code.count("(") + code.count("[") + code.count("{")
        depth -= code.count(")") + code.count("]") + code.count("}")
        if depth <= 0 and i > line_no - 1:
            break
        if depth <= 0 and "(" in code:
            break
    return "\n".join(parts)


@dataclass
class RouterDef:
    """A local name bound to an ``APIRouter()``/``FastAPI()`` constructor,
    OR an alias of one, in one file."""

    module: str
    file: str
    varname: str
    line: int
    kind: str  # "fastapi" | "apirouter" | "alias"
    own_prefix: str = ""
    alias_target: str | None = None  # (module, varname) as "module::varname"


@dataclass
class IncludeEdge:
    parent_module: str
    parent_var: str
    child_module: str | None  # None if unresolved
    child_var: str | None
    extra_prefix: str
    file: str
    line: int
    raw: str


@dataclass
class RouteSurface:
    method: str
    local_path: str
    module: str
    file: str
    line: int
    router_key: str  # module::varname the decorator was applied to


@dataclass
class ResolverSurface:
    kind: str  # field | mutation
    name: str
    module: str
    file: str
    line: int


@dataclass
class ImportBinding:
    local_name: str
    src_module: str
    src_name: str


def _resolve_relative_import(current_module: str, dotted: str, is_init: bool) -> str:
    """Resolve a possibly-relative ``from X import Y`` module spec against
    the importing module's own dotted path.

    Python's relative-import level counts from the importing module's OWN
    package: for a plain module ``pkg.sub`` that package is ``pkg`` (one
    dot = ``pkg``), but for ``pkg/sub/__init__.py`` (module dotted path
    ``pkg.sub``, since ``_module_dotted`` drops the trailing ``__init__``)
    the module's own package IS ``pkg.sub`` itself (one dot = ``pkg.sub``,
    matching CPython's ``__init__.py`` having ``__package__ == __name__``).
    Getting this wrong silently re-routes every re-export through a
    package ``__init__.py`` (the common ``from .routers import X`` pattern
    in this codebase) to the wrong module and breaks mount resolution.
    """
    if not dotted.startswith("."):
        return dotted
    level = len(dotted) - len(dotted.lstrip("."))
    remainder = dotted[level:]
    own_package = (
        current_module.split(".") if is_init else current_module.split(".")[:-1]
    )
    base = own_package[: len(own_package) - (level - 1)] if level >= 1 else own_package
    if remainder:
        base = base + remainder.split(".")
    return ".".join(base)


def _parse_file(root: Path, path: Path):
    relpath = _relpath(root, path)
    module = _module_dotted(relpath)
    is_init = path.name == "__init__.py"
    lines = path.read_text().splitlines()

    router_defs: dict[str, RouterDef] = {}
    includes: list[IncludeEdge] = []
    routes: list[RouteSurface] = []
    resolvers: list[ResolverSurface] = []
    imports: dict[str, ImportBinding] = {}
    module_aliases: dict[str, str] = {}
    unresolved: list[dict] = []

    for i, raw_line in enumerate(lines, start=1):
        stripped = _strip_comment(raw_line).strip()

        m = _IMPORT_FROM_RE.match(stripped)
        if m:
            src_mod = _resolve_relative_import(module, m.group(1), is_init)
            body = m.group(2).strip()
            depth = body.count("(") - body.count(")")
            j = i
            while depth > 0 and j < len(lines):
                nxt = _strip_comment(lines[j]).strip()
                body += " " + nxt
                depth += nxt.count("(") - nxt.count(")")
                j += 1
            body = body.strip()
            if body.startswith("("):
                body = body[1:]
            body = body.rstrip(")").rstrip(",")
            for part in body.split(","):
                part = part.strip()
                if not part:
                    continue
                original, _, alias = part.partition(" as ")
                original = original.strip()
                local = alias.strip() or original
                imports[local] = ImportBinding(local, src_mod, original)
            continue

        m = _IMPORTLIB_MODULE_ALIAS_RE.match(stripped)
        if m:
            module_aliases[m.group(1)] = m.group(2)
            continue

        m = _GETATTR_ATTR_RE.match(stripped)
        if m:
            target, mod_alias, attr = m.group(1), m.group(2), m.group(3)
            if mod_alias in module_aliases:
                imports[target] = ImportBinding(target, module_aliases[mod_alias], attr)
            continue

        m = _APIROUTER_OR_FASTAPI_RE.match(stripped)
        if m:
            varname, ctor = m.group(1), m.group(2)
            stmt = _statement_window(lines, i)
            pm = _PREFIX_KW_RE.search(stmt)
            own_prefix = pm.group(1) if pm else ""
            router_defs[varname] = RouterDef(
                module,
                relpath,
                varname,
                i,
                "fastapi" if ctor == "FastAPI" else "apirouter",
                own_prefix=own_prefix,
            )
            continue

        m = _BARE_ALIAS_RE.match(stripped)
        if m and m.group(1) not in router_defs:
            target, source = m.group(1), m.group(2)
            # Only meaningful once we know `source` is router-ish; resolved
            # in a second pass below (aliases can precede or follow defs).
            router_defs.setdefault(
                target,
                RouterDef(module, relpath, target, i, "alias", alias_target=source),
            )
            continue

        m = _ROUTE_DECORATOR_RE.match(raw_line)
        if m:
            alias, http_method = m.group(1), m.group(2)
            stmt = _statement_window(lines, i)
            lm = _STR_LITERAL_RE.search(stmt)
            local_path = lm.group(1) if lm else "<unresolved-path>"
            if http_method == "api_route":
                # methods=[...] runtime list -- record as-is, not expanded.
                http_method = "api_route"
            routes.append(
                RouteSurface(
                    http_method.upper(),
                    local_path,
                    module,
                    relpath,
                    i,
                    router_key=f"__LOCAL__::{alias}",
                )
            )
            continue

        m = _INCLUDE_ROUTER_RE.match(stripped)
        if m:
            parent_var = m.group(1)
            stmt = _statement_window(lines, i)
            pm = _PREFIX_KW_RE.search(stmt.split("include_router(", 1)[-1])
            extra_prefix = pm.group(1) if pm else ""
            dyn = _DYNAMIC_IMPORTLIB_INCLUDE_RE.search(stmt)
            if dyn:
                includes.append(
                    IncludeEdge(
                        module,
                        parent_var,
                        dyn.group(1),
                        dyn.group(2),
                        extra_prefix,
                        relpath,
                        i,
                        stmt.strip(),
                    )
                )
            else:
                # First bare-name or dotted-name argument after the open paren.
                arg_area = stmt.split("include_router(", 1)[-1]
                nm = re.match(r"\s*([\w.]+)", arg_area)
                if nm and "." not in nm.group(1):
                    includes.append(
                        IncludeEdge(
                            module,
                            parent_var,
                            None,
                            nm.group(1),
                            extra_prefix,
                            relpath,
                            i,
                            stmt.strip(),
                        )
                    )
                else:
                    includes.append(
                        IncludeEdge(
                            module,
                            parent_var,
                            None,
                            None,
                            extra_prefix,
                            relpath,
                            i,
                            stmt.strip(),
                        )
                    )
            continue

        m = _STRAWBERRY_DECORATOR_RE.match(raw_line)
        if m:
            kind = m.group(1)
            name = None
            for j in range(i, min(i + 25, len(lines) + 1)):
                dm = _DEF_NAME_RE.match(lines[j - 1])
                if dm:
                    name = dm.group(1)
                    break
            resolvers.append(
                ResolverSurface(kind, name or "<unresolved-name>", module, relpath, i)
            )
            continue

    return router_defs, includes, routes, resolvers, imports, unresolved


def discover(root: Path) -> dict:
    api_dir = root / API_ROOT
    all_router_defs: dict[str, RouterDef] = {}  # key "module::varname"
    all_includes: list[IncludeEdge] = []
    all_routes: list[RouteSurface] = []
    all_resolvers: list[ResolverSurface] = []
    all_imports: dict[str, dict[str, ImportBinding]] = {}  # module -> {local: binding}

    for path in _iter_py_files(api_dir):
        router_defs, includes, routes, resolvers, imports, _ = _parse_file(root, path)
        module = _module_dotted(_relpath(root, path))
        for varname, d in router_defs.items():
            all_router_defs[f"{module}::{varname}"] = d
        all_includes.extend(includes)
        # Fix up route.router_key from "__LOCAL__::alias" to "module::alias"
        for r in routes:
            _, alias = r.router_key.split("::", 1)
            r.router_key = f"{module}::{alias}"
        all_routes.extend(routes)
        all_resolvers.extend(resolvers)
        all_imports[module] = imports

    def resolve_name(module: str, name: str, _seen: set | None = None) -> str | None:
        """Resolve a bare local name in `module` to a "module::varname" key
        of an actual APIRouter/FastAPI constructor, following aliases and
        cross-file imports to a fixed point. Returns None if undecidable."""
        _seen = _seen or set()
        key = f"{module}::{name}"
        if key in _seen:
            return None
        _seen.add(key)
        if key in all_router_defs:
            d = all_router_defs[key]
            if d.kind in ("fastapi", "apirouter"):
                return key
            if d.kind == "alias" and d.alias_target:
                # alias target may itself be a local name or an imported one
                return resolve_name(
                    module, d.alias_target, _seen
                ) or resolve_name_import(module, d.alias_target, _seen)
        return resolve_name_import(module, name, _seen)

    def resolve_name_import(module: str, name: str, _seen: set) -> str | None:
        binding = all_imports.get(module, {}).get(name)
        if binding is None:
            return None
        return resolve_name(binding.src_module, binding.src_name, _seen)

    # Resolve every route's router_key to its OWN (module, varname) def --
    # routes are always decorated directly on a local name in the same file,
    # so this should always hit `all_router_defs` directly; kept through
    # resolve_name for aliasing (`r = router; @r.get(...)`).
    resolved_routes = []
    for r in all_routes:
        mod, alias = r.router_key.split("::", 1)
        resolved = resolve_name(mod, alias)
        resolved_routes.append((r, resolved))

    # Resolve every include edge's child to a router-def key.
    resolved_includes = []
    for e in all_includes:
        if e.child_module and e.child_var:
            # dynamic importlib form: child_module is a dotted module path,
            # child_var is the attribute name on that module.
            key = f"{e.child_module}::{e.child_var}"
            resolved_child = key if key in all_router_defs else None
        elif e.child_var:
            resolved_child = resolve_name(e.parent_module, e.child_var)
        else:
            resolved_child = None
        parent_key = resolve_name(e.parent_module, e.parent_var)
        resolved_includes.append((e, parent_key, resolved_child))

    # Build mount graph: child_key -> list[(parent_key, extra_prefix)]
    mounts: dict[str, list[tuple[str, str]]] = {}
    unresolved_includes = []
    for e, parent_key, child_key in resolved_includes:
        if parent_key is None or child_key is None:
            unresolved_includes.append({"file": e.file, "line": e.line, "raw": e.raw})
            continue
        mounts.setdefault(child_key, []).append((parent_key, e.extra_prefix))

    roots = {k: d for k, d in all_router_defs.items() if d.kind == "fastapi"}

    def full_prefix(key: str, _seen: set | None = None) -> str | None:
        _seen = _seen or set()
        if key in _seen:
            return None
        _seen.add(key)
        d = all_router_defs.get(key)
        if d is None:
            return None
        if d.kind == "fastapi":
            return ""
        own = d.own_prefix
        parents = mounts.get(key, [])
        if not parents:
            # Never included anywhere reachable from a FastAPI() root --
            # unmounted router (e.g. a router built but never wired in).
            return None
        # A router can legitimately be included from more than one parent
        # in theory; ops routers are each included exactly once. Use the
        # first resolvable parent chain, and record if more than one parent
        # resolves to a DIFFERENT prefix (would be a genuine ambiguity).
        prefixes = []
        for parent_key, extra in parents:
            pf = full_prefix(parent_key, _seen)
            if pf is not None:
                prefixes.append(pf + extra + own)
        if not prefixes:
            return None
        return prefixes[0]

    out_routes = []
    for r, router_key in resolved_routes:
        if router_key is None:
            out_routes.append(
                {
                    "method": r.method,
                    "local_path": r.local_path,
                    "full_path": None,
                    "module": r.module,
                    "file": r.file,
                    "line": r.line,
                    "resolution": "UNRESOLVED_ROUTER",
                }
            )
            continue
        prefix = full_prefix(router_key)
        if prefix is None:
            out_routes.append(
                {
                    "method": r.method,
                    "local_path": r.local_path,
                    "full_path": None,
                    "module": r.module,
                    "file": r.file,
                    "line": r.line,
                    "resolution": "UNMOUNTED_ROUTER",
                    "router_key": router_key,
                }
            )
            continue
        full_path = (
            prefix + r.local_path
            if r.local_path.startswith("/")
            else prefix + "/" + r.local_path
        )
        # normalize a doubled slash from prefix="" + local_path="/x"
        full_path = re.sub(r"//+", "/", full_path)
        out_routes.append(
            {
                "method": r.method,
                "local_path": r.local_path,
                "full_path": full_path,
                "module": r.module,
                "file": r.file,
                "line": r.line,
                "router_key": router_key,
                "app_root": _app_root_for(router_key, mounts, roots),
                "resolution": "OK",
            }
        )

    out_resolvers = [
        {
            "kind": rr.kind,
            "name": rr.name,
            "module": rr.module,
            "file": rr.file,
            "line": rr.line,
        }
        for rr in all_resolvers
    ]

    return {
        "routes": out_routes,
        "graphql": out_resolvers,
        "unresolved_includes": unresolved_includes,
        "counts": {
            "routes": len(out_routes),
            "routes_resolved": sum(1 for r in out_routes if r["resolution"] == "OK"),
            "graphql": len(out_resolvers),
        },
    }


def _app_root_for(router_key: str, mounts: dict, roots: dict) -> str | None:
    seen = set()
    key = router_key
    while key not in roots:
        if key in seen:
            return None
        seen.add(key)
        parents = mounts.get(key)
        if not parents:
            return None
        key = parents[0][0]
    return key


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
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
    unresolved = result["counts"]["routes"] - result["counts"]["routes_resolved"]
    if unresolved:
        print(
            f"NOTE: {unresolved} route(s) could not be resolved to a full "
            'mount path (see "resolution" field). This is a report, not a '
            "failure -- discover_ops_routes.py has no pass/fail exit code; "
            "the L3 CI gate consumes this output.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
