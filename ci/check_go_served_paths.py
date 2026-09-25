#!/usr/bin/env python3
"""CHAOS-6813: a Python route body may be deleted only for a path the deployed
Go ingress routes to Go.

The Go-served REST routes keep their Python route mounted with the body reduced
to the "served by Go" refusal stub (``_raise_served_by_go_api``, the shape of
CHAOS-6241's 32 deletions). That is safe only when the deployed ingress sends
the path to a Go service: a stubbed route still reached on the Python plane
answers 500 in production. The ingress routes by PATH (no HTTP method), and it
is a deploy fact, so it lives in a checked-in manifest that prod-ops updates
per release: ``ci/go_served_paths.tsv``.

This gate fails when

1. a served Python route's body is the refusal stub and its path is not in the
   manifest (the body was deleted for a path no recorded revision routes to Go);
2. a manifest row names a path no served Python route has (the manifest and the
   route registry drifted: a row nobody can act on);
3. the manifest itself is malformed (columns, plane, path form, duplicates).

Routes come from ``ci/discover_ops_routes.py`` (the served application, not a
source regex); the stub is recognised on the endpoint function's AST, so a body
that only LOOKS like a stub, or a stub with extra logic, is judged for what it
is: only a body that is nothing but the refusal call counts.

Manifest columns (tab separated, ``#`` comments):
    rev     the release label the path was first observed Go-routed
    plane   go-api | query-api (the Go service the ingress sends it to)
    path    the ingress path with every path parameter written ``{}``

Usage:
    python3 ci/check_go_served_paths.py [--root PATH] [--manifest PATH]
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

STUB_NAME = "_raise_served_by_go_api"
PLANES = {"go-api", "query-api"}
MANIFEST_RELATIVE = Path("ci/go_served_paths.tsv")
_PARAM = re.compile(r"\{[^}/]*\}")
_PATH_FORM = re.compile(r"^/[A-Za-z0-9_\-./{}]*$")


def normalize(path: str) -> str:
    """The comparison form of a route path: parameters anonymous, no trailing
    slash (the ingress and the app agree on paths up to parameter names)."""
    stripped = _PARAM.sub("{}", path)
    return stripped.rstrip("/") or "/"


@dataclass(frozen=True)
class ManifestRow:
    rev: str
    plane: str
    path: str


def load_manifest(path: Path) -> tuple[dict[str, ManifestRow], list[str]]:
    """The manifest rows by path, and every problem found reading it."""
    rows: dict[str, ManifestRow] = {}
    problems: list[str] = []
    if not path.is_file():
        return rows, [f"{path}: the manifest does not exist"]
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        cells = line.split("\t")
        if len(cells) != 3 or any(not cell.strip() for cell in cells):
            problems.append(
                f"{path}:{number}: want 3 tab-separated cells (rev, plane, path), got {line!r}"
            )
            continue
        rev, plane, route = (cell.strip() for cell in cells)
        if plane not in PLANES:
            problems.append(
                f"{path}:{number}: plane {plane!r} is not one of {sorted(PLANES)}"
            )
        if not _PATH_FORM.match(route) or normalize(route) != route:
            problems.append(
                f"{path}:{number}: path {route!r} must be written with anonymous {{}} parameters and no trailing slash"
            )
        if route in rows:
            problems.append(f"{path}:{number}: path {route!r} is listed twice")
            continue
        rows[route] = ManifestRow(rev, plane, route)
    return rows, problems


def _is_stub_body(body: list[ast.stmt]) -> bool:
    statements = list(body)
    if (
        statements
        and isinstance(statements[0], ast.Expr)
        and isinstance(statements[0].value, ast.Constant)
        and isinstance(statements[0].value.value, str)
    ):
        statements = statements[1:]  # the docstring
    if len(statements) != 1:
        return False
    statement = statements[0]
    call = None
    if isinstance(statement, ast.Expr):
        call = statement.value
    elif isinstance(statement, ast.Return):
        call = statement.value
    if not isinstance(call, ast.Call):
        return False
    func = call.func
    name = (
        func.id
        if isinstance(func, ast.Name)
        else func.attr
        if isinstance(func, ast.Attribute)
        else None
    )
    return name == STUB_NAME


def _function_at(
    tree: ast.AST, name: str, line: int | None
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """The endpoint function: matched by name, and by line when several share it
    (the anchor is the ``def`` line, or the first decorator's line)."""
    candidates = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    if len(candidates) <= 1 or line is None:
        return candidates[0] if candidates else None
    for node in candidates:
        starts = {node.lineno, *(decorator.lineno for decorator in node.decorator_list)}
        if line in starts:
            return node
    return None


def stub_routes(routes: list[dict], root: Path) -> list[dict]:
    """The served routes whose endpoint body is nothing but the refusal stub."""
    trees: dict[str, ast.AST] = {}
    found: list[dict] = []
    for route in routes:
        if not route.get("endpoint_in_ops_source") or not route.get("file"):
            continue
        file = Path(route["file"])
        if not file.is_absolute():
            file = root / file
        key = str(file)
        if key not in trees:
            trees[key] = ast.parse(file.read_text(), filename=key)
        function = _function_at(
            trees[key], route.get("endpoint_name") or "", route.get("line")
        )
        if function is not None and _is_stub_body(function.body):
            found.append(route)
    return found


def check(
    routes: list[dict], manifest: dict[str, ManifestRow], root: Path
) -> list[str]:
    problems: list[str] = []
    listed = set(manifest)
    for route in stub_routes(routes, root):
        path = normalize(route["path"])
        if path not in listed:
            problems.append(
                f"{route['method']} {route['path']} ({route.get('file')}:{route.get('line')}): the body is the "
                f"'served by Go' refusal stub, but {path} is not in the Go-served path manifest "
                f"({MANIFEST_RELATIVE}); a route still answering on the Python plane would 500 in production"
            )
    served = {normalize(route["path"]) for route in routes}
    for path, row in sorted(manifest.items()):
        if path not in served:
            problems.append(
                f"{MANIFEST_RELATIVE}: {row.rev} {row.plane} {path} names no served Python route: "
                "remove the row or fix the path"
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=".", type=Path)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument(
        "--routes-json",
        type=Path,
        default=None,
        help="a discover_ops_routes report (skips the app import)",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    manifest_path = args.manifest or root / MANIFEST_RELATIVE

    manifest, problems = load_manifest(manifest_path)
    if args.routes_json is not None:
        routes = json.loads(args.routes_json.read_text())["routes"]
    else:
        spec = importlib.util.spec_from_file_location(
            "_discover_ops_routes", root / "ci" / "discover_ops_routes.py"
        )
        assert spec is not None and spec.loader is not None
        discovery = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = discovery
        spec.loader.exec_module(discovery)  # imports the served application
        routes = discovery.discover(root)["routes"]
    problems.extend(check(routes, manifest, root))
    if problems:
        print("go-served-paths: FAIL", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    stubs = stub_routes(routes, root)
    print(
        f"go-served-paths: ok ({len(manifest)} manifest paths, {len(stubs)} stubbed routes, "
        f"{len({normalize(route['path']) for route in stubs})} stubbed paths, all in the manifest)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
