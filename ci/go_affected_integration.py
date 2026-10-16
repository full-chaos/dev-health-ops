"""Select the Go integration packages a pull request can affect (CHAOS-9947).

The pull_request storage/integration shards used to run every package on every
change. This script derives, from the changed files and `go list -test -json`,
the integration packages whose TEST BINARY (the package, its tests and every
transitive import of both) contains a changed package. Everything else cannot
observe the change through the Go build graph.

OUTPUT (stdout), first line decides:
  FULL <reason>      run the whole suite (a shared or unclassifiable change)
  AFFECTED <count>   followed by one integration package key per line

FAIL OPEN. A changed file this script cannot place in the build graph is FULL,
never "not affected". Exit status is non-zero only for unreadable input, and the
caller then also runs FULL. The build graph does not see a test reading another
package's files at run time; that limit is named in the PR and the full suite
still runs on main, merge_group and manual runs.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import sys
from pathlib import Path

# A change under one of these is shared infrastructure: always the full suite.
FULL_PREFIXES = ("ci/", ".github/", "internal/testsupport/")
FULL_FILES = frozenset({"go.mod", "go.sum", "go.work", "go.work.sum"})
# Files no Go test reads as an input.
IGNORABLE_PREFIXES = ("docs/", "docs-data/")
IGNORABLE_SUFFIXES = (".md",)


def read_json_stream(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder()
    out: list[dict] = []
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        obj, index = decoder.raw_decode(text, index)
        out.append(obj)
    return out


def read_changed(path: Path) -> list[str]:
    raw = path.read_bytes()
    sep = b"\0" if b"\0" in raw else b"\n"
    return [
        p.decode("utf-8", "surrogateescape") for p in raw.split(sep) if p.strip()
    ]


def strip_variant(import_path: str) -> str:
    return import_path.split(" [", 1)[0]


def select(
    changed: list[str],
    golist: list[dict],
    keys: list[str],
    root: str,
) -> tuple[str, list[str]]:
    packages: dict[str, dict] = {}
    binaries: dict[str, set[str]] = {}
    module_path = ""
    for entry in golist:
        import_path = entry.get("ImportPath", "")
        if import_path.endswith(".test") and " " not in import_path:
            owner = import_path[: -len(".test")]
            binaries[owner] = {strip_variant(d) for d in entry.get("Deps") or []}
            binaries[owner].add(owner)
            continue
        if " " in import_path or entry.get("ForTest"):
            continue
        directory = entry.get("Dir")
        if not directory:
            continue
        rel = os.path.relpath(directory, root).replace(os.sep, "/")
        packages[rel] = entry
        module = entry.get("Module") or {}
        module_path = module_path or module.get("Path", "")
    if not packages or not module_path:
        raise SystemExit("go list output holds no packages or no module path")

    changed_packages: set[str] = set()
    for raw_path in changed:
        path = posixpath.normpath(raw_path)
        if path in FULL_FILES or path.startswith(FULL_PREFIXES):
            return f"FULL shared change: {path}", []
        if path.endswith(".go"):
            directory = posixpath.dirname(path) or "."
            owner = packages.get(directory)
            if owner is None:
                return f"FULL Go file outside any listed package: {path}", []
            changed_packages.add(owner["ImportPath"])
            continue
        if path.startswith(IGNORABLE_PREFIXES) or path.endswith(IGNORABLE_SUFFIXES):
            continue
        owner_import = _owner_of_data_file(path, packages)
        if owner_import is None:
            return f"FULL non-Go file with no owning package input: {path}", []
        changed_packages.add(owner_import)

    affected: list[str] = []
    for key in keys:
        import_path = module_path if key == "." else f"{module_path}/{key}"
        closure = binaries.get(import_path)
        if closure is None:
            return f"FULL integration package without a test binary: {key}", []
        if closure & changed_packages:
            affected.append(key)
    return "AFFECTED", sorted(affected)


def _owner_of_data_file(path: str, packages: dict[str, dict]) -> str | None:
    """A non-Go file counts only as a testdata file or a //go:embed input of the
    nearest package above it; anything else could be a runtime read."""
    directory = posixpath.dirname(path)
    while directory and directory != ".":
        owner = packages.get(directory)
        if owner is not None:
            relative = posixpath.relpath(path, directory)
            embedded: set[str] = set()
            for field in ("EmbedFiles", "TestEmbedFiles", "XTestEmbedFiles"):
                embedded.update(owner.get(field) or [])
            parts = relative.split("/")
            if "testdata" in parts[:-1] or relative in embedded:
                return owner["ImportPath"]
            return None
        directory = posixpath.dirname(directory)
    return None


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changed", required=True, type=Path)
    parser.add_argument("--golist", required=True, type=Path)
    parser.add_argument("--keys", required=True, type=Path)
    parser.add_argument("--root", required=True)
    args = parser.parse_args(argv)

    changed = read_changed(args.changed)
    keys = [k for k in args.keys.read_text(encoding="utf-8").split("\n") if k]
    verdict, affected = select(
        changed, read_json_stream(args.golist), keys, args.root
    )
    if verdict == "AFFECTED":
        print(f"AFFECTED {len(affected)}")
        for key in affected:
            print(key)
    else:
        print(verdict)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
