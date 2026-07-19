#!/usr/bin/env python3
"""Extract version and preview details from Wrangler structured ND-JSON output."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

VERSION_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
URL_RE = re.compile(r"https://[^\s\"']+")


def _walk(
    value: Any, key_path: tuple[str, ...] = ()
) -> Iterator[tuple[tuple[str, ...], str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _walk(child, (*key_path, str(key).lower()))
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child, key_path)
    elif isinstance(value, str):
        yield key_path, value


def _load(path: Path) -> list[Any]:
    records: list[Any] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid Wrangler JSON at {path}:{line_number}: {exc}"
            ) from exc
    return records


def extract(path: Path) -> dict[str, str]:
    preview_urls: list[str] = []
    other_urls: list[str] = []
    version_ids: list[str] = []

    for record in _load(path):
        for key_path, text in _walk(record):
            keys = " ".join(key_path)
            for match in URL_RE.findall(text):
                url = match.rstrip(".,;)")
                if "workers.dev" in url and ("preview" in keys or "url" in keys):
                    preview_urls.append(url)
                else:
                    other_urls.append(url)
            for match in VERSION_RE.findall(text):
                if "version" in keys or "id" in keys:
                    version_ids.append(match)

    result: dict[str, str] = {}
    if preview_urls:
        result["preview_url"] = preview_urls[0]
    elif any("workers.dev" in url for url in other_urls):
        result["preview_url"] = next(
            url for url in other_urls if "workers.dev" in url
        )
    if version_ids:
        result["version_id"] = version_ids[0]
    return result


def _write_github_output(path: Path, values: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Extract preview_url and version_id from Wrangler ND-JSON output."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)

    if not args.input.is_file():
        print(f"ERROR: Wrangler output not found: {args.input}", file=sys.stderr)
        return 1

    try:
        values = extract(args.input)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.github_output:
        _write_github_output(args.github_output, values)
    print(json.dumps(values, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
