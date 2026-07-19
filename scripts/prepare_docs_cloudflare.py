#!/usr/bin/env python3
"""Prepare the built documentation for Cloudflare Workers Static Assets."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse


SECURITY_HEADERS = [
    "X-Content-Type-Options: nosniff",
    "Referrer-Policy: strict-origin-when-cross-origin",
    "X-Frame-Options: SAMEORIGIN",
    "Permissions-Policy: camera=(), geolocation=(), microphone=(), payment=(), usb=()",
]


def _valid_origin(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise ValueError(f"canonical origin must be an HTTPS origin: {value}")
    return value.rstrip("/")


def _load_redirects(path: Path) -> list[tuple[str, str]]:
    redirects: list[tuple[str, str]] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"source_path", "target_path"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"redirect manifest is missing {sorted(required)}: {path}")
        for line_number, row in enumerate(reader, start=2):
            source = (row.get("source_path") or "").strip()
            target = (row.get("target_path") or "").strip()
            if not source or not target:
                raise ValueError(f"empty redirect at {path}:{line_number}")
            if not source.startswith("/") or not target.startswith("/"):
                raise ValueError(
                    f"redirect paths must be site-relative at {path}:{line_number}: "
                    f"{source!r} -> {target!r}"
                )
            if source == target:
                raise ValueError(f"self redirect at {path}:{line_number}: {source}")
            if source in seen:
                raise ValueError(
                    f"duplicate redirect source at {path}:{line_number}: {source}"
                )
            seen.add(source)
            redirects.append((source, target))
    return redirects


def _write_redirects(path: Path, redirects: list[tuple[str, str]]) -> None:
    lines = [
        "# Generated from the approved Phase 9 redirect inventory.",
        "# Source order is deterministic; every rule is a permanent path redirect.",
    ]
    lines.extend(f"{source} {target} 301" for source, target in redirects)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_headers(path: Path, mode: str) -> None:
    lines = ["/*"]
    lines.extend(f"  {header}" for header in SECURITY_HEADERS)
    if mode == "preview":
        lines.append("  X-Robots-Tag: noindex, nofollow")
    lines.extend(
        [
            "",
            "/assets/*",
            "  Cache-Control: public, max-age=31536000, immutable",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_robots(path: Path, mode: str, canonical_origin: str) -> None:
    if mode == "preview":
        content = "User-agent: *\nDisallow: /\n"
    else:
        content = f"User-agent: *\nAllow: /\nSitemap: {canonical_origin}/sitemap.xml\n"
    path.write_text(content, encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_inventory(root: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        records.append(
            {
                "path": str(path.relative_to(root)),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    return records


def prepare(
    *,
    source: Path,
    output: Path,
    mode: str,
    redirects_path: Path,
    canonical_origin: str,
    source_revision: str,
) -> dict[str, object]:
    if not source.is_dir():
        raise ValueError(f"built documentation directory not found: {source}")
    if not (source / "index.html").is_file():
        raise ValueError(f"built documentation has no index.html: {source}")
    if not (source / "404.html").is_file():
        raise ValueError(f"built documentation has no 404.html: {source}")
    if not redirects_path.is_file():
        raise ValueError(f"redirect manifest not found: {redirects_path}")

    redirects = _load_redirects(redirects_path)
    if output.exists():
        shutil.rmtree(output)
    shutil.copytree(source, output)

    _write_redirects(output / "_redirects", redirects)
    _write_headers(output / "_headers", mode)
    _write_robots(output / "robots.txt", mode, canonical_origin)

    markdown_files = sorted(output.rglob("*.md"))
    if markdown_files:
        paths = ", ".join(str(path.relative_to(output)) for path in markdown_files[:10])
        raise ValueError(f"built output unexpectedly contains Markdown source: {paths}")

    files = _file_inventory(output)
    manifest: dict[str, object] = {
        "schema_version": 1,
        "mode": mode,
        "canonical_origin": canonical_origin,
        "source_revision": source_revision,
        "redirect_count": len(redirects),
        "file_count": len(files),
        "files": files,
    }
    (output / "cloudflare-build-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare MkDocs output for Cloudflare Workers Static Assets."
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("preview", "production"), required=True)
    parser.add_argument("--redirects", type=Path, required=True)
    parser.add_argument(
        "--canonical-origin",
        default="https://docs.fullchaos.dev",
    )
    parser.add_argument("--source-revision", default="")
    args = parser.parse_args(argv)

    try:
        manifest = prepare(
            source=args.source,
            output=args.output,
            mode=args.mode,
            redirects_path=args.redirects,
            canonical_origin=_valid_origin(args.canonical_origin),
            source_revision=args.source_revision,
        )
    except (OSError, ValueError, csv.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        "Prepared Cloudflare documentation assets: "
        f"mode={manifest['mode']} files={manifest['file_count']} "
        f"redirects={manifest['redirect_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
