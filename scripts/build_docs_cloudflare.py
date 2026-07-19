#!/usr/bin/env python3
"""Build the documentation asset tree expected by Wrangler.

Wrangler runs this file through ``build.command`` before ``dev``, ``deploy``,
and ``versions upload``. Local development uses a fast strict MkDocs build;
upload and deployment commands run the complete reader-critical gate first.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_DIR = ROOT / ".build" / "docs-prototype"
ASSET_DIR = ROOT / ".build" / "docs-cloudflare"
REDIRECTS = ROOT / ".github" / "documentation-program" / "phase-9" / "redirects.tsv"
SEARCH_ACCEPTANCE = (
    ROOT / ".github" / "documentation-program" / "phase-10" / "search-acceptance.json"
)


def _run(args: list[str]) -> None:
    printable = " ".join(args)
    print(f"+ {printable}", flush=True)
    subprocess.run(args, cwd=ROOT, check=True)


def _source_revision() -> str:
    github_sha = os.environ.get("GITHUB_SHA", "").strip()
    if github_sha:
        return github_sha
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_mode(requested: str, wrangler_command: str) -> str:
    override = os.environ.get("DOCS_CLOUDFLARE_MODE", "").strip().lower()
    value = override or requested
    if value in {"preview", "production"}:
        return value
    return "production" if wrangler_command == "deploy" else "preview"


def _validate_prebuilt() -> None:
    required = [
        ASSET_DIR / "index.html",
        ASSET_DIR / "404.html",
        ASSET_DIR / "_headers",
        ASSET_DIR / "_redirects",
        ASSET_DIR / "robots.txt",
        ASSET_DIR / "cloudflare-build-manifest.json",
    ]
    missing = [path.relative_to(ROOT) for path in required if not path.is_file()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise RuntimeError(
            "DOCS_CLOUDFLARE_PREBUILT is set, but the prepared asset tree is "
            f"incomplete: {joined}"
        )


def _build(*, mode: str, full_check: bool) -> None:
    (ROOT / ".build").mkdir(parents=True, exist_ok=True)

    if full_check:
        _run([sys.executable, "scripts/validate_docs_v2_publication.py"])

    _run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--config-file",
            "mkdocs.prototype.yml",
        ]
    )

    if full_check:
        _run(
            [
                sys.executable,
                "scripts/check_built_site_links.py",
                "--site-dir",
                str(SITE_DIR.relative_to(ROOT)),
            ]
        )
        _run(
            [
                sys.executable,
                "scripts/check_docs_candidate_search.py",
                "--site-dir",
                str(SITE_DIR.relative_to(ROOT)),
                "--queries",
                str(SEARCH_ACCEPTANCE.relative_to(ROOT)),
            ]
        )
        _run(
            [
                sys.executable,
                "scripts/check_docs_candidate_accessibility.py",
                "--site-dir",
                str(SITE_DIR.relative_to(ROOT)),
                "--css",
                "docs-prototype/stylesheets/extra.css",
            ]
        )
        _run([sys.executable, "scripts/check_docs_candidate_facts.py"])

    _run(
        [
            sys.executable,
            "scripts/prepare_docs_cloudflare.py",
            "--source",
            str(SITE_DIR.relative_to(ROOT)),
            "--output",
            str(ASSET_DIR.relative_to(ROOT)),
            "--mode",
            mode,
            "--redirects",
            str(REDIRECTS.relative_to(ROOT)),
            "--source-revision",
            _source_revision(),
        ]
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Build the MkDocs output consumed by Cloudflare Workers Static Assets."
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "preview", "production"),
        default="auto",
    )
    parser.add_argument(
        "--full-check",
        action="store_true",
        help="Run the complete reader-critical gate before preparing assets.",
    )
    args = parser.parse_args(argv)

    wrangler_command = os.environ.get("WRANGLER_COMMAND", "manual").strip().lower()
    mode = _resolve_mode(args.mode, wrangler_command)
    full_check = (
        args.full_check
        or _truthy("DOCS_CLOUDFLARE_FULL_CHECK")
        or wrangler_command in {"deploy", "versions upload"}
    )

    try:
        if _truthy("DOCS_CLOUDFLARE_PREBUILT"):
            _validate_prebuilt()
            print(
                "Using the existing prepared Cloudflare documentation asset tree.",
                flush=True,
            )
            return 0
        _build(mode=mode, full_check=full_check)
    except FileNotFoundError as exc:
        print(f"ERROR: required command was not found: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            f"ERROR: documentation build command failed with exit code {exc.returncode}.\n"
            "Install the documentation environment with: "
            "python -m pip install -r requirements-docs.txt",
            file=sys.stderr,
        )
        return exc.returncode or 1
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        "Cloudflare documentation assets are ready at "
        f"{ASSET_DIR.relative_to(ROOT)} (mode={mode}, full_check={full_check}).",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
