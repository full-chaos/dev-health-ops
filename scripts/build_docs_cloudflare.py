#!/usr/bin/env python3
"""Build the MkDocs site and prepare the static asset tree used by Wrangler."""

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
    """Run one repository command and stop immediately on failure."""
    print(f"+ {' '.join(args)}", flush=True)
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


def _validate_asset_tree() -> None:
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
        raise RuntimeError(f"prepared Cloudflare asset tree is incomplete: {joined}")


def build(*, mode: str, full_check: bool) -> None:
    """Build MkDocs, run the requested checks, and prepare Wrangler assets."""
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
    _validate_asset_tree()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Build the static documentation asset tree consumed by Wrangler."
    )
    parser.add_argument(
        "--mode",
        choices=("preview", "production"),
        required=True,
        help="Use preview indexing controls or production indexing controls.",
    )
    parser.add_argument(
        "--full-check",
        action="store_true",
        help="Run the complete reader-critical gate before preparing assets.",
    )
    args = parser.parse_args(argv)

    # Production is never prepared without the full publication gate.
    full_check = args.full_check or args.mode == "production"

    try:
        build(mode=args.mode, full_check=full_check)
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
        f"{ASSET_DIR.relative_to(ROOT)} (mode={args.mode}, full_check={full_check}).",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
